package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

const headerSize = 16

// Command requests
const (
	Handshake uint32 = 65536
	HudConf   uint32 = 65552
	SpeedConf uint32 = 67216
	Heartbeat uint32 = 1879048192
	ClientSet uint32 = 66528
)

// Command responses
const (
	HandshakeOk uint32 = 65537
	PhoneConf   uint32 = 65553
	SpeedOk     uint32 = 67217
	HeartbeatOk uint32 = 1879048193
	ClientOk    uint32 = 66529
)

type PXCResponse struct {
	Command uint32
	Size    uint32
	Magic   uint32
	Token   uint32
	Body    json.RawMessage
}

type HUDConfig struct {
	HUID                      string `json:"HUID"`
	HUName                    string `json:"HUName"`
	BluetoothPolicy           int    `json:"bluetoothPolicy"`
	BtAddress                 string `json:"btAddress"`
	BtName                    string `json:"btName"`
	BtPin                     string `json:"btPin"`
	CarBrand                  string `json:"carBrand"`
	CarConfig                 string `json:"carConfig"`
	CarMicSupportFeature      int    `json:"carMicSupportFeature"`
	CarModel                  string `json:"carModel"`
	Channel                   string `json:"channel"`
	CurrentHUTime             uint   `json:"currentHUTime"`
	DisablePageInRVMap        int    `json:"disablePageInRVMap"`
	DisableShowCallInfo       bool   `json:"disableShowCallInfo"`
	DisableShowInRVInfo       any    `json:"disableShowInRVInfo"`
	Dpi                       int    `json:"dpi"`
	EnableDPI                 bool   `json:"enableDPI"`
	EnableSockServerAuth      bool   `json:"enableSockServerAuth"`
	Flavor                    int    `json:"flavor"`
	MirrorMode                int    `json:"mirrorMode"`
	PackageName               string `json:"package_name"`
	ProductType               int    `json:"productType"`
	PxcVersion                string `json:"pxcVersion"`
	ScreenType                int    `json:"screenType"`
	SdkVersion                string `json:"sdkVersion"`
	SocketTimeoutPeriodWifi   int    `json:"socketTimeoutPeriodWifi"`
	SteeringMode              int    `json:"steeringMode"`
	SupportBTCall             bool   `json:"supportBTCall"`
	SupportBTSetting          bool   `json:"supportBTSetting"`
	SupportBackDesktop        bool   `json:"supportBackDesktop"`
	SupportBackDesktopNew     bool   `json:"supportBackDesktopNew"`
	SupportConnect            int    `json:"supportConnect"`
	SupportDownloadScreenEvt  bool   `json:"supportDownloadScreenEvt"`
	SupportFunction           int    `json:"supportFunction"`
	SupportHID                bool   `json:"supportHID"`
	SupportLandscapeAdaptive  bool   `json:"supportLandscapeAdaptive"`
	SupportMic                bool   `json:"supportMic"`
	SupportMirrorOverlayTouch bool   `json:"supportMirrorOverlayTouch"`
	SupportMirrorReconnect    bool   `json:"supportMirrorReconnect"`
	SupportOTASpeenUp         bool   `json:"supportOTASpeenUp"`
	SupportOTAUpdate          bool   `json:"supportOTAUpdate"`
	SupportPhoneSignal        bool   `json:"supportPhoneSignal"`
	SupportRVForAdb           bool   `json:"supportRVForAdb"`
	SupportScreenMirroring    bool   `json:"supportScreenMirroring"`
	SupportScreenTouch        bool   `json:"supportScreenTouch"`
	SupportSyncCorrectTime    bool   `json:"supportSyncCorrectTime"`
	SupportThirdPartyApp      bool   `json:"supportThirdPartyApp"`
	TransportType             int    `json:"transportType"`
	UseBTCallRecords          bool   `json:"useBTCallRecords"`
	UUID                      string `json:"uuid"`
	VersionCode               string `json:"version_code"`
	VersionName               string `json:"version_name"`
	WakeUpWord                string `json:"wakeupWord"`
}

type PhoneConfig struct {
	PxcVersion            string `json:"pxcVersion"`
	PhoneUUID             string `json:"phoneUUID"`
	PhoneBrand            string `json:"phoneBrand"`
	PhoneModel            string `json:"phoneModel"`
	PhoneOsVersion        string `json:"phoneOsVersion"`
	PhoneOs               string `json:"phoneOs"`
	Package               string `json:"package"`
	VersionCode           int    `json:"versionCode"`
	Token                 int    `json:"token"`
	Pubkey                string `json:"pubkey"`
	EncryptedHUID         string `json:"encryptedHUID"`
	BluetoothName         string `json:"bluetoothName"`
	SupportH264IFrame     bool   `json:"supportH264IFrame"`
	AppVersionFingerPrint string `json:"appVersionFingerPrint"`
}

type PXCControl struct {
	port string
	quit chan any

	wg       sync.WaitGroup
	listener net.Listener

	Events      chan PXCResponse
	Errors      chan error
	KeyPair     *KeyPair
	HudConfig   *HUDConfig
	PhoneConfig *PhoneConfig
}

func NewPXCControl(port string, kp *KeyPair, config *PhoneConfig) *PXCControl {
	return &PXCControl{
		port:        port,
		quit:        make(chan any),
		Events:      make(chan PXCResponse),
		Errors:      make(chan error),
		KeyPair:     kp,
		PhoneConfig: config,
	}
}

func (s *PXCControl) decodeHeader(payload []byte) (*PXCResponse, error) {
	if len(payload) < headerSize {
		return nil, errors.New("invalid header")
	}
	return &PXCResponse{
		Command: binary.LittleEndian.Uint32(payload[0:4]),
		Size:    binary.LittleEndian.Uint32(payload[4:8]),
		Magic:   binary.LittleEndian.Uint32(payload[8:12]),
		Token:   binary.LittleEndian.Uint32(payload[12:16]),
		Body:    nil,
	}, nil
}

func (s *PXCControl) buildPC() error {
	if s.HudConfig == nil {
		return errors.New("hudConfig is nil")
	}
	if s.HudConfig.HUID == "" {
		return errors.New("hudConfig.HUID is empty")
	}

	// Set public key
	if pubK, err := s.KeyPair.GetPublicEncoded(); err != nil {
		return err
	} else {
		s.PhoneConfig.Pubkey = pubK
	}

	// Encrypt HUID
	if enc, err := LegacyPrivateEncryptPKCS1v15(&s.KeyPair.PrivateKey, []byte(s.HudConfig.HUID)); err != nil {
		return err
	} else {
		s.PhoneConfig.EncryptedHUID = base64.StdEncoding.EncodeToString(enc)
	}
	return nil
}

func (s *PXCControl) writeResponse(res *PXCResponse, conn net.Conn, raw *[]byte) error {
	if raw == nil && res != nil {
		// -- build header
		res.Token = 0 // Token is always 0 for responses
		res.Size = headerSize
		if len(res.Body) > 0 {
			res.Size = res.Size + uint32(len(res.Body))
		}
		res.Magic = res.Size ^ res.Command

		// -- write header
		if err := binary.Write(conn, binary.LittleEndian, res.Command); err != nil {
			return err
		}
		if err := binary.Write(conn, binary.LittleEndian, res.Size); err != nil {
			return err
		}
		if err := binary.Write(conn, binary.LittleEndian, res.Magic); err != nil {
			return err
		}
		if err := binary.Write(conn, binary.LittleEndian, res.Token); err != nil {
			return err
		}

		// -- write body
		if len(res.Body) > 0 {
			if _, err := conn.Write(res.Body); err != nil {
				return err
			}
		}
	} else if raw != nil {
		fmt.Printf("\nOutput Bytes: %x\n", *raw)
		if _, err := conn.Write(*raw); err != nil {
			return err
		}
	}

	return nil
}

func (s *PXCControl) handleEvent(event *PXCResponse, conn net.Conn) {
	switch event.Command {
	case Handshake:
		response := &PXCResponse{Command: HandshakeOk}
		if err := s.writeResponse(response, conn, nil); err != nil {
			s.Errors <- err
		}
	case HudConf:
		if s.HudConfig != nil {
			break
		}
		// Read HudConfig and store it
		var conf HUDConfig
		if err := json.Unmarshal(event.Body, &conf); err != nil {
			s.Errors <- err
		}
		s.HudConfig = &conf
		// Set encrypted huid to phone config
		if err := s.buildPC(); err != nil {
			s.Errors <- err
		}
		// Respond with phone conf
		if b, err := json.Marshal(s.PhoneConfig); err != nil {
			s.Errors <- err
		} else {
			response := &PXCResponse{Command: PhoneConf, Body: b}
			if err = s.writeResponse(response, conn, nil); err != nil {
				s.Errors <- err
				break
			}
		}
	case SpeedConf:
		response := &PXCResponse{Command: SpeedOk}
		if err := s.writeResponse(response, conn, nil); err != nil {
			s.Errors <- err
		}
	case ClientSet:
		response := &PXCResponse{Command: ClientOk}
		if err := s.writeResponse(response, conn, nil); err != nil {
			s.Errors <- err
		}
	case Heartbeat:
		response := &PXCResponse{Command: HeartbeatOk}
		if err := s.writeResponse(response, conn, nil); err != nil {
			s.Errors <- err
		}
	default:
		if len(event.Body) == 0 {
			response := &PXCResponse{Command: event.Command + 1}
			if err := s.writeResponse(response, conn, nil); err != nil {
				s.Errors <- err
			}
		} else {
			s.Errors <- fmt.Errorf("unknown event Command: %d Body: %x", event.Command, event.Body)
		}
	}
}

func (s *PXCControl) Start() error {
	// Create listener
	ln, err := net.Listen("tcp", s.port)
	if err != nil {
		return err
	}

	s.listener = ln
	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

func (s *PXCControl) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return // Shutting down
			default:
				log.Printf("Error accepting connection: %v", err)
				continue
			}
		}

		log.Printf("New PXC client from %s", conn.RemoteAddr())
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

// Handling a single TCP connection
func (s *PXCControl) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer func(conn net.Conn) {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}(conn)

	reader := bufio.NewReader(conn)

	for {
		var request *PXCResponse

		// Read the 16 byte header
		headerBytes := make([]byte, headerSize)
		if n, err := io.ReadFull(reader, headerBytes); err != nil {
			s.Errors <- fmt.Errorf("error reading header: %v (read %d bytes: %x)", err, n, headerBytes[:n])
			return
		}
		if req, err := s.decodeHeader(headerBytes); err != nil {
			s.Errors <- fmt.Errorf("error decoding header: %v", err)
			return
		} else {
			request = req
		}

		// Sanity check...most...commands
		if request.Command != SpeedConf && request.Command != SpeedOk && request.Command != ClientSet {
			if request.Magic != request.Command+request.Size {
				log.Printf("Sanity check failed: %d %d", request.Magic, request.Command)
				return
			}
		}

		// Read event body if size is greater than 0
		var payload []byte
		if request.Size > 0 {
			payload = make([]byte, request.Size-headerSize)
			if _, err := io.ReadFull(reader, payload); err != nil {
				log.Printf("[TCPService] read payload failed from %s: %v", conn.RemoteAddr(), err)
				return
			}
			request.Body = payload
		}

		// Decide what to do with the event
		s.handleEvent(request, conn)
	}
}

func (s *PXCControl) Stop(ctx context.Context) error {
	// Signal acceptLoop to stop
	close(s.quit)

	// Closing the listener will cause Accept() to error out
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			log.Printf("[TCPService] error closing listener: %v", err)
		}
	}

	// Wait for all goroutines
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		if s.Errors != nil {
			close(s.Errors)
		}
		if s.Events != nil {
			close(s.Events)
		}
		return nil
	}
}
