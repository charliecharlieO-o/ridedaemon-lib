package net

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/charliecharlieO-o/ridedaemon-go/internal/logging"
)

// This protocol is not so weird, it has a 16 byte header plus payload
// standard little endian order and some simple checks, the client (bike)
// establishes connection 2 times, so it's normal for one connection to drop
// after setting up the speed configuration.

const pxcHeaderSize = 16

// Command requests
const (
	PxcHandshake uint32 = 65536
	PxcHudConf   uint32 = 65552
	PxcSpeedConf uint32 = 67216
	PxcHeartbeat uint32 = 1879048192
	PxcClientSet uint32 = 66528

	// Newer CFDL26 firmware sends additional control notifications before it
	// opens the media ports. PXC request commands are even and use cmd+1 as ACK.
	PxcOtaFtpInfo       uint32 = 0x103a0
	PxcMediaFeatureConf uint32 = 0x10020
	PxcCheckSnResult    uint32 = 0x201c0
)

// Command responses
const (
	PxcHandshakeOk uint32 = 65537
	PxcPhoneConf   uint32 = 65553
	PxcSpeedOk     uint32 = 67217
	PxcHeartbeatOk uint32 = 1879048193
	PxcClientOk    uint32 = 66529
	PxcCheckSnAck  uint32 = PxcClientSet + 1
	PxcCheckSnDone uint32 = PxcCheckSnResult + 1
)

type checkSnRequest struct {
	ClientSet string `json:"client_set"`
	Serial    string `json:"sn"`
}

type checkSnResult struct {
	IsOK      bool   `json:"isOk"`
	ErrorCode int    `json:"errCode"`
	ErrorMsg  string `json:"errMsg"`
	ID        string `json:"id"`
	ClientSet string `json:"client_set"`
}

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
	port     string
	quit     chan any
	wg       sync.WaitGroup
	listener net.Listener
	tracker  *ConnTracker

	stopOnce sync.Once

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
		tracker:     NewConnTracker(),
		Events:      make(chan PXCResponse, 16),
		Errors:      make(chan error, 16),
		KeyPair:     kp,
		PhoneConfig: config,
	}
}

func (s *PXCControl) emitEvent(evt PXCResponse) {
	select {
	case s.Events <- evt:
	default:
		// channel full, drop
	}
}

func (s *PXCControl) emitError(err error) {
	select {
	case s.Errors <- err:
	default:
		logging.Printf("Dropping error from PXC [No one's listening!]: %v", err)
	}
}

func (s *PXCControl) decodeHeader(payload []byte) (*PXCResponse, error) {
	if len(payload) < pxcHeaderSize {
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
		res.Token = 0 // Padding is always 0 for responses
		res.Size = pxcHeaderSize
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
		if _, err := conn.Write(*raw); err != nil {
			return err
		}
	}

	return nil
}

func (s *PXCControl) handleEvent(event *PXCResponse, conn net.Conn) {
	switch event.Command {
	case PxcHandshake:
		response := &PXCResponse{Command: PxcHandshakeOk}
		if err := s.writeResponse(response, conn, nil); err != nil {
			s.emitError(&PxcError{PxcWriteErr, err, true})
		}
	case PxcHudConf:
		if s.HudConfig != nil {
			break
		}
		s.emitEvent(*event)
		// Read HudConfig and store it
		var conf HUDConfig
		if err := json.Unmarshal(event.Body, &conf); err != nil {
			s.emitError(&PxcError{PxcHudCfgErr, err, true})
			break
		}
		s.HudConfig = &conf
		// Set encrypted huid to phone config
		if err := s.buildPC(); err != nil {
			s.emitError(&PxcError{PxcHudCfgErr, err, true})
			break
		}
		// Respond with phone conf
		if b, err := json.Marshal(s.PhoneConfig); err != nil {
			s.emitError(&PxcError{PxcHudCfgErr, err, true})
			break
		} else {
			response := &PXCResponse{Command: PxcPhoneConf, Body: b}
			if err = s.writeResponse(response, conn, nil); err != nil {
				s.emitError(&PxcError{PxcWriteErr, err, true})
				break
			}
			s.emitEvent(*response)
		}
	case PxcSpeedConf:
		s.emitEvent(*event)
		response := &PXCResponse{Command: PxcSpeedOk}
		if err := s.writeResponse(response, conn, nil); err != nil {
			s.emitError(&PxcError{PxcWriteErr, err, true})
		}
	case PxcClientSet:
		s.emitEvent(*event)
		ack := &PXCResponse{Command: PxcCheckSnAck}
		if err := s.writeResponse(ack, conn, nil); err != nil {
			s.emitError(&PxcError{PxcWriteErr, err, true})
			break
		}

		var request checkSnRequest
		if len(event.Body) > 0 {
			if err := json.Unmarshal(event.Body, &request); err != nil {
				s.emitError(&PxcError{PxcDecodeErr, fmt.Errorf("decode CHECK_SN: %w", err), false})
			}
		}
		result := checkSnResult{
			IsOK:      true,
			ErrorCode: 0,
			ErrorMsg:  "",
			ID:        request.Serial,
			ClientSet: request.ClientSet,
		}
		if result.ClientSet == "" {
			result.ClientSet = "easy_conn"
		}
		body, err := json.Marshal(result)
		if err != nil {
			s.emitError(&PxcError{PxcWriteErr, err, true})
			break
		}
		response := &PXCResponse{Command: PxcCheckSnResult, Body: body}
		if err := s.writeResponse(response, conn, nil); err != nil {
			s.emitError(&PxcError{PxcWriteErr, err, true})
		}
	case PxcHeartbeat:
		s.emitEvent(*event) // Necessary to indicate PXC finished
		response := &PXCResponse{Command: PxcHeartbeatOk}
		if err := s.writeResponse(response, conn, nil); err != nil {
			s.emitError(&PxcError{PxcWriteErr, err, true})
		}
	case PxcCheckSnDone:
		// The bike acknowledges the phone-originated CHECK_SN_RESULT.
		s.emitEvent(*event)
	default:
		if event.Command&1 == 0 {
			// Unknown even commands are requests. CFDL26 uses several JSON and
			// binary notifications here and will not open 10921/10920 until ACKed.
			logging.Printf(
				"Acknowledging unhandled PXC request command=0x%x bodyBytes=%d",
				event.Command,
				len(event.Body),
			)
			s.emitEvent(*event)
			response := &PXCResponse{Command: event.Command + 1}
			if err := s.writeResponse(response, conn, nil); err != nil {
				s.emitError(&PxcError{PxcWriteErr, err, true})
			}
		} else {
			// Unknown odd commands are responses. Never answer them, otherwise
			// two peers using cmd+1 acknowledgements can create an ACK loop.
			logging.Printf(
				"Ignoring unhandled PXC response command=0x%x bodyBytes=%d",
				event.Command,
				len(event.Body),
			)
			s.emitEvent(*event)
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
				logging.Printf("Error accepting connection: %v", err)
				continue
			}
		}

		logging.Printf("New PXC client from %s", conn.RemoteAddr())
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

// Handling a single TCP connection
func (s *PXCControl) handleConn(conn net.Conn) {
	s.tracker.Add(conn)
	defer func() {
		s.tracker.Remove(conn)
		_ = conn.Close()
		s.wg.Done()
	}()

	reader := bufio.NewReader(conn)

	logging.Printf("Starting PXC Loop")
	defer logging.Printf("Stopping PXC Loop")
	for {
		var request *PXCResponse

		// Read the 16 byte header
		headerBytes := make([]byte, pxcHeaderSize)
		if n, err := io.ReadFull(reader, headerBytes); err != nil {
			s.emitError(&PxcError{
				PxcDecodeErr,
				fmt.Errorf("error reading header: %v (read %d bytes: %x)", err, n, headerBytes[:n]),
				true,
			})
			return
		}
		if req, err := s.decodeHeader(headerBytes); err != nil {
			s.emitError(&PxcError{
				PxcDecodeErr,
				fmt.Errorf("error decoding header: %v", err),
				true,
			})
			return
		} else {
			request = req
		}

		// Sanity check
		if request.Magic != request.Size^request.Command {
			logging.Printf("Sanity check failed: %d %d", request.Magic, request.Command)
			return
		}

		// Read event body if size is greater than 0
		var payload []byte
		if request.Size > 0 {
			payload = make([]byte, request.Size-pxcHeaderSize)
			if _, err := io.ReadFull(reader, payload); err != nil {
				s.emitError(&PxcError{
					PxcDecodeErr,
					fmt.Errorf("[PXCService] read payload failed from %s: %v", conn.RemoteAddr(), err),
					true,
				})
				return
			}
			request.Body = payload
		}

		// Decide what to do with the event
		s.handleEvent(request, conn)
	}
}

func (s *PXCControl) Stop(ctx context.Context) error {
	logging.Printf("Stopping pxc server")
	// Signal acceptLoop to stop
	s.stopOnce.Do(func() {
		close(s.quit)
		if s.listener != nil {
			if err := s.listener.Close(); err != nil {
				logging.Printf("[TCPService] error closing listener: %v", err)
			}
		}
	})

	// Close all tcp connections
	s.tracker.CloseAll()

	// Wait for all goroutines
	done := make(chan any)
	go func() {
		logging.Printf("Waiting for PXC routines to vacate")
		s.wg.Wait()
		logging.Printf("PXC routines exited")
		close(done)
	}()

	select {
	case <-ctx.Done():
		logging.Printf("PXC ctx timeout")
		return ctx.Err()
	case <-done:
		logging.Printf("PXC stream closed through done channel")
		if s.Errors != nil {
			close(s.Errors)
		}
		if s.Events != nil {
			close(s.Events)
		}
		return nil
	}
}
