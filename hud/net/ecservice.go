package net

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

const initCmd int = 16
const initOk int = 17

type ECResponse struct {
	Code      int
	Separator uint16
	Size      int
	Magic     int
	Body      []byte
}

type ECService struct {
	ip          string
	port        string
	packageName string
	phoneType   string
}

func NewECService(ip, port, packageName, phoneType string) *ECService {
	return &ECService{ip, port, packageName, phoneType}
}

func (s *ECService) encodePayload(payload *ECResponse) []byte {
	ttlLen := len(payload.Body) + 16
	magic := ttlLen ^ 16 // 16 is the header size
	header := make([]byte, 16)

	binary.LittleEndian.PutUint16(header[0:], uint16(payload.Code))
	binary.LittleEndian.PutUint16(header[3:], payload.Separator)
	binary.LittleEndian.PutUint16(header[4:], uint16(ttlLen))
	binary.LittleEndian.PutUint16(header[8:], uint16(magic))
	binary.LittleEndian.PutUint16(header[11:], payload.Separator)

	return append(header, payload.Body...)
}

func (s *ECService) decodePayload(payload []byte) (*ECResponse, error) {
	if len(payload) < 16 {
		return nil, fmt.Errorf("payload too short")
	}

	response := &ECResponse{}
	header := payload[:16]
	body := payload[16:]

	// Sanity check there's a newline defining the end of the payload
	newlineIdx := bytes.IndexByte(body, '\n')
	if newlineIdx == -1 {
		return nil, fmt.Errorf("no newline terminator found (0x0a)")
	}

	// Process header
	response.Code = int(binary.LittleEndian.Uint16(header[0:2]))
	response.Size = int(binary.LittleEndian.Uint16(header[4:6]))
	response.Separator = binary.LittleEndian.Uint16(header[11:13])
	response.Magic = int(binary.LittleEndian.Uint16(header[8:10]))

	// Process body
	response.Body = body[:newlineIdx]

	return response, nil
}

// readECResponse reads one complete EC packet. A TCP read can return any prefix
// of a packet, so the response must be assembled using the advertised length.
func readECResponse(r io.Reader) (*ECResponse, error) {
	header := make([]byte, 16)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	totalLen := int(binary.LittleEndian.Uint16(header[4:6]))
	if totalLen < len(header) || totalLen > 64*1024 {
		return nil, fmt.Errorf("invalid EC response length: %d", totalLen)
	}
	body := make([]byte, totalLen-len(header))
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return (&ECService{}).decodePayload(append(header, body...))
}

func (s *ECService) InitStreamCmd() error {
	addr := net.JoinHostPort(s.ip, s.port)
	conn, err := net.DialTimeout("tcp", addr, time.Second*5)
	if err != nil {
		return err
	}
	return s.initStreamCmd(conn)
}

// InitStreamCmdWithConn performs the normal EasyConn init handshake over a caller-owned route.
// Android callers use this to pass a socket opened by Network.socketFactory.
func (s *ECService) InitStreamCmdWithConn(conn net.Conn) error {
	if conn == nil {
		return fmt.Errorf("nil EC init connection")
	}
	return s.initStreamCmd(conn)
}

func (s *ECService) initStreamCmd(conn net.Conn) (err error) {
	defer func(conn net.Conn) {
		if err = conn.Close(); err != nil {
			log.Println(err)
		}
	}(conn)

	// Send init payload
	if err = conn.SetDeadline(time.Now().Add(time.Second * 5)); err != nil {
		return fmt.Errorf("failed to set io deadline: %s", err)
	}

	var payload []byte
	init := struct {
		PhoneType   string `json:"phoneType"`
		PackageName string `json:"packageName"`
	}{s.phoneType, s.packageName}
	payload, err = json.Marshal(init)
	if err != nil {
		return fmt.Errorf("failed to marshal init payload: %s", err)
	}
	payload = s.encodePayload(&ECResponse{
		Code:      initCmd,
		Separator: 0x70,
		Body:      payload,
	})

	// Write init payload over tcp
	if _, err = conn.Write(payload); err != nil {
		return fmt.Errorf("failed to write to %s: %w", s.ip, err)
	}

	// Receive one complete response. TCP packet boundaries are not protocol boundaries.
	var response *ECResponse
	response, err = readECResponse(conn)
	if err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// make sure the response was successful
	if response != nil && response.Code == initOk {
		return nil
	} else if response == nil {
		return fmt.Errorf("no response")
	} else {
		var data map[string]interface{}
		decodeErr := json.Unmarshal(response.Body, &data)
		if decodeErr != nil {
			return fmt.Errorf(
				"unsuccessful EC response: code=%d, decoerr=%s", response.Code, decodeErr,
			)
		}
		return fmt.Errorf(
			"unsuccessful EC response: code=%d, body=%s", response.Code, data,
		)
	}
}
