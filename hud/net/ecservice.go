package net

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
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
		return nil, fmt.Errorf("now newline terminator found (0x0a)")
	}

	// Process header
	response.Code = int(header[0])
	response.Size = int(header[4])
	response.Separator = uint16(header[11])
	response.Magic = int(header[8])

	// Process body
	response.Body = body[:newlineIdx]

	return response, nil
}

func (s *ECService) InitStreamCmd() error {
	// Connect to EC Service
	addr := net.JoinHostPort(s.ip, s.port)
	conn, err := net.DialTimeout("tcp", addr, time.Second*5)
	if err != nil {
		return err
	}
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

	// receive & decode response
	var n int
	buf := make([]byte, 64)
	n, err = conn.Read(buf)
	if err != nil {
		return fmt.Errorf("failed to read from %s: %w", s.ip, err)
	}

	var response *ECResponse
	raw := buf[:n]
	response, err = s.decodePayload(raw)
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
