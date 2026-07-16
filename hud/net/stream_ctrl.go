package net

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/charliecharlieO-o/ridedaemon-go/internal/logging"
)

const mediaCtrlHeaderSize = 8

// Command requests
const (
	MediaCtrlInit       uint16 = 16
	MediaCtrlScreenConf uint16 = 96
	MediaCtrlChk        uint16 = 112
	MediaCtrlPing       uint16 = 64
)

// Command responses
const (
	MediaCtrlAck       uint16 = 17
	MediaCtrlViewState uint16 = 97
	MediaCtrlRcv       uint16 = 113
	MediaCtrlPong      uint16 = 65
)

type MediaCtrlResponse struct {
	Command uint16
	Size    uint16
	Padding uint32
	Payload []byte
}

type ViewConfig struct {
	State int `json:"state"`
}

type View struct {
	ViewAreaConfig  ViewConfig `json:"viewAreaConfig"`
	SupportFunction int        `json:"supportFunction"`
}

type MediaControl struct {
	port     string
	quit     chan any
	wg       sync.WaitGroup
	listener net.Listener
	tracker  *ConnTracker

	stopOnce sync.Once

	Errors chan error
	Events chan MediaCtrlResponse
}

func NewMediaControl(port string) *MediaControl {
	return &MediaControl{
		port:    port,
		quit:    make(chan any),
		tracker: NewConnTracker(),
		Errors:  make(chan error, 16),
		Events:  make(chan MediaCtrlResponse, 16),
	}
}

func (s *MediaControl) emitEvent(evt MediaCtrlResponse) {
	select {
	case s.Events <- evt:
	default:
		// channel full, drop
	}
}

func (s *MediaControl) emitError(err error) {
	select {
	case s.Errors <- err:
	default:
		logging.Printf("Dropping error from MediaControl [No one's listening!]: %v", err)
	}
}

func (s *MediaControl) decodeHeader(b []byte) (*MediaCtrlResponse, error) {
	if len(b) < mediaCtrlHeaderSize {
		return nil, errors.New("invalid header")
	}
	return &MediaCtrlResponse{
		Command: binary.LittleEndian.Uint16(b[0:2]),
		Size:    binary.LittleEndian.Uint16(b[2:4]),
		Padding: binary.LittleEndian.Uint32(b[4:8]),
	}, nil
}

func (s *MediaControl) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return // Normal shut down
			default:
				logging.Printf("Error accepting connection: %s", err)
				continue
			}
		}

		logging.Printf("New MediaControl client from %s", conn.RemoteAddr())
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *MediaControl) writeResponse(res *MediaCtrlResponse, conn net.Conn) error {
	// -- write header
	if err := binary.Write(conn, binary.LittleEndian, res.Command); err != nil {
		return err
	}
	if err := binary.Write(conn, binary.LittleEndian, res.Size); err != nil {
		return err
	}
	if err := binary.Write(conn, binary.LittleEndian, res.Padding); err != nil {
		return err
	}

	// -- write payload
	if len(res.Payload) > 0 {
		if _, err := conn.Write(res.Payload); err != nil {
			return err
		}
	}
	return nil
}

func (s *MediaControl) handleEvent(event *MediaCtrlResponse, conn net.Conn) {
	switch event.Command {
	case MediaCtrlInit:
		s.emitEvent(*event)
		payload := buildMediaCaptureAckPayload(event.Payload)
		response := &MediaCtrlResponse{Command: MediaCtrlAck, Size: uint16(len(payload)), Payload: payload}
		if err := s.writeResponse(response, conn); err != nil {
			s.emitError(&CtrlError{CtrlWriteErr, err, true})
			break
		}
	case MediaCtrlScreenConf:
		s.emitEvent(*event)
		viewState := View{
			ViewAreaConfig:  ViewConfig{State: 0},
			SupportFunction: 0,
		}
		var payload []byte
		if p, err := json.Marshal(viewState); err != nil {
			s.emitError(&CtrlError{CtrlDecodeErr, err, true})
			break
		} else {
			payload = p
		}
		response := &MediaCtrlResponse{Command: MediaCtrlViewState, Size: uint16(len(payload)), Payload: payload}
		if err := s.writeResponse(response, conn); err != nil {
			s.emitError(&CtrlError{CtrlWriteErr, err, true})
			break
		}
	case MediaCtrlChk:
		response := &MediaCtrlResponse{Command: MediaCtrlRcv, Size: 0}
		if err := s.writeResponse(response, conn); err != nil {
			s.emitError(&CtrlError{CtrlWriteErr, err, true})
			break
		}
	case MediaCtrlPing:
		response := &MediaCtrlResponse{Command: MediaCtrlPong, Size: 0}
		if err := s.writeResponse(response, conn); err != nil {
			s.emitError(&CtrlError{CtrlWriteErr, err, true})
			break
		}
	default:
		s.emitEvent(*event)
		// Try to send a default command + 1 empty response
		response := &MediaCtrlResponse{Command: event.Command + 1, Size: 0}
		if err := s.writeResponse(response, conn); err != nil {
			s.emitError(&CtrlError{CtrlWriteErr, err, true})
			break
		}
	}
}

func buildMediaCaptureAckPayload(request []byte) []byte {
	encoder := uint32(2)
	width := uint16(800)
	height := uint16(384)
	extendedProtocol := byte(1)

	if len(request) >= 4 {
		requestedWidth := binary.LittleEndian.Uint16(request[0:2])
		requestedHeight := binary.LittleEndian.Uint16(request[2:4])
		if aligned := requestedWidth &^ 0x0f; aligned >= 16 {
			width = aligned
		}
		if aligned := requestedHeight &^ 0x0f; aligned >= 16 {
			height = aligned
		}
	}
	if len(request) >= 12 {
		if requestedEncoder := binary.LittleEndian.Uint32(request[8:12]); requestedEncoder != 0 {
			encoder = requestedEncoder
		}
	}
	if len(request) >= 30 {
		extendedProtocol = request[29]
	}

	payload := make([]byte, 9)
	binary.LittleEndian.PutUint32(payload[0:4], encoder)
	binary.LittleEndian.PutUint16(payload[4:6], width)
	binary.LittleEndian.PutUint16(payload[6:8], height)
	payload[8] = extendedProtocol
	logging.Printf(
		"Media capture negotiated encoder=%d width=%d height=%d extended=%d",
		encoder,
		width,
		height,
		extendedProtocol,
	)
	return payload
}

// Handling individual TCP Connection/Client
func (s *MediaControl) handleConn(conn net.Conn) {
	s.tracker.Add(conn)
	defer func() {
		s.tracker.Remove(conn)
		_ = conn.Close()
		s.wg.Done()
	}()

	reader := bufio.NewReader(conn)

	logging.Printf("Starting StreamCtrl Loop")
	defer logging.Printf("Stopping StreamCtrl Loop")
	for {
		var request *MediaCtrlResponse

		// Read 8 byte header
		headerBytes := make([]byte, mediaCtrlHeaderSize)
		if n, err := io.ReadFull(reader, headerBytes); err != nil {
			s.emitError(
				&CtrlError{
					CtrlDecodeErr,
					fmt.Errorf("error reading header: %v (read %d bytes: %x)", err, n, headerBytes[:n]),
					true,
				})
			return
		}
		if req, err := s.decodeHeader(headerBytes); err != nil {
			s.emitError(
				&CtrlError{
					CtrlDecodeErr,
					fmt.Errorf("error decoding header: %v", err),
					true,
				})
			return
		} else {
			request = req
		}

		// Read body if payload is greater than 0
		var payload []byte
		if request.Size > 0 {
			payload = make([]byte, request.Size)
			if _, err := io.ReadFull(reader, payload); err != nil {
				s.emitError(
					&CtrlError{
						CtrlDecodeErr,
						fmt.Errorf("[MediaControl] read payload failed from %s: %v", conn.RemoteAddr(), err),
						true,
					})
				return
			}
			request.Payload = payload
		}

		// Decide what to do with the request
		s.handleEvent(request, conn)
	}
}

func (s *MediaControl) Start() error {
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

func (s *MediaControl) Stop(ctx context.Context) error {
	logging.Printf("Stopping media control")
	// Signal connection accept loop to stop
	s.stopOnce.Do(func() {
		close(s.quit)
		// Close listener
		if s.listener != nil {
			if err := s.listener.Close(); err != nil {
				logging.Printf("[TCPService] error closing listener: %v", err)
			}
		}
	})

	// Close all tcp connections
	s.tracker.CloseAll()

	// Wait for go routines to vacate
	done := make(chan any)
	go func() {
		logging.Printf("Waiting for Stream Ctrl routines to vacate")
		s.wg.Wait()
		logging.Printf("Stream Ctrl routines vacated")
		close(done)
	}()

	// Either timeout with context or go routines vacate and we close normally
	select {
	case <-ctx.Done():
		logging.Printf("Stream Ctrl ctx timeout")
		return ctx.Err()
	case <-done:
		logging.Printf("Stream Ctrl closed through done channel")
		if s.Errors != nil {
			close(s.Errors)
		}
		if s.Events != nil {
			close(s.Events)
		}
		return nil
	}
}
