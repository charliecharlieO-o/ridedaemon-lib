package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type FrameSource interface {
	NextFrame(now time.Time) ([]byte, error)
}

type LiveSource interface {
	FrameSource
	IsActive(now time.Time) bool
}

type connectionState struct {
	frameCounter uint32
	pollCount    uint64
}

func buildFramedPacket(body []byte, frameCounter uint32) ([]byte, uint32) {
	idx := frameCounter

	// 4B index
	idxBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(idxBytes, idx)

	// 4B len
	totalLen := uint32(len(idxBytes) + len(body))
	lenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBytes, totalLen)

	// data
	frame := make([]byte, 0, mediaStepFrameSize+len(body))
	frame = append(frame, lenBytes...)
	frame = append(frame, idxBytes...)
	frame = append(frame, body...)

	return frame, idx
}

func sendChunked(w *bufio.Writer, frame []byte, chunkSize int, sleep time.Duration) error {
	// no chunking needed
	if chunkSize <= 0 {
		if _, err := w.Write(frame); err != nil {
			return err
		}
		return w.Flush()
	}

	offset := 0
	for offset < len(frame) {
		end := offset + chunkSize
		if end > len(frame) {
			end = len(frame)
		}

		// Write chunk :p
		if _, err := w.Write(frame[offset:end]); err != nil {
			return err
		}
		if err := w.Flush(); err != nil {
			return err
		}

		offset = end
		if sleep > 0 {
			time.Sleep(sleep)
		}
	}
	return nil
}

// Step frame 72 00 00 00 00 00 00 00 - 8 bytes
const mediaStepFrameSize = 8

type MediaStream struct {
	port     string
	quit     chan any
	wg       sync.WaitGroup
	listener net.Listener

	// Shared frame source (temp)
	src FrameSource

	// Config
	chunkSize  int           // e.g 0x1000
	chunkSleep time.Duration // e.g 3 * time.Millisecond

	// Interface events
	Errors chan error
}

func NewMediaStream(port string, src FrameSource, chunkSize int, chunkSleep time.Duration) *MediaStream {
	return &MediaStream{
		port:       port,
		quit:       make(chan any),
		Errors:     make(chan error),
		src:        src,
		chunkSize:  chunkSize,
		chunkSleep: chunkSleep,
	}
}

func (s *MediaStream) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return // Normal shut down
			default:
				log.Printf("Error accepting connection: %s", err)
				continue
			}
		}

		log.Printf("New MediaStream client from %s", conn.RemoteAddr())
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

// Handling individual TCP Connection/Client
func (s *MediaStream) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer func(conn net.Conn) {
		if err := conn.Close(); err != nil {
			log.Printf("MediaStream: Error closing connection: %v", err)
		}
	}(conn)

	// Keep a fast & stead connection
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
		_ = tcpConn.SetKeepAlive(true)
	}

	input := bufio.NewReaderSize(conn, 8*1024)
	output := bufio.NewWriterSize(conn, 64*1024)
	defer func() {
		if err := output.Flush(); err != nil {
			log.Printf("MediaStream: Error flushing output: %v", err)
		}
	}()

	header := make([]byte, mediaStepFrameSize)
	zero4 := []byte{0, 0, 0, 0}

	st := &connectionState{
		frameCounter: 0,
		pollCount:    0,
	}

	for {
		// Read 8 bytes (poll header)
		if _, err := io.ReadFull(input, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				log.Printf("MediaStream: Error reading header: %v", err)
				return
			}
			log.Printf("MediaStream: Unknown error reading header: %v", err)
			return
		}

		cmd := binary.LittleEndian.Uint16(header[0:2])

		if cmd != 0x0072 {
			// Not a poll pacing command, discard and send idle 0's
			log.Printf("MediaStream: non-0x0072 cmd %04x, sending idle", cmd)
			if _, err := output.Write(zero4); err != nil {
				log.Printf("MediaStream: error writing idle: %v", err)
				return
			}
			if err := output.Flush(); err != nil {
				log.Printf("MediaStream: error flushing idle: %v", err)
				return
			}
			continue
		}

		st.pollCount++

		body, err := s.src.NextFrame(time.Now())
		if err != nil {
			log.Printf("MediaStream: error reading frame: %v", err)
			// we will send an idle 0s body so the connection stays open
			if _, err = output.Write(zero4); err != nil {
				log.Printf("MediaStream: error writing idle after src failure: %v", err)
				return
			}
			if err = output.Flush(); err != nil {
				log.Printf("MediaStream: error flushing idle after src failure: %v", err)
				return
			}
			continue
		}

		// If no payload is available, send idle 0s
		if body == nil || len(body) == 0 {
			if _, err = output.Write(zero4); err != nil {
				log.Printf("MediaStream: error writing idle: %v", err)
				return
			}
			if err = output.Flush(); err != nil {
				log.Printf("MediaStream: flush error (idle): %v", err)
				return
			}
			continue
		}

		// Legacy pacing - 4B len + 4B idx + body
		frame, idx := buildFramedPacket(body, st.frameCounter)
		st.frameCounter = (st.frameCounter + 1) & 0x7FFFFFFF

		// Send it chunked, send it paced
		if err = sendChunked(output, frame, s.chunkSize, s.chunkSleep); err != nil {
			log.Printf("MediaStream: error sending chunks (idx=%d): %v", idx, err)
			return
		}
	}
}

func (s *MediaStream) Start() error {
	// Create listener
	ln, err := net.Listen("tcp", s.port)
	if err != nil {
		return err
	}

	s.listener = ln
	s.wg.Add(1)
	go s.acceptLoop()
	log.Println("Media Stream started")
	return nil
}

func (s *MediaStream) Stop(ctx context.Context) error {
	// Signal connection accept loop to stop
	close(s.quit)

	// Close listener
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			log.Printf("[TCPService] error closing listener: %v", err)
		}
	}

	// Wait for go routines to vacate
	done := make(chan any)
	go func() {
		s.wg.Wait()
		close(done)
	}()

	// Either timeout with context or go routines vacate and we close normally
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		if s.Errors != nil {
			close(s.Errors)
		}
		return nil
	}
}
