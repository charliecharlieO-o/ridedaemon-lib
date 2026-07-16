package stream

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type FrameSource interface {
	NextFrame(now time.Time) ([]byte, error)
}

type LiveSource interface {
	FrameSource
	IsActive(now time.Time) bool
	PushFrame(au []byte)
	PrepareForConsumer()
}

var streamHeader = []byte{
	0x02, 0x00, 0x00, 0x00,
	0x20, 0x03, 0x80, 0x01,
}
var audNAL = []byte{0x00, 0x00, 0x00, 0x01, 0x09, 0xF0}

func splitByAudAnnexB(data []byte) [][]byte {
	var starts []int
	i := 0

	isStartCode := func(pos int) int {
		if pos+3 < len(data) &&
			data[pos] == 0x00 &&
			data[pos+1] == 0x00 &&
			data[pos+2] == 0x01 {
			return 3
		}
		if pos+4 < len(data) &&
			data[pos] == 0x00 &&
			data[pos+1] == 0x00 &&
			data[pos+2] == 0x00 &&
			data[pos+3] == 0x01 {
			return 4
		}
		return 0
	}

	for i < len(data) {
		startCode := isStartCode(i)
		if startCode != 0 {
			nalPos := i + startCode
			if nalPos < len(data) {
				nal := int(data[nalPos] & 0x1F)
				if nal == 9 { // AUD
					starts = append(starts, i)
				}
			}
			i += startCode
		} else {
			i++
		}
	}

	if len(starts) == 0 {
		return nil
	}

	out := make([][]byte, 0, len(starts))
	for idx := 0; idx < len(starts); idx++ {
		begin := starts[idx]
		end := len(data)
		if idx+1 < len(starts) {
			end = starts[idx+1]
		}

		// Copy the slice so the array can be garbage collected and reused safely
		chunk := make([]byte, end-begin)
		copy(chunk, data[begin:end])
		out = append(out, chunk)
	}

	return out
}

func startsWithStreamHeader(b, header []byte) bool {
	if len(b) < len(header) {
		return false
	}
	for i := range header {
		if b[i] != header[i] {
			return false
		}
	}
	return true
}

func concatBytes(parts ...[]byte) []byte {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	out := make([]byte, total)
	pos := 0
	for _, p := range parts {
		copy(out[pos:], p)
		pos += len(p)
	}
	return out
}

type FileFrameSource struct {
	mu sync.Mutex

	aus       [][]byte // AUD-delimited access units
	replayIdx int
	pollCount uint64

	lastAu       []byte
	lastAdvance  time.Time
	interval     time.Duration // 1/target fps
	streamHeader []byte        // optional SPS/PPS/etc to prepend for first poll (warmup)
}

// NewFileFrameSource loads a .h264 file, splits it into AUD-delim AUs
// annex-b and creates a replay source.
func NewFileFrameSource(path string, targetFPS int) (*FileFrameSource, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading h264 file %q: %w", path, err)
	}

	aus := splitByAudAnnexB(data)
	if len(aus) == 0 {
		return nil, fmt.Errorf("no AUD-delimited AUs found in %q", path)
	}

	if targetFPS <= 0 {
		targetFPS = 15
	}

	interval := time.Second / time.Duration(targetFPS)
	return &FileFrameSource{
		aus:          aus,
		replayIdx:    0,
		pollCount:    0,
		lastAu:       nil,
		lastAdvance:  time.Time{},
		interval:     interval,
		streamHeader: append([]byte(nil), streamHeader...), // copy bytes
	}, nil
}

func (s *FileFrameSource) NextFrame(now time.Time) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Nothing to send
	if len(s.aus) == 0 {
		return nil, nil
	}

	firstPoll := s.pollCount == 0

	shouldAdvAU := false
	if s.lastAu == nil {
		shouldAdvAU = true
	} else {
		if s.lastAdvance.IsZero() || now.Sub(s.lastAdvance) >= s.interval {
			shouldAdvAU = true
		}
	}

	if shouldAdvAU {
		au := s.aus[s.replayIdx%len(s.aus)]
		s.replayIdx++

		// Prepend stream header on first payload if absent
		if firstPoll && !startsWithStreamHeader(au, s.streamHeader) {
			au = concatBytes(s.streamHeader, au)
		}

		s.lastAu = au
		s.lastAdvance = now
		s.pollCount++

		return au, nil
	}

	// Idle poll - keep cadence, no new frame
	s.pollCount++
	return nil, nil
}

type RawFrameSource struct {
	FileFrameSource
}

func NewRawFrameSource(src []byte, targetFPS int) (*RawFrameSource, error) {
	aus := splitByAudAnnexB(src)
	if len(aus) == 0 {
		return nil, errors.New("no AUD-delimited AUs found in source")
	}

	if targetFPS <= 0 {
		targetFPS = 15
	}

	interval := time.Second / time.Duration(targetFPS)
	return &RawFrameSource{
		FileFrameSource: FileFrameSource{
			aus:          aus,
			replayIdx:    0,
			pollCount:    0,
			lastAu:       nil,
			lastAdvance:  time.Time{},
			interval:     interval,
			streamHeader: append([]byte(nil), streamHeader...),
		},
	}, nil
}

type LiveStreamSource struct {
	mu sync.Mutex

	// Ring buffer for queued AUs for efficiency purposes
	aus      [][]byte
	head     int
	tail     int
	count    int
	capacity int

	lastAU      []byte // Freeze frame
	lastAdvance time.Time
	interval    time.Duration // 1/fps

	active      bool
	lastInput   time.Time
	liveTimeout time.Duration
	awaitIDR    bool
}

func NewLiveStreamSource(targetFPS int, liveTimeout time.Duration, maxQ int) *LiveStreamSource {
	if targetFPS <= 0 {
		targetFPS = 15
	}
	if liveTimeout <= 0 {
		liveTimeout = 3 * time.Second
	}
	if maxQ <= 0 {
		maxQ = 15
	}
	return &LiveStreamSource{
		aus:         make([][]byte, maxQ),
		capacity:    maxQ,
		lastAU:      nil,
		lastAdvance: time.Time{},
		interval:    time.Second / time.Duration(targetFPS),
		active:      false,
		lastInput:   time.Time{},
		liveTimeout: liveTimeout,
		awaitIDR:    true,
	}
}

// PushFrame is called by a live encoder, expected H.264 AU in Annex B aud + sps/pps + idr
func (s *LiveStreamSource) PushFrame(au []byte) {
	if len(au) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	s.active = true
	s.lastInput = now
	if s.awaitIDR {
		if !hasAnnexBNALType(au, 5) {
			return
		}
		s.awaitIDR = false
	}

	if s.count == s.capacity {
		// Drop the oldest frame
		s.aus[s.head] = nil
		s.head = (s.head + 1) % s.capacity
		s.count--
	}

	buf := append([]byte(nil), au...) // copy buffer so memory can be reused

	s.aus[s.tail] = buf
	s.tail = (s.tail + 1) % s.capacity
	s.count++
}

// PrepareForConsumer clears stale predictive frames and waits for a fresh IDR.
func (s *LiveStreamSource) PrepareForConsumer() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.aus {
		s.aus[i] = nil
	}
	s.count = 0
	s.head = 0
	s.tail = 0
	s.lastAU = nil
	s.lastAdvance = time.Time{}
	s.awaitIDR = true
}

func (s *LiveStreamSource) IsActive(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.active {
		return false
	}

	if !s.lastInput.IsZero() && now.Sub(s.lastInput) > s.liveTimeout {
		s.resetLocked()
		return false
	}
	return true
}

func (s *LiveStreamSource) resetLocked() {
	s.active = false
	s.lastAU = nil
	s.lastAdvance = time.Time{}
	s.count = 0
	s.head = 0
	s.tail = 0
	s.awaitIDR = true
}

func hasAnnexBNALType(data []byte, wanted byte) bool {
	for i := 0; i+3 < len(data); {
		startCodeLen := 0
		if data[i] == 0 && data[i+1] == 0 && data[i+2] == 1 {
			startCodeLen = 3
		} else if i+4 < len(data) && data[i] == 0 && data[i+1] == 0 &&
			data[i+2] == 0 && data[i+3] == 1 {
			startCodeLen = 4
		}
		if startCodeLen == 0 {
			i++
			continue
		}
		nalIndex := i + startCodeLen
		if nalIndex < len(data) && data[nalIndex]&0x1f == wanted {
			return true
		}
		i = nalIndex + 1
	}
	return false
}

// NextFrame fits our FrameSource interface's NextFrame signature.
func (s *LiveStreamSource) NextFrame(now time.Time) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Active checks
	if !s.active {
		return nil, nil
	}
	if !s.lastInput.IsZero() && now.Sub(s.lastInput) > s.liveTimeout {
		s.resetLocked()
		return nil, nil
	}

	// Advance to a new AU based on target FPS
	shouldAdvAU := false
	if s.lastAU == nil {
		shouldAdvAU = true
	} else if s.lastAdvance.IsZero() || now.Sub(s.lastAdvance) >= s.interval {
		shouldAdvAU = true
	}

	if shouldAdvAU {
		// Pop the next live AU
		if s.count > 0 {
			au := s.aus[s.head]
			s.aus[s.head] = nil // give the garbage collector a hand ;)
			s.head = (s.head + 1) % s.capacity
			s.count--

			s.lastAU = au
			s.lastAdvance = now
			return au, nil
		}

		// No queued frames? we can repeat lastAU or return nil
		if s.lastAU != nil {
			s.lastAdvance = now
			return s.lastAU, nil
		}
		return nil, nil
	}

	return nil, nil // idle but we keep cadence
}

// extractAusFromStream scans `buf` for AUD-delimited AUs in Annex B format
// and returns (completeAUs, remainingBytes).
//
// It assumes NALs are prefixed by 0x000001 or 0x00000001, and treats
// each AUD (nal_unit_type == 9) as the start of an AU.
// Everything from one AUD start to the next AUD start is an AU.
func extractAusFromStream(buf []byte) ([][]byte, []byte) {
	const nalAud = 9

	var audStarts []int

	// Find all start codes followed by AUD NAL
	i := 0
	for i+3 < len(buf) {
		// 3-byte start code: 0x000001
		if buf[i] == 0 && buf[i+1] == 0 && buf[i+2] == 1 {
			nalHeaderIdx := i + 3
			if nalHeaderIdx < len(buf) {
				nalType := buf[nalHeaderIdx] & 0x1F
				if nalType == nalAud {
					audStarts = append(audStarts, i)
				}
			}
			i += 3
			continue
		}

		// 4-byte start code: 0x00000001
		if i+4 < len(buf) &&
			buf[i] == 0 && buf[i+1] == 0 && buf[i+2] == 0 && buf[i+3] == 1 {
			nalHeaderIdx := i + 4 // first byte after start code
			if nalHeaderIdx < len(buf) {
				nalType := buf[nalHeaderIdx] & 0x1F
				if nalType == nalAud {
					audStarts = append(audStarts, i)
				}
			}
			i += 4
			continue
		}

		i++
	}

	// No AUD at all: nothing we can form into AUs yet.
	if len(audStarts) == 0 {
		return nil, buf
	}

	// Only one AUD: treat everything from that AUD onward as "maybe incomplete".
	if len(audStarts) == 1 {
		return nil, buf[audStarts[0]:]
	}

	// Build complete AUs between successive AUDs: [AUD_i, AUD_{i+1})
	aus := make([][]byte, 0, len(audStarts)-1)
	for j := 0; j < len(audStarts)-1; j++ {
		start := audStarts[j]
		end := audStarts[j+1]
		if start < end && end <= len(buf) {
			// Copy out so caller can safely reuse original buffer.
			auCopy := append([]byte(nil), buf[start:end]...)
			aus = append(aus, auCopy)
		}
	}

	// Remaining data from last AUD to end (possibly incomplete AU).
	remaining := append([]byte(nil), buf[audStarts[len(audStarts)-1]:]...)

	return aus, remaining
}

// feedStreamToLiveSource reads io buffer data and feeds it to the LiveStreamSource in chunks
func feedStreamToLiveSource(r io.Reader, src *LiveStreamSource) error {
	const chunkSize = 4096
	buf := make([]byte, chunkSize)
	var leftover []byte

	for {
		n, err := r.Read(buf)
		if n > 0 {
			chunk := append(leftover, buf[:n]...)
			aus, rem := extractAusFromStream(chunk)
			leftover = rem

			for _, au := range aus {
				src.PushFrame(au)
			}
		}

		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

type MuxSource struct {
	stopAll  atomic.Bool
	Live     LiveSource
	NoSignal FrameSource
}

func (m *MuxSource) NextFrame(now time.Time) ([]byte, error) {
	if m.stopAll.Load() {
		return nil, nil
	}

	// Prefer live if it's active
	if m.Live != nil && m.Live.IsActive(now) {
		au, err := m.Live.NextFrame(now)
		if err != nil {
			return nil, err
		}
		return au, nil
	}

	// No live available switch to static
	if m.NoSignal != nil {
		return m.NoSignal.NextFrame(now)
	}
	return nil, nil
}

func (m *MuxSource) StopAllFrames() {
	m.stopAll.Store(true)
}

func (m *MuxSource) PrepareLiveConsumer() {
	if m.Live != nil {
		m.Live.PrepareForConsumer()
	}
}
