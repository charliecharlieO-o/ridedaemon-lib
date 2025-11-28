package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

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

type NoSignalSource struct {
	mu sync.Mutex

	aus       [][]byte // AUD-delimited access units
	replayIdx int
	pollCount uint64

	lastAu       []byte
	lastAdvance  time.Time
	interval     time.Duration // 1/target fps
	streamHeader []byte        // optional SPS/PPS/etc to prepend for first poll (warmup)
}

// NewNoSignalSource loads a .h264 file, splits it into AUD-delim AUs
// annex-b and creates a replay source.
func NewNoSignalSource(path string, targetFPS int) (*NoSignalSource, error) {
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
	return &NoSignalSource{
		aus:          aus,
		replayIdx:    0,
		pollCount:    0,
		lastAu:       nil,
		lastAdvance:  time.Time{},
		interval:     interval,
		streamHeader: append([]byte(nil), streamHeader...), // copy bytes
	}, nil
}

func (s *NoSignalSource) NextFrame(now time.Time) ([]byte, error) {
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
