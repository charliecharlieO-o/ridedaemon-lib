package net

import (
	"encoding/binary"
	"testing"
)

func TestMediaCaptureAckUsesRequestedCFDL26Dimensions(t *testing.T) {
	request := make([]byte, 204)
	binary.LittleEndian.PutUint16(request[0:2], 720)
	binary.LittleEndian.PutUint16(request[2:4], 712)
	binary.LittleEndian.PutUint32(request[8:12], 2)
	request[29] = 0

	assertMediaCaptureAck(t, buildMediaCaptureAckPayload(request), 2, 720, 704, 0)
}

func TestMediaCaptureAckPreservesLegacyNegotiation(t *testing.T) {
	request := make([]byte, 32)
	binary.LittleEndian.PutUint16(request[0:2], 800)
	binary.LittleEndian.PutUint16(request[2:4], 386)
	binary.LittleEndian.PutUint32(request[8:12], 2)
	request[29] = 1

	assertMediaCaptureAck(t, buildMediaCaptureAckPayload(request), 2, 800, 384, 1)
}

func TestMediaCaptureAckFallsBackForMissingPayload(t *testing.T) {
	assertMediaCaptureAck(t, buildMediaCaptureAckPayload(nil), 2, 800, 384, 1)
}

func assertMediaCaptureAck(
	t *testing.T,
	payload []byte,
	wantEncoder uint32,
	wantWidth uint16,
	wantHeight uint16,
	wantExtended byte,
) {
	t.Helper()
	if len(payload) != 9 {
		t.Fatalf("payload length = %d, want 9", len(payload))
	}
	if got := binary.LittleEndian.Uint32(payload[0:4]); got != wantEncoder {
		t.Fatalf("encoder = %d, want %d", got, wantEncoder)
	}
	if got := binary.LittleEndian.Uint16(payload[4:6]); got != wantWidth {
		t.Fatalf("width = %d, want %d", got, wantWidth)
	}
	if got := binary.LittleEndian.Uint16(payload[6:8]); got != wantHeight {
		t.Fatalf("height = %d, want %d", got, wantHeight)
	}
	if got := payload[8]; got != wantExtended {
		t.Fatalf("extended protocol = %d, want %d", got, wantExtended)
	}
}
