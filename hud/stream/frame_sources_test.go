package stream

import (
	"testing"
	"time"
)

func TestLiveSourceWaitsForIDR(t *testing.T) {
	source := NewLiveStreamSource(30, 3*time.Second, 3)
	pFrame := annexBAU(1)
	idrFrame := annexBAU(5)

	source.PushFrame(pFrame)
	if frame, _ := source.NextFrame(time.Now()); frame != nil {
		t.Fatal("predictive frame was emitted before the first IDR")
	}

	source.PushFrame(idrFrame)
	frame, err := source.NextFrame(time.Now())
	if err != nil {
		t.Fatalf("read IDR: %v", err)
	}
	if !hasAnnexBNALType(frame, 5) {
		t.Fatal("first emitted frame is not an IDR")
	}
}

func TestPrepareForConsumerDropsQueuedFramesAndWaitsForFreshIDR(t *testing.T) {
	source := NewLiveStreamSource(30, 3*time.Second, 3)
	source.PushFrame(annexBAU(5))
	source.PushFrame(annexBAU(1))
	source.PrepareForConsumer()

	if frame, _ := source.NextFrame(time.Now()); frame != nil {
		t.Fatal("stale frame remained queued after consumer reset")
	}
	source.PushFrame(annexBAU(1))
	if frame, _ := source.NextFrame(time.Now()); frame != nil {
		t.Fatal("predictive frame was emitted while waiting for a fresh IDR")
	}
	source.PushFrame(annexBAU(5))
	if frame, _ := source.NextFrame(time.Now()); !hasAnnexBNALType(frame, 5) {
		t.Fatal("fresh IDR was not emitted after consumer reset")
	}
}

func annexBAU(nalType byte) []byte {
	return []byte{0, 0, 0, 1, 9, 0xf0, 0, 0, 0, 1, nalType, 1, 2, 3}
}
