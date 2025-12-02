package api

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/charliecharlieO-o/ridedaemon-go/hud/core"
	"github.com/charliecharlieO-o/ridedaemon-go/hud/stream"
	"github.com/charliecharlieO-o/ridedaemon-go/internal/logging"
)

// BuildAnnexBAUFromAVCC converts a single AVCC-formatted sample (one frame's worth of NAL units) into a single Annex-B AU.
// It assumes 4-byte big-endian NAL lengths.
func BuildAnnexBAUFromAVCC(avcc []byte) ([]byte, error) {
	if len(avcc) < 4 {
		return nil, fmt.Errorf("avcc sample too short")
	}

	out := make([]byte, 0, len(avcc)+32)

	// Prepend an AUD NAL. We use a common AUD RBSP payload 0xF0, which most decoders ignore.
	// Annex-B start code (4 bytes) + NAL header (0x09) + rbsp_byte
	out = append(out, 0x00, 0x00, 0x00, 0x01, 0x09, 0xF0)

	i := 0
	for {
		if i+4 > len(avcc) {
			break
		}
		nalLen := int(binary.BigEndian.Uint32(avcc[i : i+4]))
		i += 4
		if nalLen == 0 || i+nalLen > len(avcc) { // Truncated / malformed sample
			break
		}

		out = append(out, 0x00, 0x00, 0x00, 0x01) // Start code
		out = append(out, avcc[i:i+nalLen]...)    // NAL bytes
		i += nalLen
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("no NAL units found in AVCC sample")
	}
	return out, nil
}

type CanBeFatalErr interface {
	error
	IsFatal() bool
}

type MobileConfig struct {
	StaticSignal     []byte
	TargetFPS        int
	StartupTimeout   time.Duration
	TeardownTimeout  time.Duration
	DiscoveryTimeout time.Duration
	DiscoveryTries   int
}

type MobileEvent struct {
	Time    int64 // millis for mobile
	Type    int
	Payload []byte
}

type MobileCallback interface {
	OnError(msg string, fatal bool)
	OnEvent(event MobileEvent)
	OnStopped()
}

type MobileSession struct {
	cfg MobileConfig
	cb  MobileCallback

	hud      *core.CfmotoHUD
	mux      *stream.MuxSource
	streamer *stream.AUStreamer

	stopped chan struct{}
}

func NewMobileSession(cfg MobileConfig) (*MobileSession, error) {
	ms := &MobileSession{
		cfg:     cfg,
		stopped: make(chan struct{}),
	}

	// Setup Sources
	static, err := stream.NewRawFrameSource(cfg.StaticSignal, cfg.TargetFPS)
	if err != nil {
		return nil, err
	}
	live := stream.NewLiveStreamSource(cfg.TargetFPS, 3*time.Second, 3)
	ms.mux = &stream.MuxSource{NoSignal: static, Live: live}

	// Build streamer
	ms.streamer = stream.NewAUStreamer(live)

	// Build hud session
	ms.hud = core.NewCfmotoHUD(cfg.TargetFPS, ms.mux)
	go func() {
		for hErr := range ms.hud.Errors {
			ms.relayError(hErr)
		}
	}()
	go func() {
		for evt := range ms.hud.Events {
			ms.relayEvent(evt)
		}
	}()

	go ms.watchHud()

	return ms, nil
}

func (ms *MobileSession) relayError(err error) {
	msg := err.Error()
	fatal := true

	var fErr CanBeFatalErr
	if errors.As(err, &fErr) {
		fatal = fErr.IsFatal()
	}

	if ms.cb != nil {
		go ms.cb.OnError(msg, fatal)
	}
}

func (ms *MobileSession) relayEvent(evt core.HudEvent) {
	mobEvt := MobileEvent{
		Time: time.Now().UnixMilli(),
		Type: evt.Cmd,
	}
	if dta, ok := evt.Data.([]byte); ok {
		mobEvt.Payload = append([]byte(nil), dta...)
	}

	if ms.cb != nil {
		go ms.cb.OnEvent(mobEvt)
	}
}

func (ms *MobileSession) watchHud() {
	<-ms.hud.Done()
	// notify callback
	if ms.cb != nil {
		go ms.cb.OnStopped()
	}
	close(ms.stopped)
}

func (ms *MobileSession) discoverHost(ctx context.Context) error {
	tries := 0
	for tries < ms.cfg.DiscoveryTries {
		tries++
		if err := ms.hud.SearchForHost(ctx, ms.cfg.DiscoveryTimeout); err != nil {
			logging.Printf("Error discovering host: %v\n", err)
		} else {
			return nil
		}
	}
	return errors.New("discovery timed out")
}

func (ms *MobileSession) StartSession() error {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), ms.cfg.StartupTimeout)
	defer cancel()
	if err := ms.discoverHost(ctxWithTimeout); err != nil {
		return err
	}
	if err := ms.hud.StartStream(ctxWithTimeout); err != nil {
		return err
	}
	return nil
}

func (ms *MobileSession) StopSession() error {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), ms.cfg.TeardownTimeout)
	defer cancel()
	if err := ms.hud.StopStream(ctxWithTimeout); err != nil {
		return err
	}
	return nil
}

func (ms *MobileSession) PushFrame(avccChunk []byte) {
	if !ms.hud.IsRunning() {
		return
	}
	if len(avccChunk) == 0 {
		return
	}

	au, err := BuildAnnexBAUFromAVCC(avccChunk)
	if err != nil {
		if ms.cb != nil {
			go ms.cb.OnError("invalid AVCC: "+err.Error(), false)
		}
	}

	ms.mux.Live.PushFrame(au) // push directly to live source
}

func (ms *MobileSession) IsRunning() bool {
	select {
	case <-ms.stopped:
		return false
	default:
		return true
	}
}
