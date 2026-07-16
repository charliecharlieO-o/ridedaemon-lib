package api

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	stdnet "net"
	"os"
	"time"

	"github.com/charliecharlieO-o/ridedaemon-go/hud/core"
	"github.com/charliecharlieO-o/ridedaemon-go/hud/stream"
	"github.com/charliecharlieO-o/ridedaemon-go/internal/logging"
)

// To make BuildAnnexBAUFromAVCC work well:
// Configure MediaCodec so that keyframes include SPS/PPS in the sample payload.
// That’s usually done with something like KEY_PREPEND_HEADER_TO_SYNC_FRAMES.
// Keep GOP = 1 or 2, no B-frames (you were already planning this).
// Then you don’t need to juggle SPS/PPS separately, they’ll appear as the first NALs in the keyframe sample, and
// our AVCC -> Annex-B converter just wraps them in start codes and keeps them.

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
	nalCount := 0
	for {
		if i+4 > len(avcc) {
			break
		}
		nalLen := int(binary.BigEndian.Uint32(avcc[i : i+4]))
		i += 4
		if nalLen == 0 || i+nalLen > len(avcc) { // Truncated / malformed sample
			return nil, fmt.Errorf("invalid AVCC NAL length")
		}

		out = append(out, 0x00, 0x00, 0x00, 0x01) // Start code
		out = append(out, avcc[i:i+nalLen]...)    // NAL bytes
		i += nalLen
		nalCount++
	}

	if i != len(avcc) || nalCount == 0 {
		return nil, fmt.Errorf("no NAL units found in AVCC sample")
	}
	return out, nil
}

func BuildAnnexBAU(sample []byte) ([]byte, error) {
	if len(sample) < 4 {
		return nil, fmt.Errorf("sample too short")
	}

	// 1) Detect Annex-B: start code at beginning
	if bytes.HasPrefix(sample, []byte{0x00, 0x00, 0x00, 0x01}) ||
		bytes.HasPrefix(sample, []byte{0x00, 0x00, 0x01}) {
		out := make([]byte, 0, len(sample)+16)
		// prepend AUD
		out = append(out, 0x00, 0x00, 0x00, 0x01, 0x09, 0xF0)
		out = append(out, sample...)
		return out, nil
	}

	// 2) Otherwise, assume AVCC (4-byte lengths)
	return BuildAnnexBAUFromAVCC(sample)
}

type CanBeFatalErr interface {
	error
	IsFatal() bool
}

type MobileConfig struct {
	StaticSignal        []byte
	TargetFPS           int
	StartupTimeoutSec   int
	TeardownTimeoutSec  int
	DiscoveryTimeoutSec int
	DiscoveryTries      int
}

func NewMobileConfig(static []byte, fps int, startupTimeoutSec, teardownTimeoutSec, discTimeout, discTries int) *MobileConfig {
	return &MobileConfig{
		StaticSignal:        static,
		TargetFPS:           fps,
		StartupTimeoutSec:   startupTimeoutSec,
		TeardownTimeoutSec:  teardownTimeoutSec,
		DiscoveryTimeoutSec: discTimeout,
		DiscoveryTries:      discTries,
	}
}

type MobileCallback interface {
	OnError(msg string, fatal bool)
	OnEvent(time int64, source int, command int, payload []byte)
	OnStopped()
}

type StreamHost struct {
	core.EcHost
}

func NewStreamHost(ip, port, pkg string) *StreamHost {
	return &StreamHost{
		core.EcHost{
			Ip:      ip,
			Port:    port,
			Package: pkg,
		},
	}
}

type MobileSession struct {
	cfg MobileConfig
	cb  MobileCallback

	hud      *core.CfmotoHUD
	mux      *stream.MuxSource
	streamer *stream.AUStreamer

	stopped chan struct{}
}

func NewMobileSession(cfg *MobileConfig, cb MobileCallback) (*MobileSession, error) {
	ms := &MobileSession{
		cfg:     *cfg,
		cb:      cb,
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
		for {
			select {
			case cErr, ok := <-ms.hud.Errors:
				if !ok {
					// if we close Errors, this lets us exit cleanly
					return
				}
				if cErr != nil {
					ms.relayError(cErr)
				}
			case <-ms.hud.Done():
				return // HUD session is over - stop relaying
			}
		}
	}()
	go func() {
		for {
			select {
			case evt, ok := <-ms.hud.Events:
				if !ok {
					return
				}
				ms.relayEvent(evt)
			case <-ms.hud.Done():
				return
			}
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
	timestamp := time.Now().UnixMilli()
	src := int(evt.Source)
	command := evt.Cmd

	var payload []byte
	if dta, ok := evt.Data.([]byte); ok {
		payload = append([]byte(nil), dta...)
	}

	if ms.cb != nil {
		go ms.cb.OnEvent(timestamp, src, command, payload)
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

// DiscoverHost uses zeroconf to search for the mDNS service, use only if SELinux or the mobile OS allows for it
func (ms *MobileSession) DiscoverHost() error {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), time.Duration(ms.cfg.StartupTimeoutSec)*time.Second)
	defer cancel()

	tries := 0
	for tries < ms.cfg.DiscoveryTries {
		tries++
		if err := ms.hud.SearchForHost(ctxWithTimeout, time.Duration(ms.cfg.DiscoveryTimeoutSec)*time.Second); err != nil {
			logging.Printf("error discovering host: %v\n", err)
			return err
		} else {
			return nil
		}
	}

	return errors.New("discovery timed out")
}

func (ms *MobileSession) SetECHost(host *StreamHost) error {
	if err := ms.hud.SetHost(&host.EcHost); err != nil {
		return err
	}
	return nil
}

func (ms *MobileSession) StartSession() error {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), time.Duration(ms.cfg.StartupTimeoutSec)*time.Second)
	defer cancel()
	if err := ms.hud.StartStream(ctxWithTimeout); err != nil {
		return err
	}
	return nil
}

// StartSessionWithSocketFd uses an already-connected TCP socket supplied by Android.
// The descriptor ownership is transferred to this method and is always closed here.
func (ms *MobileSession) StartSessionWithSocketFd(fd int64) error {
	if fd < 0 {
		return errors.New("invalid EC init socket descriptor")
	}
	file := os.NewFile(uintptr(fd), "ec-init")
	if file == nil {
		return errors.New("unable to adopt EC init socket descriptor")
	}
	conn, err := stdnet.FileConn(file)
	closeErr := file.Close()
	if err != nil {
		return fmt.Errorf("unable to adopt EC init socket: %w", err)
	}
	if closeErr != nil {
		_ = conn.Close()
		return fmt.Errorf("unable to release EC init descriptor: %w", closeErr)
	}

	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), time.Duration(ms.cfg.StartupTimeoutSec)*time.Second)
	defer cancel()
	if err := ms.hud.StartStreamWithInitConn(ctxWithTimeout, conn); err != nil {
		return err
	}
	return nil
}

func (ms *MobileSession) StopSession() error {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), time.Duration(ms.cfg.TeardownTimeoutSec)*time.Second)
	defer cancel()
	logging.Printf("Stopping session\n")
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

	au, err := BuildAnnexBAU(avccChunk)
	if err != nil {
		if ms.cb != nil {
			go ms.cb.OnError("invalid AVCC: "+err.Error(), false)
		}
		logging.Printf("MobileSession: invalid AVCC: %v\n", err)
		return
	}

	ms.mux.Live.PushFrame(au)
}

func (ms *MobileSession) IsRunning() bool {
	select {
	case <-ms.stopped:
		return false
	default:
		return true
	}
}
