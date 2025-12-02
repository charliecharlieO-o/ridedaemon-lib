package core

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/charliecharlieO-o/ridedaemon-go/hud/net"
	"github.com/charliecharlieO-o/ridedaemon-go/hud/stream"
	"github.com/charliecharlieO-o/ridedaemon-go/internal/logging"
	"github.com/google/uuid"
	"github.com/grandcat/zeroconf"
)

type HudEventSource int

const (
	UnknownEventSource HudEventSource = iota + 1
	EventSourcePXC
	EventSourceMediaControl
)

type HudEvent struct {
	Source HudEventSource
	Time   time.Time
	Cmd    int
	Data   any
}

type StoppableServer interface {
	Start() error
	Stop(ctx context.Context) error
}

type ecHost struct {
	Ip      string
	Port    string
	Package string
}

type CfmotoHUD struct {
	// State
	mu      sync.Mutex
	running bool

	// Config
	host        *ecHost
	targetFPS   int
	packageName string
	phoneUUID   uuid.UUID
	phoneConfig *net.PhoneConfig

	// net management
	keyPair      *net.KeyPair
	ecService    *net.ECService
	pxcControl   *net.PXCControl
	mediaControl *net.MediaControl
	mediaStream  *net.MediaStream

	// Streaming
	muxSource *stream.MuxSource

	// Communications
	stopped  chan any
	stopOnce sync.Once
	Events   chan HudEvent
	Errors   chan error
}

func NewCfmotoHUD(targetFPS int, mux *stream.MuxSource) *CfmotoHUD {
	if targetFPS <= 0 {
		targetFPS = 30
	}
	id := uuid.New()
	pkg := "com.cfmoto.cfmotointernational"
	return &CfmotoHUD{
		packageName: pkg,
		phoneUUID:   id,
		phoneConfig: &net.PhoneConfig{
			PxcVersion:            "1.0.2",
			PhoneUUID:             id.String(),
			PhoneBrand:            "google",
			PhoneModel:            "Pixel 6a",
			PhoneOsVersion:        "36",
			PhoneOs:               "Android",
			Package:               pkg,
			VersionCode:           121,
			Token:                 0,
			Pubkey:                "", // Done inside PXC
			EncryptedHUID:         "", // Done inside PXC
			BluetoothName:         "Pixel 6a",
			SupportH264IFrame:     true,
			AppVersionFingerPrint: "V:2.2.1(121)--ONLINE",
		},
		targetFPS: targetFPS,
		muxSource: mux,
		stopped:   make(chan any),
		Events:    make(chan HudEvent, 32),
		Errors:    make(chan error, 32),
	}
}

func (hud *CfmotoHUD) handleServerEvent(evt any) {
	now := time.Now()
	hudEvent := HudEvent{Source: UnknownEventSource, Time: now, Data: evt}
	switch e := evt.(type) {
	case *net.PXCResponse:
		if e.Command == net.PxcHeartbeat {
			break
		}
		hudEvent.Source = EventSourcePXC
		hudEvent.Cmd = int(e.Command)
		hudEvent.Data = e.Body
	case *net.MediaCtrlResponse:
		hudEvent.Source = EventSourceMediaControl
		hudEvent.Cmd = int(e.Command)
		hudEvent.Data = e.Payload
	}

	select {
	case hud.Events <- hudEvent:
	default:
	}
}

func (hud *CfmotoHUD) isFatalErr(err error) bool {
	var fe net.FatalError
	if errors.As(err, &fe) {
		return fe.IsFatal()
	}
	return false
}

func (hud *CfmotoHUD) handleServerError(err error) {
	if err == nil {
		return
	}

	// Bubble up error
	select {
	case hud.Errors <- err:
	default:
	}
	if hud.isFatalErr(err) {
		// Fatal, we need to exit cleanly
		go func() {
			ctxWithTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = hud.StopStream(ctxWithTimeout)
		}()
	}
}

func (hud *CfmotoHUD) startPxcEventFwd(s *net.PXCControl) {
	// PXC Events
	go func() {
		for evt := range s.Events {
			hud.handleServerEvent(evt)
		}
	}()
}

func (hud *CfmotoHUD) startMediaEventFwd(s *net.MediaControl) {
	// Media Ctrl events
	go func() {
		for evt := range s.Events {
			hud.handleServerEvent(evt)
		}
	}()
}

func (hud *CfmotoHUD) SearchForHost(ctx context.Context, timeout time.Duration) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var err error
	var mdns *net.MDNSService
	var entries <-chan *zeroconf.ServiceEntry

	if mdns, err = net.NewMDNSService(); err != nil {
		return err
	}
	if entries, err = mdns.LookupEntries(); err != nil {
		return err
	}

	go func() {
		mdnsErr := mdns.Browse(ctxWithTimeout, "_EasyConn._tcp", "local")
		if mdnsErr != nil {
			logging.Printf("error while browsing for mdns host: %s", mdnsErr)
		}
	}()

	// Find the display service IP, port and appropriate package name
	reService := regexp.MustCompile("^packagename=(" + hud.packageName + ")$")
	reIP := regexp.MustCompile("^ip=(.*)$")

	for {
		select {
		case <-ctxWithTimeout.Done():
			if errors.Is(ctxWithTimeout.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("search for host timed out after: %s", timeout)
			}
			return ctxWithTimeout.Err()
		case entry, ok := <-entries:
			if !ok {
				if ctxWithTimeout.Err() != nil {
					return ctxWithTimeout.Err()
				}
				return errors.New("mDNS entries channel closed before host found")
			}

			var ip, ecPort, packageName string

			for _, txt := range entry.Text {
				if reService.MatchString(txt) {
					sub := reService.FindStringSubmatch(txt)
					logging.Printf("Found EC service package: %s", sub[1])
					ecPort = strconv.Itoa(entry.Port)
					packageName = sub[1]
				}
				if reIP.MatchString(txt) {
					sub := reIP.FindStringSubmatch(txt)
					logging.Printf("Found EC IP package: %s", sub[1])
					ip = sub[1]
				}
			}

			if ip != "" && ecPort != "" {
				// Found what we need
				hud.host = &ecHost{
					Ip:      ip,
					Port:    ecPort,
					Package: packageName,
				}

				// Stop browsing as soon as we’ve found the host.
				cancel()

				return nil
			}
		}
	}
}

func (hud *CfmotoHUD) StartStream(ctx context.Context) (err error) {
	hud.mu.Lock()
	if hud.running {
		hud.mu.Unlock()
		return errors.New("already running")
	}
	if hud.host == nil {
		hud.mu.Unlock()
		return errors.New("host is not set or has not been found")
	}
	if hud.muxSource == nil {
		hud.mu.Unlock()
		return errors.New("mux source is not set")
	}
	hud.mu.Unlock()

	if hud.keyPair, err = net.GenKeyPair(); err != nil {
		return err
	}

	logging.Printf("Sending EC stream init command")
	hud.ecService = net.NewECService(
		hud.host.Ip, hud.host.Port, hud.host.Package, hud.phoneConfig.PhoneOs,
	)
	if err = hud.ecService.InitStreamCmd(); err != nil {
		hud.ecService = nil
		return err
	}

	var started []StoppableServer

	// If err != nil on exit, stop everything still running
	defer func() {
		if err == nil || len(started) == 0 {
			return
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// stop reverse order
		for i := len(started) - 1; i >= 0; i-- {
			if stopErr := started[i].Stop(shutdownCtx); stopErr != nil {
				logging.Printf("Error stopping server %T: %v", started[i], stopErr)
			}
		}

		hud.mu.Lock()
		hud.running = false
		hud.mu.Unlock()
	}()

	pxcReady := make(chan any, 1)
	pxcServer := net.NewPXCControl(":10922", hud.keyPair, hud.phoneConfig)
	// PXC is ready
	go func() {
		for event := range pxcServer.Events {
			if event.Command == net.PxcHeartbeat {
				// Notify PXC is ready non-blocking
				select {
				case pxcReady <- struct{}{}:
				default:
				}
				return // exit the routine
			}
		}
	}()
	// PXC error handling
	go func() {
		for pxcError := range pxcServer.Errors {
			hud.handleServerError(pxcError)
		}
	}()
	// hud.startPxcEventFwd(pxcServer) -- uncomment for testing only, too slow!

	if err = pxcServer.Start(); err != nil {
		return fmt.Errorf("start pxc error: %w", err)
	}
	started = append(started, pxcServer)

	// IS PXC on or has the context expired (exit on ctx done)?
	select {
	case <-pxcReady:
		break
	case <-ctx.Done():
		return ctx.Err()
	}

	mediaControl := net.NewMediaControl(":10921")
	// -- maybe we could change chunkStep to something smaller to reduce latency?
	mediaStream := net.NewMediaStream(":10920", hud.muxSource, 0x1000, 3*time.Millisecond)

	// Media error handling
	go func() {
		for medCtrl := range mediaControl.Errors {
			hud.handleServerError(medCtrl)
		}
	}()
	go func() {
		for medStr := range mediaStream.Errors {
			hud.handleServerError(medStr)
		}
	}()
	hud.startMediaEventFwd(mediaControl)

	if err = mediaStream.Start(); err != nil {
		return fmt.Errorf("start media stream error: %w", err)
	}
	started = append(started, mediaStream)

	if err = mediaControl.Start(); err != nil {
		return fmt.Errorf("start media server error: %w", err)
	}
	started = append(started, mediaControl)

	hud.mu.Lock()
	hud.pxcControl = pxcServer
	hud.mediaStream = mediaStream
	hud.mediaControl = mediaControl
	hud.running = true
	hud.mu.Unlock()

	return nil
}

func (hud *CfmotoHUD) StopStream(ctx context.Context) error {
	hud.mu.Lock()
	if !hud.running {
		hud.mu.Unlock()
		return nil
	}

	// Take copies of servers and clear state under lock
	pxc := hud.pxcControl
	mediaCtrl := hud.mediaControl
	mediaStream := hud.mediaStream

	hud.pxcControl = nil
	hud.mediaControl = nil
	hud.mediaStream = nil
	hud.running = false
	hud.mu.Unlock()

	ctxWithTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	servers := []StoppableServer{mediaStream, mediaCtrl, pxc}

	for _, server := range servers {
		if server == nil {
			continue
		}
		wg.Add(1)
		go func(server StoppableServer) {
			defer wg.Done()
			_ = server.Stop(ctxWithTimeout)
		}(server)
	}

	done := make(chan any)
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}

	close(hud.Events)
	close(hud.Errors)

	hud.stopOnce.Do(func() {
		close(hud.stopped)
	})

	return nil
}

func (hud *CfmotoHUD) IsRunning() bool {
	hud.mu.Lock()
	defer hud.mu.Unlock()
	return hud.running
}

func (hud *CfmotoHUD) Done() <-chan any {
	return hud.stopped
}
