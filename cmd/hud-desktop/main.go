package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/charliecharlieO-o/ridedaemon-go/hud/core"
	"github.com/charliecharlieO-o/ridedaemon-go/hud/stream"
)

const targetFPS = 30

var muxSource *stream.MuxSource

func SetupSources() error {
	noSigSrc, err := stream.NewFileFrameSource("./static/iris_out.h264", targetFPS)
	if err != nil {
		return err
	}
	liveSrc := stream.NewLiveStreamSource(targetFPS, 3*time.Second, 3)
	muxSource = &stream.MuxSource{NoSignal: noSigSrc, Live: liveSrc}
	return nil
}

func main() {
	ctx := context.Background()

	// Setup sources
	if err := SetupSources(); err != nil {
		log.Fatalf("Failed to setup sources: %s", err)
		return
	}
	desktopStreamer := stream.NewDesktopStreamer(muxSource.Live.(*stream.LiveStreamSource))

	// Build Cfmoto HUD
	cfmotoHUD := core.NewCfmotoHUD(targetFPS, muxSource)
	go func() {
		for err := range cfmotoHUD.Errors {
			log.Printf("Error: %s\n", err)
		}
	}()
	go func() {
		for evt := range cfmotoHUD.Events {
			log.Printf("Event: %v\n", evt)
		}
	}()

	if err := cfmotoHUD.SearchForHost(ctx, 10*time.Second); err != nil {
		log.Printf("Failed to search for hosts: %s", err)
		return
	}

	if err := cfmotoHUD.StartStream(ctx); err != nil {
		log.Printf("Failed to start stream: %s", err)
	}

	if err := desktopStreamer.Start(); err != nil {
		log.Printf("Failed to start stream: %s", err)
	}

	// Create channel to receive OS signals
	sigs := make(chan os.Signal, 1)

	// Notify on SIGINT (Ctrl+C) and SIGTERM (kill)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	// Block until a signal is received
	<-sigs

	fmt.Println("Shutting down streamer")
	muxSource.StopAllFrames()
	_ = desktopStreamer.Stop()

	fmt.Println("Stopping HUD")
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := cfmotoHUD.StopStream(ctxWithTimeout); err != nil {
		log.Printf("Failed to stop hud: %s", err)
	}
}
