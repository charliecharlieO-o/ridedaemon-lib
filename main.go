package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
)

type Host struct {
	Ip      string
	Port    string
	Package string
}

func SearchForService(ctx context.Context) *Host {
	svc, _ := NewMDNSService()
	entries, _ := svc.LookupEntries()

	log.Println("Looking for MDNS entries")

	go func() {
		err := svc.Browse(ctx, "_EasyConn._tcp", "local")
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Find the display service IP, port and appropriate package name
	var ip, ecPort, packageName string
	reService := regexp.MustCompile("^packagename=(com.cfmoto.cfmotointernational)$")
	reIP := regexp.MustCompile("^ip=(.*)$")

	count := 0
	for entry := range entries {
		count++
		log.Printf("Entry No.%d\n", count)
		exit := false

		dta, _ := json.Marshal(entry)
		log.Println(string(dta))

		for _, i := range entry.Text {
			if reService.MatchString(i) {
				sub := reService.FindStringSubmatch(i)
				log.Println("Found EC service package:", sub[1])
				ecPort = strconv.Itoa(entry.Port)
				packageName = sub[1]
				exit = true
			}
			if reIP.MatchString(i) {
				sub := reIP.FindStringSubmatch(i)
				log.Println("Found CFMOTO service IP at:", sub[1])
				ip = sub[1]
				exit = true
			}
		}
		if exit {
			break
		}
	}
	log.Println("Finished mDNS discovery")

	if ip != "" {
		return &Host{
			Ip:      ip,
			Port:    ecPort,
			Package: packageName,
		}
	} else {
		return nil
	}
}

func main() {
	// Load no signal stream service
	noSigSrc, err := NewNoSignalSource("./static.h264", 15)
	if err != nil {
		log.Fatalf("Failed to create no signal source: %v", err)
	} else {
		log.Println("Signal source created")
	}

	ctx, cancel := context.WithCancel(context.Background())
	host := SearchForService(ctx)

	if host != nil {
		// Initialize the EasyConn command service
		log.Println("Starting EC service...")
		ecService := NewECService(host.Ip, host.Port, host.Package, "Android")
		if err := ecService.Start(); err != nil {
			log.Fatal(err)
		}
		log.Println("EC service started")

		// Startup stage channels
		pxcReady := make(chan any, 1)

		// Build Services - preemptive
		// Create PXC Server - Exchanges setup info between the streaming service and the phone
		var keyPair *KeyPair
		phoneUUID := uuid.New()
		if kp, err := GenKeyPair(); err != nil {
			log.Fatal(err)
			return
		} else {
			keyPair = kp
		}
		phoneConfig := &PhoneConfig{
			PxcVersion:            "1.0.2",
			PhoneUUID:             phoneUUID.String(),
			PhoneBrand:            "google",
			PhoneModel:            "Pixel 6a",
			PhoneOsVersion:        "36",
			PhoneOs:               "Android",
			Package:               "com.cfmoto.cfmotointernational",
			VersionCode:           121,
			Token:                 0,
			Pubkey:                "", // Done inside PXC
			EncryptedHUID:         "", // Done inside PXC
			BluetoothName:         "Pixel 6a",
			SupportH264IFrame:     true,
			AppVersionFingerPrint: "V:2.2.1(121)--ONLINE",
		}
		pxcServer := NewPXCControl(":10922", keyPair, phoneConfig)

		// Defer close
		defer func(pxcServer *PXCControl, ctx context.Context) {
			err := pxcServer.Stop(ctx)
			if err != nil {
				log.Printf("Error stopping pxc control: %s", err)
			}
		}(pxcServer, ctx)

		// Async event handlers
		go func() {
			for err := range pxcServer.Errors {
				log.Printf("PXC Error: %s\n", err)
			}
		}()
		go func() {
			for event := range pxcServer.Events {
				if event.Command == PxcHeartbeat {
					// Notify PXC is ready non-blocking
					select {
					case pxcReady <- struct{}{}:
					default:
					}
					continue
				}
				b, _ := json.Marshal(event)
				log.Printf("PXC Event: %s\n", string(b))
			}
		}()

		// Create Media Control Server - Gets display configuration and init state
		mediaControl := NewMediaControl(":10921")

		// Defer close
		defer func(s *MediaControl, ctx context.Context) {
			err := mediaControl.Stop(ctx)
			if err != nil {
				log.Printf("Error stopping media control: %s", err)
			}
		}(mediaControl, ctx)

		// Async event handlers
		go func() {
			for err := range mediaControl.Errors {
				log.Printf("MediaCtrl Error: %s\n", err)
			}
		}()
		go func() {
			for event := range mediaControl.Events {
				b, _ := json.Marshal(event)
				log.Printf("Event: %s\n", string(b))
			}
		}()

		// Create Media Stream Server - This service is the one that sends H264 stream data
		mediaStream := NewMediaStream(":10920", noSigSrc, 0x1000, 3*time.Millisecond)

		defer func(s *MediaStream, ctx context.Context) {
			err := mediaStream.Stop(ctx)
			if err != nil {
				log.Printf("Error stopping media stream: %s", err)
			}
		}(mediaStream, ctx)

		// Async event handlers
		go func() {
			for err := range mediaStream.Errors {
				log.Printf("MediaStream Error: %s\n", err)
			}
		}()

		// Async startup sequence -- start with PXC
		log.Println("Starting up PXC ---------")
		if err := pxcServer.Start(); err != nil {
			log.Fatalf("Error starting pxc control: %s", err)
		}

		// Wait until PXC is ready
		<-pxcReady

		log.Println("Starting up Media Strm ---------")
		if err := mediaStream.Start(); err != nil {
			log.Fatalf("Error starting media stream: %s", err)
		}

		log.Println("Starting up Media Ctrl ---------")
		if err := mediaControl.Start(); err != nil {
			log.Fatalf("Error starting media control: %s", err)
		}
	}

	// Create channel to receive OS signals
	sigs := make(chan os.Signal, 1)

	// Notify on SIGINT (Ctrl+C) and SIGTERM (kill)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	// Block until a signal is received
	<-sigs

	fmt.Println("Shutting down gracefully")
	cancel()
}
