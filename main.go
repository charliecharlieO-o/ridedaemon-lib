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

	"github.com/google/uuid"
)

func main() {
	svc, _ := NewMDNSService()
	entries, _ := svc.LookupEntries()

	log.Println("Looking for MDNS entries")

	ctx, cancel := context.WithCancel(context.Background())
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
	cancel()

	// Initialize the EasyConn command service
	log.Println("Starting EC service...")
	ecService := NewECService(ip, ecPort, packageName, "Android")
	if err := ecService.Start(); err != nil {
		log.Fatal(err)
	}
	log.Println("EC service started")

	// Startup PXC Server - Exchanges setup info between the streaming service and the phone
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
	go func() {
		for err := range pxcServer.Errors {
			log.Printf("PXC Error: %s\n", err)
		}
	}()
	go func() {
		for event := range pxcServer.Events {
			log.Printf("Event: %s\n", event)
		}
	}()
	if err := pxcServer.Start(); err != nil {
		log.Fatalf("Error starting pxc control: %s", err)
	}

	// Create channel to receive OS signals
	sigs := make(chan os.Signal, 1)

	// Notify on SIGINT (Ctrl+C) and SIGTERM (kill)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	fmt.Println("Press Ctrl+C to exit...")

	// Block until a signal is received
	<-sigs

	fmt.Println("Shutting down gracefully")
}
