package main

import (
	"context"
	"encoding/json"
	"log"
	"regexp"
	"strconv"
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
}
