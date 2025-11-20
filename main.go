package main

import (
	"context"
	"log"
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

	for entry := range entries {
		log.Println("FOUND:", entry.HostName, entry.Port)
	}

	cancel()
}
