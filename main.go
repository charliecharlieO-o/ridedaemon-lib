package main

import (
	"context"
	"encoding/json"
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
		if jsonP, err := json.Marshal(entry); err != nil {
			log.Println(err)
		} else {
			log.Println(string(jsonP))
		}
	}

	cancel()
}
