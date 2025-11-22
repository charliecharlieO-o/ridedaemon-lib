package main

import (
	"context"
	"log"
	"regexp"
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

	reService := regexp.MustCompile("^packagename=com.cfmoto.cfmotointernational$")
	reIP := regexp.MustCompile("^ip=.*$")
	for entry := range entries {
		log.Println("FOUND:", entry.HostName, entry.Port)
		for _, i := range entry.Text {
			if reService.MatchString(i) {
				log.Println("Found CFMOTO service")
			}
			if reIP.MatchString(i) {
				sub := reService.FindStringSubmatch(i)
				log.Println("Found CFMOTO service IP at:", sub[1])
			}
		}
	}

	cancel()
}
