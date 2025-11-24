package main

import (
	"context"
	"errors"
	"log"
	"sync/atomic"

	"github.com/grandcat/zeroconf"
)

// Usage:
// svc, _ := NewMDNSService()
//
//entries, _ := svc.LookupEntries()
//
//ctx, cancel := context.WithCancel(context.Background())
//go svc.Browse(ctx, "_EasyConn._tcp", "local.")
//
//for entry := range entries {
//    log.Println("FOUND:", entry.HostName, entry.Port)
//}

type MDNSService struct {
	resolver *zeroconf.Resolver
	entries  chan *zeroconf.ServiceEntry
	looking  int32
	stop     chan any
}

func NewMDNSService() (*MDNSService, error) {
	r, err := zeroconf.NewResolver(nil)
	if err != nil {
		return nil, err
	}

	return &MDNSService{
		resolver: r,
	}, nil
}

func (s *MDNSService) LookupEntries() (<-chan *zeroconf.ServiceEntry, error) {
	if !atomic.CompareAndSwapInt32(&s.looking, 0, 1) {
		return nil, errors.New("MDNS service already running")
	}

	s.entries = make(chan *zeroconf.ServiceEntry)
	s.stop = make(chan any)

	out := make(chan *zeroconf.ServiceEntry)

	go func(in <-chan *zeroconf.ServiceEntry, stop <-chan any, out chan<- *zeroconf.ServiceEntry) {
		defer close(out)

		for {
			select {
			case entry, ok := <-in:
				if !ok {
					log.Println("mDNS: input channel closed")
					return
				}
				out <- entry // forward entry to caller
			case <-stop:
				log.Println("Stopping lookup")
				return
			}
		}
	}(s.entries, s.stop, out)

	return out, nil
}

func (s *MDNSService) Browse(ctx context.Context, service string, domain string) error {
	err := s.resolver.Browse(ctx, service, domain, s.entries)
	if err != nil {
		return err
	}

	<-ctx.Done()
	close(s.stop)
	atomic.StoreInt32(&s.looking, 0)

	return nil
}
