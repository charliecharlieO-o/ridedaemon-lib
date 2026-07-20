package core

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type orderedServer struct {
	name     string
	events   *[]string
	startErr error
}

func (s *orderedServer) Start() error {
	*s.events = append(*s.events, "start-"+s.name)
	return s.startErr
}

func (s *orderedServer) Stop(context.Context) error { return nil }

func TestReverseServersStartBeforeEasyConnInit(t *testing.T) {
	var events []string
	servers := []StoppableServer{
		&orderedServer{name: "pxc", events: &events},
		&orderedServer{name: "media-data", events: &events},
		&orderedServer{name: "media-control", events: &events},
	}

	started, err := startReverseServersThenInit(servers, func() error {
		events = append(events, "init")
		return nil
	})

	if err != nil {
		t.Fatalf("start reverse servers: %v", err)
	}
	if len(started) != len(servers) {
		t.Fatalf("started %d servers, want %d", len(started), len(servers))
	}
	want := []string{"start-pxc", "start-media-data", "start-media-control", "init"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("startup order = %v, want %v", events, want)
	}
}

func TestReverseServerFailurePreventsEasyConnInit(t *testing.T) {
	var events []string
	initCalled := false
	servers := []StoppableServer{
		&orderedServer{name: "pxc", events: &events},
		&orderedServer{name: "media-data", events: &events, startErr: errors.New("bind failed")},
		&orderedServer{name: "media-control", events: &events},
	}

	started, err := startReverseServersThenInit(servers, func() error {
		initCalled = true
		return nil
	})

	if err == nil {
		t.Fatal("server startup failure was ignored")
	}
	if initCalled {
		t.Fatal("EasyConn init ran before every reverse server was listening")
	}
	if len(started) != 1 {
		t.Fatalf("started %d servers, want one successfully opened server", len(started))
	}
}
