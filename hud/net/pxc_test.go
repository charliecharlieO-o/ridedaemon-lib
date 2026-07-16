package net

import (
	"encoding/binary"
	"encoding/json"
	"io"
	stdnet "net"
	"testing"
	"time"
)

func TestUnknownEvenPXCRequestWithBodyIsAcknowledged(t *testing.T) {
	control := NewPXCControl(":0", nil, nil)
	responses := handlePXCEventAndReadResponses(t, control, &PXCResponse{
		Command: PxcOtaFtpInfo,
		Body:    json.RawMessage(`{"port":11021}`),
	}, 1)

	if responses[0].Command != PxcOtaFtpInfo+1 {
		t.Fatalf("ACK command = 0x%x, want 0x%x", responses[0].Command, PxcOtaFtpInfo+1)
	}
	if len(responses[0].Body) != 0 {
		t.Fatalf("ACK body length = %d, want 0", len(responses[0].Body))
	}
	assertNoPXCError(t, control)
}

func TestCheckSnSendsAckAndPositiveResult(t *testing.T) {
	control := NewPXCControl(":0", nil, nil)
	responses := handlePXCEventAndReadResponses(t, control, &PXCResponse{
		Command: PxcClientSet,
		Body:    json.RawMessage(`{"client_set":"easy_conn","sn":"SERIAL-123"}`),
	}, 2)

	if responses[0].Command != PxcCheckSnAck {
		t.Fatalf("CHECK_SN ACK command = 0x%x, want 0x%x", responses[0].Command, PxcCheckSnAck)
	}
	if responses[1].Command != PxcCheckSnResult {
		t.Fatalf("CHECK_SN result command = 0x%x, want 0x%x", responses[1].Command, PxcCheckSnResult)
	}
	var result checkSnResult
	if err := json.Unmarshal(responses[1].Body, &result); err != nil {
		t.Fatalf("decode CHECK_SN result: %v", err)
	}
	if !result.IsOK || result.ID != "SERIAL-123" || result.ClientSet != "easy_conn" {
		t.Fatalf("unexpected CHECK_SN result: %+v", result)
	}
	assertNoPXCError(t, control)
}

func TestUnknownOddPXCResponseIsNotAcknowledged(t *testing.T) {
	control := NewPXCControl(":0", nil, nil)
	client, server := stdnet.Pipe()
	defer client.Close()
	defer server.Close()
	if err := server.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	control.handleEvent(&PXCResponse{Command: PxcMediaFeatureConf + 1}, client)
	buffer := make([]byte, 1)
	if _, err := server.Read(buffer); err == nil {
		t.Fatal("unknown odd PXC response unexpectedly produced an ACK")
	}
	assertNoPXCError(t, control)
}

func handlePXCEventAndReadResponses(
	t *testing.T,
	control *PXCControl,
	event *PXCResponse,
	count int,
) []PXCResponse {
	t.Helper()
	client, server := stdnet.Pipe()
	defer client.Close()
	defer server.Close()
	if err := server.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		control.handleEvent(event, client)
		close(done)
	}()

	responses := make([]PXCResponse, 0, count)
	for i := 0; i < count; i++ {
		header := make([]byte, pxcHeaderSize)
		if _, err := io.ReadFull(server, header); err != nil {
			t.Fatalf("read PXC response header %d: %v", i, err)
		}
		response := PXCResponse{
			Command: binary.LittleEndian.Uint32(header[0:4]),
			Size:    binary.LittleEndian.Uint32(header[4:8]),
			Magic:   binary.LittleEndian.Uint32(header[8:12]),
			Token:   binary.LittleEndian.Uint32(header[12:16]),
		}
		if response.Size < pxcHeaderSize {
			t.Fatalf("invalid PXC response size %d", response.Size)
		}
		if response.Magic != response.Size^response.Command {
			t.Fatalf("invalid PXC response magic 0x%x", response.Magic)
		}
		body := make([]byte, int(response.Size)-pxcHeaderSize)
		if _, err := io.ReadFull(server, body); err != nil {
			t.Fatalf("read PXC response body %d: %v", i, err)
		}
		response.Body = body
		responses = append(responses, response)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("PXC handler did not complete")
	}
	return responses
}

func assertNoPXCError(t *testing.T, control *PXCControl) {
	t.Helper()
	select {
	case err := <-control.Errors:
		t.Fatalf("unexpected PXC error: %v", err)
	default:
	}
}
