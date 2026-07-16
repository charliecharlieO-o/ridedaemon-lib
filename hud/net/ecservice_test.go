package net

import (
	"io"
	"net"
	"testing"
)

func TestReadECResponseHandlesFragmentedTCPPacket(t *testing.T) {
	service := &ECService{}
	packet := service.encodePayload(&ECResponse{
		Code:      initOk,
		Separator: 0x70,
		Body:      []byte("{}\n"),
	})

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() {
		_, _ = server.Write(packet[:7])
		_, _ = server.Write(packet[7:])
	}()

	response, err := readECResponse(client)
	if err != nil {
		t.Fatalf("readECResponse() error = %v", err)
	}
	if response.Code != initOk {
		t.Fatalf("response code = %d, want %d", response.Code, initOk)
	}
}

func TestInitStreamCmdWithConnUsesProvidedConnection(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	service := &ECService{ip: "192.0.2.1", port: "10930", phoneType: "Android", packageName: "test"}
	go func() {
		header := make([]byte, 16)
		if _, err := io.ReadFull(server, header); err != nil {
			return
		}
		bodyLen := int(header[4]) | int(header[5])<<8
		_, _ = io.ReadFull(server, make([]byte, bodyLen-len(header)))
		response := service.encodePayload(&ECResponse{
			Code: initOk, Separator: 0x70, Body: []byte("{}\n"),
		})
		_, _ = server.Write(response)
	}()

	if err := service.InitStreamCmdWithConn(client); err != nil {
		t.Fatalf("InitStreamCmdWithConn() error = %v", err)
	}
}
