package api

import "testing"

func TestBuildAnnexBAUFromAVCCRejectsTruncatedNAL(t *testing.T) {
	_, err := BuildAnnexBAUFromAVCC([]byte{0, 0, 0, 4, 0x65})
	if err == nil {
		t.Fatal("BuildAnnexBAUFromAVCC() accepted a truncated NAL")
	}
}

func TestNewMobileSessionAllowsLiveOnlyMode(t *testing.T) {
	config := NewMobileConfig(nil, 30, 10, 5, 10, 3)
	session, err := NewMobileSession(config, nil)
	if err != nil {
		t.Fatalf("create live-only mobile session: %v", err)
	}
	defer session.StopSession()
	if session.mux.NoSignal != nil {
		t.Fatal("live-only session unexpectedly has a static fallback")
	}
}
