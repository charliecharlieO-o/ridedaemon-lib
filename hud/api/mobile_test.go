package api

import "testing"

func TestBuildAnnexBAUFromAVCCRejectsTruncatedNAL(t *testing.T) {
	_, err := BuildAnnexBAUFromAVCC([]byte{0, 0, 0, 4, 0x65})
	if err == nil {
		t.Fatal("BuildAnnexBAUFromAVCC() accepted a truncated NAL")
	}
}
