package webrtcav

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
)

func TestKeepMethodOnRedirectBlocksHTTPSDowngrade(t *testing.T) {
	client := keepMethodOnRedirect(&http.Client{})
	httpsReq := &http.Request{URL: &url.URL{Scheme: "https", Host: "example.com", Path: "/whip"}}
	httpReq := &http.Request{
		URL:      &url.URL{Scheme: "http", Host: "example.com", Path: "/whip"},
		Response: &http.Response{StatusCode: http.StatusTemporaryRedirect},
	}
	if err := client.CheckRedirect(httpReq, []*http.Request{httpsReq}); err == nil {
		t.Fatal("expected https->http 307 redirect to be rejected")
	}
}

func TestSDPFragmentGroupsByMidAndSkipsRejected(t *testing.T) {
	local := &webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP: strings.Join([]string{
			"v=0",
			"o=- 1 2 IN IP4 127.0.0.1",
			"s=-",
			"t=0 0",
			"a=ice-ufrag:sessufrag",
			"a=ice-pwd:sesspassword1234567890",
			"m=audio 0 UDP/TLS/RTP/SAVPF 111",
			"c=IN IP4 0.0.0.0",
			"a=mid:0",
			"m=video 9 UDP/TLS/RTP/SAVPF 96",
			"c=IN IP4 0.0.0.0",
			"a=mid:1",
			"",
		}, "\r\n"),
	}
	mid1 := "1"
	frag, err := sdpFragment(local, []webrtc.ICECandidateInit{
		{Candidate: ""},
		{Candidate: "candidate:1 1 udp 2122260223 192.0.2.1 54400 typ host", SDPMid: &mid1},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	got := string(frag)
	if strings.Contains(got, "m=audio") || strings.Contains(got, "a=\r\n") {
		t.Fatalf("unexpected rejected section or empty attribute in fragment:\n%s", got)
	}
	if !strings.Contains(got, "a=ice-ufrag:sessufrag\r\n") || !strings.Contains(got, "m=video 9") || !strings.Contains(got, "a=mid:1\r\n") {
		t.Fatalf("fragment missing session creds or video mid:\n%s", got)
	}
}

func TestAcceptsTricklePatchAndLinkedICEServersCaseInsensitive(t *testing.T) {
	if !acceptsTricklePatch([]string{"application/json", "Application/Trickle-ICE-SDPFrag"}) {
		t.Fatal("expected case-insensitive multi-header Accept-Patch match")
	}
	servers := linkedICEServers([]string{
		`<turn:turn.example.net?transport=udp>; rel="ICE-SERVER"; username="u"; credential="p\"q;r"`,
	})
	if len(servers) != 1 || servers[0].Username != "u" {
		t.Fatalf("servers = %+v", servers)
	}
}

func TestH264ProfileLevelIDExact7ByteAnnexB(t *testing.T) {
	extra := []byte{0x00, 0x00, 0x01, 0x67, 0x64, 0x00, 0x1f}
	if got := h264ProfileLevelID(extra); got != "64001f" {
		t.Fatalf("h264ProfileLevelID = %q, want 64001f", got)
	}
}

func TestMergeCodecParametersClearsStaleFieldsOnCodecSwitch(t *testing.T) {
	base := av.CodecParameters{
		ID:        av.CodecH264,
		Profile:   "high",
		Level:     "4.0",
		ExtraData: av.Buffer{Bytes: []byte{1, 2, 3}},
	}
	mergeCodecParameters(&base, av.CodecParameters{ID: av.CodecVP8})
	if base.ID != av.CodecVP8 || base.Profile != "" || base.Level != "" || len(base.ExtraData.Bytes) != 0 {
		t.Fatalf("stale fields retained after codec switch: %+v", base)
	}
}
