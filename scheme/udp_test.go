package scheme_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/scheme"
)

// freeUDPPort returns a loopback port nothing listens on.
func freeUDPPort(t *testing.T) int {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).Port
}

// awaitUDPListener waits until something holds the port.
func awaitUDPListener(t *testing.T, port int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
		if err != nil {
			return
		}
		conn.Close()
	}
	t.Fatal("the receiver never listened")
}

// TestUDPCarriesATransportStream sends an MP4 as MPEG-TS over loopback UDP
// and receives it with a second run: the sender plays at the file's media
// time, and every packet arrives.
func TestUDPCarriesATransportStream(t *testing.T) {
	data, err := os.ReadFile("../container/mp4/testdata/h264_aac.mp4")
	if err != nil {
		t.Fatal(err)
	}
	port := freeUDPPort(t)
	addr := fmt.Sprintf("udp://127.0.0.1:%d", port)
	rt := bundle.MustNew()
	plan, err := rt.Describe(goav.URIInput(addr).To(goav.Write("got.ts", &bytes.Buffer{})))
	if err != nil || !plan.Realtime {
		t.Fatalf("receive plan realtime=%v err=%v", plan.Realtime, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	received := goavtest.NewCollector()
	receiveCtx, stopReceiving := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- rt.Run(receiveCtx, goav.URIInput(addr).To(received.Sink())) }()
	awaitUDPListener(t, port)
	start := time.Now()
	if err := rt.Run(ctx, goav.FileInput("in.mp4", bytes.NewReader(data)).To(goav.URI(addr))); err != nil {
		t.Fatalf("send: %v", err)
	}
	if took := time.Since(start); took < 300*time.Millisecond {
		t.Fatalf("a 500ms file went out in %v: the destination is not paced", took)
	}
	waitCtx, stopWaiting := context.WithTimeout(ctx, 3*time.Second)
	packets, err := received.WaitPackets(waitCtx, 29)
	stopWaiting()
	stopReceiving()
	if runErr := <-done; runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("receive: %v", runErr)
	}
	if err != nil {
		t.Fatalf("received %d of the 29 packets sent: %v", len(received.Packets()), err)
	}
	kinds := map[av.MediaType]int{}
	for _, p := range packets {
		kinds[p.Type]++
	}
	if kinds[av.MediaVideo] != 6 || kinds[av.MediaAudio] != 23 {
		t.Fatalf("received %v packets, sent 6 video and 23 audio", kinds)
	}
}

// TestUDPRefusesBadURLs names the fix when binding a udp URL.
func TestUDPRefusesBadURLs(t *testing.T) {
	udp := scheme.UDP()
	for raw, want := range map[string]string{
		"udp://127.0.0.1":                "no port",
		"udp://:5000?ttl=4":              "unknown parameter \"ttl\"",
		"udp://10.0.0.1:5000?iface=eth0": "not a multicast group",
	} {
		u, _ := url.Parse(raw)
		_, err := udp.(provider.InputScheme).Input(u)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Input(%s) = %v, want %q", raw, err, want)
		}
	}
	u, _ := url.Parse("udp://:5000")
	if _, err := udp.(provider.DestinationScheme).Destination(u); err == nil || !strings.Contains(err.Error(), "no host") {
		t.Errorf("Destination(udp://:5000) = %v", err)
	}
}

// TestUDPJoinsMulticast reads a datagram sent to a multicast group the
// input joined. Hosts without a multicast route skip it.
func TestUDPJoinsMulticast(t *testing.T) {
	port := freeUDPPort(t)
	u, _ := url.Parse(fmt.Sprintf("udp://239.255.71.%d:%d", 1+port%200, port))
	in, err := scheme.UDP().(provider.InputScheme).Input(u)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := in.(provider.ByteSource).OpenBytes(ctx)
	if err != nil {
		t.Skipf("no multicast here: %v", err)
	}
	defer r.Close()
	if live, ok := r.(interface{ Live() bool }); !ok || !live.Live() {
		t.Fatal("the udp input is not live")
	}
	out, err := scheme.UDP().(provider.DestinationScheme).Destination(u)
	if err != nil {
		t.Fatal(err)
	}
	w, err := out.(provider.Destination).Open(ctx, provider.Info{})
	if err != nil {
		t.Skipf("no multicast route: %v", err)
	}
	defer w.Close()
	packet := append([]byte{0x47}, bytes.Repeat([]byte{0xff}, 187)...)
	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 188)
		if _, err := io.ReadFull(r, buf); err == nil {
			got <- buf
		}
	}()
	for range 20 {
		if _, err := w.Write(packet); err != nil {
			t.Skipf("no multicast route: %v", err)
		}
		select {
		case b := <-got:
			if !bytes.Equal(b, packet) {
				t.Fatalf("read % x", b[:8])
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Skip("multicast does not loop back here")
}
