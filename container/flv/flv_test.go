package flv

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/av1"
	"github.com/thesyncim/goav/internal/h264"
	"github.com/thesyncim/goav/internal/hevc"
)

// The fixtures are ffmpeg 8 output, 1 s of 64x48 video at 10 fps and a
// 48 kHz mono tone: classic H.264 (with B-frames) and AAC, and E-FLV HEVC,
// AV1, VP9, and Opus, and a multitrack file with two AAC tracks.
func TestDemuxFixtures(t *testing.T) {
	for _, tc := range []struct {
		name         string
		video, audio av.CodecID
		// config checks the video ExtraData is its configuration record.
		config       func([]byte) bool
		videoPackets int
		audioPackets int
	}{
		{"h264_aac", av.CodecH264, av.CodecAAC, func(b []byte) bool { _, err := h264.ParseConfig(b); return err == nil }, 10, 48},
		{"hevc_aac", av.CodecHEVC, av.CodecAAC, func(b []byte) bool { _, err := hevc.ParseConfig(b); return err == nil }, 10, 48},
		{"av1_opus", av.CodecAV1, av.CodecOpus, func(b []byte) bool { _, err := av1.ParseConfig(b); return err == nil }, 10, 51},
		{"vp9_opus", av.CodecVP9, av.CodecOpus, func(b []byte) bool { return bytes.HasPrefix(b, []byte{1, 0, 0, 0}) && len(b) == 12 }, 10, 51},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := demuxBytes(t, fixture(t, tc.name))
			if len(got.streams) != 2 || got.streams[0].ID != "video" || got.streams[1].ID != "audio" {
				t.Fatalf("streams = %+v", got.streams)
			}
			video, audio := got.streams[0], got.streams[1]
			if video.Codec.ID != tc.video || audio.Codec.ID != tc.audio {
				t.Fatalf("codecs %s/%s, want %s/%s", video.Codec.ID, audio.Codec.ID, tc.video, tc.audio)
			}
			if !tc.config(video.Codec.ExtraData.Bytes) {
				t.Fatalf("video ExtraData % x is not its configuration record", video.Codec.ExtraData.Bytes)
			}
			if video.Codec.Width != 64 || video.Codec.Height != 48 || video.TimeBase != timeBase {
				t.Fatalf("video %dx%d base %v", video.Codec.Width, video.Codec.Height, video.TimeBase)
			}
			if audio.Codec.SampleRate != 48000 || audio.Codec.Channels != 1 || len(audio.Codec.ExtraData.Bytes) == 0 {
				t.Fatalf("audio %d Hz %d ch extradata %d", audio.Codec.SampleRate, audio.Codec.Channels, len(audio.Codec.ExtraData.Bytes))
			}
			if n := got.count("video"); n != tc.videoPackets {
				t.Fatalf("%d video packets, want %d", n, tc.videoPackets)
			}
			if n := got.count("audio"); n != tc.audioPackets {
				t.Fatalf("%d audio packets, want %d", n, tc.audioPackets)
			}
			if len(got.events) != 0 {
				t.Fatalf("events %+v", got.events)
			}
			firstVideo := true
			for _, p := range got.packets {
				if p.StreamID != "video" {
					continue
				}
				if firstVideo && !p.Keyframe {
					t.Fatalf("first video packet is not a keyframe")
				}
				firstVideo = false
				if (tc.video == av.CodecH264 || tc.video == av.CodecHEVC) && !h264.HasStartCode(p.Payload.Bytes) {
					t.Fatalf("%s packet is not Annex B: % x", tc.video, p.Payload.Bytes[:8])
				}
			}
		})
	}
}

// B-frames: H.264's composition time makes presentation run ahead of
// decode, and decode times stay in order.
func TestDemuxCompositionTime(t *testing.T) {
	got := demuxBytes(t, fixture(t, "h264_aac"))
	var reordered bool
	var last int64 = -1
	for _, p := range got.packets {
		if p.StreamID != "video" {
			continue
		}
		if p.DTS.Value < last {
			t.Fatalf("decode time %d after %d", p.DTS.Value, last)
		}
		last = p.DTS.Value
		reordered = reordered || p.PTS.Value > p.DTS.Value
	}
	if !reordered {
		t.Fatal("no packet presents after it decodes; the fixture has B-frames")
	}
}

func TestDemuxMultitrackFixture(t *testing.T) {
	got := demuxBytes(t, fixture(t, "multi"))
	var ids []string
	for _, s := range got.streams {
		ids = append(ids, string(s.ID)+"="+string(s.Codec.ID))
	}
	if strings.Join(ids, " ") != "video=h264 audio=aac audio-1=aac" {
		t.Fatalf("streams %v", ids)
	}
	if got.count("audio") != 48 || got.count("audio-1") != 48 {
		t.Fatalf("audio %d, audio-1 %d packets", got.count("audio"), got.count("audio-1"))
	}
}

// Every fixture survives demux, mux, demux: the same streams, codec
// configurations, timing, keyframes, and payloads, and the muxed bytes pass
// the structural validator.
func TestRemuxRoundTrip(t *testing.T) {
	for _, name := range []string{"h264_aac", "hevc_aac", "av1_opus", "vp9_opus", "multi"} {
		t.Run(name, func(t *testing.T) {
			first := demuxBytes(t, fixture(t, name))
			muxed := muxPackets(t, first.streams, first.packets)
			validateFLV(t, muxed)
			second := demuxBytes(t, muxed)
			if len(second.streams) != len(first.streams) {
				t.Fatalf("%d streams, want %d", len(second.streams), len(first.streams))
			}
			for i, s := range second.streams {
				want := first.streams[i]
				if s.ID != want.ID || s.Codec.ID != want.Codec.ID || !bytes.Equal(s.Codec.ExtraData.Bytes, want.Codec.ExtraData.Bytes) {
					t.Fatalf("stream %d = %s %s % x, want %s %s % x", i, s.ID, s.Codec.ID, s.Codec.ExtraData.Bytes, want.ID, want.Codec.ID, want.Codec.ExtraData.Bytes)
				}
			}
			if len(second.packets) != len(first.packets) {
				t.Fatalf("%d packets, want %d", len(second.packets), len(first.packets))
			}
			for i, p := range second.packets {
				want := first.packets[i]
				payload := want.Payload.Bytes
				if want.StreamID == "video" && first.streams[0].Codec.ID == av.CodecAV1 {
					payload, _ = av1.StripTemporalDelimiters(payload)
				}
				if p.StreamID != want.StreamID || p.DTS != want.DTS || p.PTS != want.PTS || p.Keyframe != want.Keyframe || !bytes.Equal(p.Payload.Bytes, payload) {
					t.Fatalf("packet %d = %s dts %d pts %d key %v %d bytes, want %s dts %d pts %d key %v %d bytes",
						i, p.StreamID, p.DTS.Value, p.PTS.Value, p.Keyframe, len(p.Payload.Bytes), want.StreamID, want.DTS.Value, want.PTS.Value, want.Keyframe, len(payload))
				}
			}
		})
	}
}

// A stream that declares no configuration gets one from its first
// keyframe's parameter sets, and video before that keyframe is dropped.
func TestMuxLearnsConfigFromKeyframe(t *testing.T) {
	for _, name := range []string{"h264_aac", "hevc_aac"} {
		t.Run(name, func(t *testing.T) {
			src := demuxBytes(t, fixture(t, name))
			video := src.streams[0]
			var sets []byte
			if video.Codec.ID == av.CodecH264 {
				c, _ := h264.ParseConfig(video.Codec.ExtraData.Bytes)
				sets = c.AppendParameterSets(nil)
			} else {
				c, _ := hevc.ParseConfig(video.Codec.ExtraData.Bytes)
				for _, nal := range c.ParameterSets {
					if kind := hevc.NALType(nal); kind >= hevc.NALVPS && kind <= hevc.NALPPS {
						sets = append(append(sets, h264.StartCode[:]...), nal...)
					}
				}
			}
			stream := video
			stream.Codec.ExtraData = av.Buffer{}
			// A live join: the first frame is not a keyframe, and the next
			// keyframe carries the parameter sets in band.
			var packets []av.Packet
			dropped := 0
			for _, p := range src.packets {
				if p.StreamID != "video" {
					continue
				}
				if len(packets) == 0 {
					p.Keyframe = false
				}
				if dropped == 0 && p.Keyframe {
					dropped = len(packets)
					p.Payload.Bytes = append(bytes.Clone(sets), p.Payload.Bytes...)
				}
				packets = append(packets, p)
			}
			muxed := muxPackets(t, []av.Stream{stream}, packets)
			validateFLV(t, muxed)
			got := demuxBytes(t, muxed)
			config := got.streams[0].Codec.ExtraData.Bytes
			if video.Codec.ID == av.CodecH264 {
				c, err := h264.ParseConfig(config)
				if err != nil || len(c.SPS) != 1 || len(c.PPS) != 1 {
					t.Fatalf("learned avcC % x: %v", config, err)
				}
			} else {
				want, _ := hevc.ParseConfig(video.Codec.ExtraData.Bytes)
				c, err := hevc.ParseConfig(config)
				if err != nil || len(c.ParameterSets) != 3 || !bytes.Equal(config[1:22], video.Codec.ExtraData.Bytes[1:22]) || len(want.ParameterSets) < 3 {
					t.Fatalf("learned hvcC % x, ffmpeg's % x: %v", config[:23], video.Codec.ExtraData.Bytes[:23], err)
				}
			}
			if len(got.packets) != len(packets)-dropped || !got.packets[0].Keyframe {
				t.Fatalf("%d packets, want %d (the %d before the first keyframe dropped)", len(got.packets), len(packets)-dropped, dropped)
			}
		})
	}
}

// Several tracks of each media mux as Enhanced RTMP multitrack (one track
// per message), with onMetaData track maps, and demux back to the same
// streams.
func TestMuxMultitrack(t *testing.T) {
	h264src := demuxBytes(t, fixture(t, "h264_aac"))
	hevcsrc := demuxBytes(t, fixture(t, "hevc_aac"))
	opussrc := demuxBytes(t, fixture(t, "av1_opus"))
	streams := []av.Stream{h264src.streams[0], h264src.streams[1], hevcsrc.streams[0], opussrc.streams[1]}
	streams[2].ID, streams[3].ID = "hevc", "opus"
	packets := append([]av.Packet(nil), h264src.packets...)
	for _, p := range hevcsrc.packets {
		if p.StreamID == "video" {
			p.StreamID = "hevc"
			packets = append(packets, p)
		}
	}
	for _, p := range opussrc.packets {
		if p.StreamID == "audio" {
			p.StreamID = "opus"
			packets = append(packets, p)
		}
	}
	sortByDTS(packets)
	muxed := muxPackets(t, streams, packets)
	validateFLV(t, muxed)
	got := demuxBytes(t, muxed)
	var ids []string
	for _, s := range got.streams {
		ids = append(ids, string(s.ID)+"="+string(s.Codec.ID))
	}
	if strings.Join(ids, " ") != "video=h264 audio=aac video-1=hevc audio-1=opus" {
		t.Fatalf("streams %v, events %+v", ids, got.events)
	}
	for id, want := range map[av.StreamID]int{"video": 10, "audio": 48, "video-1": 10, "audio-1": 51} {
		if n := got.count(id); n != want {
			t.Fatalf("%s: %d packets, want %d", id, n, want)
		}
	}
	ffprobeStreams(t, muxed, "h264,aac,hevc,opus")
}

func sortByDTS(packets []av.Packet) {
	for i := 1; i < len(packets); i++ {
		for j := i; j > 0 && packets[j].DTS.Value < packets[j-1].DTS.Value; j-- {
			packets[j], packets[j-1] = packets[j-1], packets[j]
		}
	}
}

// Timestamps past 24 bits use the extended byte, and past 32 bits (a live
// publish beyond 49.7 days) wrap on the wire and unwrap on read.
func TestExtendedTimestamps(t *testing.T) {
	src := demuxBytes(t, fixture(t, "av1_opus"))
	stream := src.streams[1]
	var packets []av.Packet
	for i, start := range []int64{0x00fffff0, 0x00fffff0 + 20, 1<<32 - 20, 1<<32 + 5, 1<<32 + 40} {
		p := src.packets[0]
		p.StreamID = "audio"
		p.DTS = av.Timestamp{Value: start, Base: timeBase}
		p.PTS = p.DTS
		if i == 0 {
			p.DTS.Value, p.PTS.Value = 0, 0 // the origin
		}
		packets = append(packets, p)
	}
	got := demuxBytes(t, muxPackets(t, []av.Stream{stream}, packets))
	for i, p := range got.packets {
		if p.DTS.Value != packets[i].DTS.Value {
			t.Fatalf("packet %d decodes at %d, want %d", i, p.DTS.Value, packets[i].DTS.Value)
		}
	}
}

// A seekable output gets onMetaData's duration and file size patched.
func TestMuxPatchesDurationOnSeekableOutput(t *testing.T) {
	src := demuxBytes(t, fixture(t, "h264_aac"))
	path := filepath.Join(t.TempDir(), "out.flv")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	m := &Muxer{}
	if err := m.Open(context.Background(), format.Output{Writer: file}, src.streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := range src.packets {
		if err := m.Write(context.Background(), &src.packets[i], nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	file.Close()
	data, _ := os.ReadFile(path)
	validateFLV(t, data)
	d := &Demuxer{}
	if err := d.Open(context.Background(), format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	duration, _ := number(d.hints, "duration")
	size, _ := number(d.hints, "filesize")
	if duration < 1 || duration > 1.3 || int(size) != len(data) {
		t.Fatalf("duration %v s, filesize %v of %d bytes", duration, size, len(data))
	}
}

// The muxer refuses what FLV cannot carry, naming it.
func TestMuxRefusals(t *testing.T) {
	for _, tc := range []struct {
		streams []av.Stream
		want    string
	}{
		{nil, "no streams"},
		{[]av.Stream{{ID: "s", Type: av.MediaVideo, Codec: av.CodecParameters{ID: "vvc"}}}, "not video vvc"},
		{[]av.Stream{{ID: "f", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecFLAC}}}, "needs its STREAMINFO"},
	} {
		err := (&Muxer{}).Open(context.Background(), format.Output{Writer: &bytes.Buffer{}}, tc.streams, format.OpenOptions{})
		if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("Open(%v) = %v, want ErrUnsupported naming %q", tc.streams, err, tc.want)
		}
	}
	m := &Muxer{}
	stream := av.Stream{ID: "a", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecAAC}}
	if err := m.Open(context.Background(), format.Output{Writer: &bytes.Buffer{}}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	packet := av.Packet{StreamID: "a", Payload: av.Buffer{Bytes: []byte{1, 2, 3}}, DTS: av.Timestamp{Base: timeBase}}
	if err := m.Write(context.Background(), &packet, nil); !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), "AudioSpecificConfig") {
		t.Fatalf("AAC without a config = %v", err)
	}
}

// ffprobeStreams checks ffprobe, when installed, reads data's streams as
// the comma-separated codec names want, and ffmpeg decodes it cleanly.
func ffprobeStreams(t *testing.T, data []byte, want string) {
	t.Helper()
	probe, err := exec.LookPath("ffprobe")
	if err != nil {
		return
	}
	path := filepath.Join(t.TempDir(), "probe.flv")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(probe, "-v", "error", "-show_entries", "stream=codec_name", "-of", "csv=p=0", path).CombinedOutput()
	if err != nil {
		t.Fatalf("ffprobe: %v\n%s", err, out)
	}
	if got := strings.Join(strings.Fields(string(out)), ","); got != want {
		t.Fatalf("ffprobe reads %s, want %s", got, want)
	}
	if ffmpeg, err := exec.LookPath("ffmpeg"); err == nil {
		out, err := exec.Command(ffmpeg, "-v", "error", "-i", path, "-map", "0", "-f", "null", "-").CombinedOutput()
		if err != nil || len(bytes.TrimSpace(out)) != 0 {
			t.Fatalf("ffmpeg decode: %v\n%s", err, out)
		}
	}
}

// ffprobe and ffmpeg (when installed) read every remuxed fixture: the
// classic and enhanced codecs and multitrack audio.
func TestFFprobeReadsMuxedFiles(t *testing.T) {
	for name, want := range map[string]string{
		"h264_aac": "h264,aac",
		"hevc_aac": "hevc,aac",
		"av1_opus": "av1,opus",
		"vp9_opus": "vp9,opus",
		"multi":    "h264,aac,aac",
	} {
		t.Run(name, func(t *testing.T) {
			src := demuxBytes(t, fixture(t, name))
			ffprobeStreams(t, muxPackets(t, src.streams, src.packets), want)
		})
	}
}

// The enhanced spec's branches, on hand-built tags: ModEx before the packet
// type, CodedFramesX, a negative composition time, both many-track layouts,
// command and metadata frames, an unknown FourCC, the unofficial legacy
// HEVC id, and encrypted tags.
func TestDemuxEnhancedBranches(t *testing.T) {
	avcC := demuxBytes(t, fixture(t, "h264_aac")).streams[0].Codec.ExtraData.Bytes
	asc := []byte{0x11, 0x90}
	frame := []byte{0, 0, 0, 2, 0x09, 0xf0} // one AVCC NAL: an access unit delimiter
	four := func(s string) []byte { return []byte(s) }
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	u24 := func(n int) []byte { return []byte{byte(n >> 16), byte(n >> 8), byte(n)} }
	data := rawFile(flagVideo|flagAudio,
		// Enhanced AVC sequence start behind a ModEx of 3 bytes.
		rawTag(tagVideo, 0, cat([]byte{0x90 | packetModEx, 2, 0, 0, 0, 0x00 | packetSequenceStart}, four("avc1"), avcC)),
		rawTag(tagAudio, 0, cat([]byte{0xaf, 0}, asc)),
		// CodedFrames with a negative composition time.
		rawTag(tagVideo, 40, cat([]byte{0x90 | packetCodedFrames}, four("avc1"), []byte{0xff, 0xff, 0xf6}, frame)),
		// CodedFramesX: composition time zero.
		rawTag(tagVideo, 80, cat([]byte{0xa0 | packetCodedFramesX}, four("avc1"), frame)),
		// A command frame and a metadata frame: skipped.
		rawTag(tagVideo, 90, []byte{0xd0 | packetCodedFrames, 'a', 'v', 'c', '1', 0}),
		rawTag(tagVideo, 90, cat([]byte{0xd0 | packetMetadata}, four("avc1"), []byte{2, 0, 1, 'x', 5})),
		// An unknown FourCC: skipped.
		rawTag(tagVideo, 95, cat([]byte{0x90 | packetCodedFrames}, four("vvc1"), []byte{0, 0, 0}, frame)),
		// An encrypted tag: skipped.
		append([]byte{0x20 | tagVideo}, rawTag(tagVideo, 96, frame)[1:]...),
		// ManyTracks AAC: tracks 0 and 2 in one message.
		rawTag(tagAudio, 100, cat([]byte{0x90 | audioMultitrack, manyTracks<<4 | packetCodedFrames}, four("mp4a"),
			[]byte{0}, u24(2), []byte{0xa0, 0xa1}, []byte{2}, u24(1), []byte{0xa2})),
		// ManyTracksManyCodecs: track 1 starts as HEVC (unknown config,
		// any bytes) and track 2 as AVC.
		rawTag(tagVideo, 120, cat([]byte{0x90 | videoMultitrack, manyTracksManyCodecs<<4 | packetSequenceStart},
			four("avc1"), []byte{2}, u24(len(avcC)), avcC)),
		rawTag(tagVideo, 120, cat([]byte{0x90 | videoMultitrack, manyTracksManyCodecs<<4 | packetCodedFrames},
			four("avc1"), []byte{2}, u24(3+len(frame)), []byte{0, 0, 0}, frame)),
		// The unofficial legacy HEVC id: its own track 0 cannot be, since
		// video track 0 is AVC; a codec change is not followed.
		rawTag(tagVideo, 130, cat([]byte{0x10 | legacyHEVC, 1, 0, 0, 0}, frame)),
	)
	got := demuxBytes(t, data)
	if len(got.streams) != 2 {
		t.Fatalf("streams %+v", got.streams)
	}
	if !bytes.Equal(got.streams[0].Codec.ExtraData.Bytes, avcC) || !bytes.Equal(got.streams[1].Codec.ExtraData.Bytes, asc) {
		t.Fatal("sequence starts not applied")
	}
	type want struct {
		id       av.StreamID
		dts, pts int64
		key      bool
		size     int
	}
	wants := []want{
		{"video", 40, 30, true, 6},
		{"video", 80, 80, false, 6},
		{"audio", 100, 100, true, 2},
		{"audio-2", 100, 100, true, 1},
		{"video-2", 120, 120, true, 6},
	}
	if len(got.packets) != len(wants) {
		for _, p := range got.packets {
			t.Logf("%s %d", p.StreamID, p.DTS.Value)
		}
		t.Fatalf("%d packets, want %d", len(got.packets), len(wants))
	}
	for i, w := range wants {
		p := got.packets[i]
		if p.StreamID != w.id || p.DTS.Value != w.dts || p.PTS.Value != w.pts || p.Keyframe != w.key || len(p.Payload.Bytes) != w.size {
			t.Fatalf("packet %d = %s %d/%d key %v %d bytes, want %+v", i, p.StreamID, p.DTS.Value, p.PTS.Value, p.Keyframe, len(p.Payload.Bytes), w)
		}
	}
	// Later tracks are announced before their first packet, configured.
	if got.order != "pppepep" {
		t.Fatalf("order %q", got.order)
	}
	if got.events[1].Stream == nil || !bytes.Equal(got.events[1].Stream.Codec.ExtraData.Bytes, avcC) {
		t.Fatalf("video-2 announced without its configuration: %+v", got.events[1])
	}
}

// A sequence start that changes mid-stream bumps the epoch and announces
// the new configuration; a repeated one changes nothing.
func TestDemuxCodecChange(t *testing.T) {
	src := demuxBytes(t, fixture(t, "h264_aac"))
	avcC := src.streams[0].Codec.ExtraData.Bytes
	changed := bytes.Clone(avcC)
	changed[3]++ // another level
	frame := []byte{0, 0, 0, 2, 0x09, 0xf0}
	config := func(record []byte) []byte { return append([]byte{0x17, 0, 0, 0, 0}, record...) }
	data := rawFile(flagVideo,
		rawTag(tagVideo, 0, config(avcC)),
		rawTag(tagVideo, 0, append([]byte{0x17, 1, 0, 0, 0}, frame...)),
		rawTag(tagVideo, 40, config(avcC)),
		rawTag(tagVideo, 40, append([]byte{0x27, 1, 0, 0, 0}, frame...)),
		rawTag(tagVideo, 80, config(changed)),
		rawTag(tagVideo, 80, append([]byte{0x17, 1, 0, 0, 0}, frame...)),
	)
	got := demuxBytes(t, data)
	if got.order != "ppep" || got.events[0].Type != av.EventCodecChanged || got.events[0].Epoch != 1 ||
		!bytes.Equal(got.events[0].Codec.ExtraData.Bytes, changed) || got.packets[2].CodecEpoch != 1 || got.packets[1].CodecEpoch != 0 {
		t.Fatalf("order %q events %+v", got.order, got.events)
	}
}

// onMetaData's track maps make the read-ahead wait for every track, and its
// geometry seeds the streams.
func TestDemuxMetadataExpectsTracks(t *testing.T) {
	src := demuxBytes(t, fixture(t, "h264_aac"))
	avcC := src.streams[0].Codec.ExtraData.Bytes
	meta := appendAMFString(nil, "onMetaData")
	meta = append(meta, amfECMAArray, 0, 0, 0, 0)
	meta = appendNumberProperty(meta, "videocodecid", 7)
	meta = appendNumberProperty(meta, "width", 1280)
	meta = appendNumberProperty(meta, "height", 720)
	meta = appendAMFName(meta, "videoTrackIdInfoMap")
	meta = append(meta, amfObject)
	meta = appendTrackEntry(meta, 1, appendNumberProperty(appendNumberProperty(nil, "width", 640), "height", 360))
	meta = appendAMFEnd(appendAMFEnd(meta))
	frame := []byte{0, 0, 0, 2, 0x09, 0xf0}
	data := rawFile(flagVideo,
		rawTag(tagScript, 0, meta),
		rawTag(tagVideo, 0, append([]byte{0x17, 0, 0, 0, 0}, avcC...)),
		rawTag(tagVideo, 0, append([]byte{0x17, 1, 0, 0, 0}, frame...)),
		rawTag(tagVideo, 33, append([]byte{0x90 | videoMultitrack, oneTrack<<4 | packetSequenceStart, 'a', 'v', 'c', '1', 1}, avcC...)),
		rawTag(tagVideo, 33, append([]byte{0x90 | videoMultitrack, oneTrack<<4 | packetCodedFramesX, 'a', 'v', 'c', '1', 1}, frame...)),
	)
	got := demuxBytes(t, data)
	if len(got.streams) != 2 || len(got.events) != 0 {
		t.Fatalf("streams %+v events %+v", got.streams, got.events)
	}
	if v := got.streams[0].Codec; v.Width != 1280 || v.Height != 720 {
		t.Fatalf("track 0 %dx%d", v.Width, v.Height)
	}
	if v := got.streams[1].Codec; got.streams[1].ID != "video-1" || v.Width != 640 || v.Height != 360 {
		t.Fatalf("track 1 %s %dx%d", got.streams[1].ID, v.Width, v.Height)
	}
}

// A file cut inside a tag ends at the last whole tag; bytes that are not
// FLV fail Open.
func TestDemuxTruncationAndGarbage(t *testing.T) {
	data := fixture(t, "h264_aac")
	whole := demuxBytes(t, data)
	cut := demuxBytes(t, data[:len(data)-30])
	if len(cut.packets) != len(whole.packets)-1 {
		t.Fatalf("cut file: %d packets, want %d", len(cut.packets), len(whole.packets)-1)
	}
	for _, bad := range [][]byte{nil, []byte("FLX\x01\x05\x00\x00\x00\x09"), []byte("FLV\x01\x05\x00\x00\x00\x03")} {
		if _, err := demux(bad); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("demux(% x) = %v, want ErrInvalidData", bad, err)
		}
	}
	// A stream that names its tags with a nonsense body size runs out of
	// bytes and ends rather than allocating what it claims.
	huge := rawFile(flagVideo, []byte{tagVideo, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 1, 2})
	if got, err := demux(huge); err != nil || len(got.packets) != 0 {
		t.Fatalf("huge tag: %v %+v", err, got)
	}
}

// The registered descriptor is the muxer's codec list.
func TestDescriptorMatchesMuxer(t *testing.T) {
	registry := format.NewRegistry()
	Register(registry)
	desc, err := registry.MuxerDescriptor(av.FormatFLV)
	if err != nil || len(desc.Codecs) != len(codecFourCC) {
		t.Fatalf("descriptor %+v: %v", desc, err)
	}
	for _, id := range desc.Codecs {
		if _, ok := codecFourCC[id]; !ok {
			t.Fatalf("descriptor names %s, which the muxer does not write", id)
		}
	}
	if _, err := registry.DemuxerFactory(av.FormatFLV); err != nil {
		t.Fatal(err)
	}
}
