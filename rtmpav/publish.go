package rtmpav

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/flv"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/amf0"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// publisher is the destination an rtmp URL binds to: a media sink that
// muxes FLV and sends each tag as an RTMP message.
type publisher struct {
	target target
	config config

	session *session
	muxer   flv.Muxer
	sender  tagSender
	// failed is the error the server's messages ended the publish with.
	failed    chan struct{}
	failure   error
	failOnce  sync.Once
	closeOnce sync.Once
	closeErr  error
}

var (
	_ pipeline.Sink            = (*publisher)(nil)
	_ pipeline.NodeDescriber   = (*publisher)(nil)
	_ interface{ Live() bool } = (*publisher)(nil)
)

// takes is what a publish takes, in preference order: the classic codecs,
// then the enhanced ones a server may negotiate.
var takes = shape.Set{
	shape.Packet(av.MediaVideo, av.CodecH264),
	shape.Packet(av.MediaVideo, av.CodecHEVC),
	shape.Packet(av.MediaVideo, av.CodecAV1),
	shape.Packet(av.MediaVideo, av.CodecVP9),
	shape.Packet(av.MediaAudio, av.CodecAAC),
	shape.Packet(av.MediaAudio, av.CodecOpus),
	shape.Packet(av.MediaAudio, av.CodecFLAC),
}

func (p *publisher) Name() string { return "rtmp" }

// DescribeNode details the node in Describe output.
func (p *publisher) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: p.Name(), Kind: pipeline.NodeSink, Detail: "rtmp publish"}
}

// Live reports a publish consumes in real time: the planner paces a file
// routed to it.
func (p *publisher) Live() bool { return true }

// InputShapes is what a publish takes: H.264, HEVC, AV1, and VP9 video,
// AAC, Opus, and FLAC audio.
func (p *publisher) InputShapes() shape.Set { return takes.Clone() }

// EncodeShapes is what frames are encoded to: H.264 and AAC, which every
// server reads.
func (p *publisher) EncodeShapes() shape.Set { return shape.Set{takes[0], takes[4]} }

// OpenStreams connects, publishes, and writes the FLV header, onMetaData,
// and sequence starts.
func (p *publisher) OpenStreams(ctx context.Context, streams []av.Stream) error {
	if p.session != nil {
		return errors.New("rtmpav: publish opened twice")
	}
	s, err := dial(ctx, p.target, p.config, true)
	if err != nil {
		return err
	}
	if err := s.caps.check(streams); err != nil {
		s.close()
		return err
	}
	s.writeTimeout = writeTimeout
	p.session, p.failed = s, make(chan struct{})
	p.sender = tagSender{session: s}
	go p.watch(s)
	if err := p.muxer.Open(ctx, format.Output{Writer: &p.sender, Realtime: true}, streams, format.OpenOptions{Realtime: true}); err != nil {
		err = p.err(err)
		s.close()
		p.session = nil
		return err
	}
	return nil
}

// watch reads what the server sends while the publish runs: it answers
// pings and acknowledges, and records the error that ends the publish, a
// refusal or the connection's end.
func (p *publisher) watch(s *session) {
	for {
		msg, err := s.read(0)
		if err != nil {
			p.fail(fmt.Errorf("rtmpav: %s: the connection ended: %w", p.target.name, err))
			return
		}
		if msg.typ != typeCommandAMF0 && msg.typ != typeCommandAMF3 {
			continue
		}
		cmd, err := decodeCommand(msg)
		if err != nil || cmd.name != "onStatus" {
			continue
		}
		if level, _ := cmd.info().String("level"); level == "error" {
			p.fail(fmt.Errorf("rtmpav: %s: %w", p.target.name, s.refusal("publish", cmd.info())))
			return
		}
	}
}

func (p *publisher) fail(err error) {
	p.failOnce.Do(func() {
		p.failure = err
		close(p.failed)
	})
}

// err prefers the server's account of a failed write.
func (p *publisher) err(err error) error {
	select {
	case <-p.failed:
		return p.failure
	default:
		return fmt.Errorf("rtmpav: %s: %w", p.target.name, err)
	}
}

// Handle publishes one packet.
func (p *publisher) Handle(ctx context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessagePacket || msg.Packet == nil {
		return nil
	}
	if p.session == nil {
		return errors.New("rtmpav: publish not opened")
	}
	select {
	case <-p.failed:
		return p.failure
	default:
	}
	if err := p.muxer.Write(ctx, msg.Packet, nil); err != nil {
		return p.err(err)
	}
	return nil
}

// Close ends each video sequence, unpublishes, and closes the connection.
func (p *publisher) Close() error {
	p.closeOnce.Do(func() {
		if p.session == nil {
			return
		}
		select {
		case <-p.failed:
		default:
			p.closeErr = p.muxer.Close()
		}
		p.session.close()
	})
	return p.closeErr
}

// tagSender takes the FLV bytes the muxer writes, one whole tag per write,
// and sends each tag as an RTMP message: its body unchanged, onMetaData
// as @setDataFrame, as publishers send it.
type tagSender struct {
	session *session
	header  bool
}

var setDataFrame, _ = amf0.Append(nil, "@setDataFrame")

func (t *tagSender) Write(p []byte) (int, error) {
	n := len(p)
	if !t.header {
		// The FLV header and the first PreviousTagSize.
		if len(p) < 13 {
			return 0, errors.New("rtmpav: short FLV header")
		}
		t.header, p = true, p[13:]
	}
	for len(p) > 0 {
		if len(p) < 15 {
			return 0, errors.New("rtmpav: partial FLV tag")
		}
		size := int(p[1])<<16 | int(p[2])<<8 | int(p[3])
		if len(p) < 15+size {
			return 0, errors.New("rtmpav: partial FLV tag")
		}
		timestamp := uint32(p[7])<<24 | uint32(p[4])<<16 | uint32(p[5])<<8 | uint32(p[6])
		msg := message{typ: p[0], streamID: t.session.streamID, timestamp: timestamp, body: p[11 : 11+size]}
		switch msg.typ {
		case typeAudio:
			msg.chunkStream = csAudio
		case typeVideo:
			msg.chunkStream = csVideo
		default:
			msg.chunkStream, msg.typ = csData, typeDataAMF0
			msg.body = append(append([]byte(nil), setDataFrame...), msg.body...)
		}
		if err := t.session.write(msg); err != nil {
			return 0, err
		}
		p = p[15+size:]
	}
	return n, nil
}
