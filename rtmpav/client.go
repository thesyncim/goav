package rtmpav

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/flv/amf0"
)

const (
	// dialTimeout bounds the TCP connect; setupTimeout everything from
	// there to the publish or play starting.
	dialTimeout  = 10 * time.Second
	setupTimeout = 15 * time.Second
	// idleTimeout bounds a play's wait for its next message.
	idleTimeout = 30 * time.Second
	// writeTimeout bounds a publish's wait to send one message.
	writeTimeout = 30 * time.Second
)

// fourCCs are the Enhanced RTMP codec identifiers of the codecs goav
// carries.
var fourCCs = map[av.CodecID]string{
	av.CodecH264: "avc1",
	av.CodecHEVC: "hvc1",
	av.CodecAV1:  "av01",
	av.CodecVP9:  "vp09",
	av.CodecVP8:  "vp08",
	av.CodecAAC:  "mp4a",
	av.CodecOpus: "Opus",
	av.CodecFLAC: "fLaC",
}

// Enhanced RTMP capability flags.
const (
	fourCCCanDecode  = 0x01
	fourCCCanEncode  = 0x02
	fourCCCanForward = 0x04
	capsMultitrack   = 0x02
	capsModEx        = 0x04
)

// enhancedCommand is what connect states of Enhanced RTMP: the FourCC
// list and the per-codec maps the spec now recommends, flags set to
// codec, and the extended capabilities.
func enhancedCommand(flags float64, caps float64) amf0.Object {
	var list []any
	video, audio := amf0.Object{}, amf0.Object{}
	for _, id := range []av.CodecID{av.CodecAV1, av.CodecVP9, av.CodecVP8, av.CodecHEVC, av.CodecH264, av.CodecOpus, av.CodecFLAC, av.CodecAAC} {
		code := fourCCs[id]
		list = append(list, code)
		entry := amf0.Property{Name: code, Value: flags}
		if id == av.CodecOpus || id == av.CodecFLAC || id == av.CodecAAC {
			audio = append(audio, entry)
		} else {
			video = append(video, entry)
		}
	}
	return amf0.Object{
		{Name: "fourCcList", Value: list},
		{Name: "videoFourCcInfoMap", Value: video},
		{Name: "audioFourCcInfoMap", Value: audio},
		{Name: "capsEx", Value: caps},
	}
}

// capabilities is what a server's connect result says of Enhanced RTMP:
// nothing (declared false), or the codecs it takes and whether it takes
// multitrack.
type capabilities struct {
	declared   bool
	codecs     []string
	any        bool
	multitrack bool
}

func readCapabilities(objects ...amf0.Object) capabilities {
	var c capabilities
	for _, obj := range objects {
		if list, ok := obj.Get("fourCcList"); ok {
			c.declared = true
			items, _ := list.([]any)
			for _, item := range items {
				if code, ok := item.(string); ok {
					c.codecs = append(c.codecs, code)
				}
			}
		}
		for _, name := range []string{"videoFourCcInfoMap", "audioFourCcInfoMap"} {
			if codecs, ok := obj.Object(name); ok {
				c.declared = true
				for _, p := range codecs {
					if flags, _ := p.Value.(float64); flags != 0 {
						c.codecs = append(c.codecs, p.Name)
					}
				}
			}
		}
		if caps, ok := obj.Number("capsEx"); ok {
			c.declared = true
			c.multitrack = c.multitrack || int(caps)&capsMultitrack != 0
		}
	}
	c.any = slices.Contains(c.codecs, "*")
	return c
}

// check refuses the streams a server that declared its capabilities does
// not take: an enhanced codec it did not list, or several tracks of one
// media without multitrack. H.264 and AAC on the first track of their
// media go out with the legacy headers every server reads.
func (c capabilities) check(streams []av.Stream) error {
	if !c.declared {
		return nil
	}
	tracks := map[av.MediaType]int{}
	for _, stream := range streams {
		media := stream.Type
		tracks[media]++
		legacy := tracks[media] == 1 && (stream.Codec.ID == av.CodecH264 || stream.Codec.ID == av.CodecAAC)
		if legacy {
			continue
		}
		if tracks[media] > 1 && !c.multitrack && len(c.codecs) > 0 {
			return fmt.Errorf("%w: stream %s is a second %s track, and the server takes no multitrack: route one %s stream to it", ErrCodecRefused, stream.ID, media, media)
		}
		if !c.any && !slices.Contains(c.codecs, fourCCs[stream.Codec.ID]) {
			fix := ".Decode().Encode(codec.H264())"
			if media == av.MediaAudio {
				fix = `.Decode().Encode(codec.Codec("aac", "audio"))`
			}
			return fmt.Errorf("%w: stream %s is %s, and the server takes %s: encode it, %s", ErrCodecRefused, stream.ID, stream.Codec.ID, strings.Join(c.codecs, ", "), fix)
		}
	}
	return nil
}

// session is a client connection that connected to an application and
// created the stream it publishes or plays.
type session struct {
	*conn
	target   target
	publish  bool
	streamID uint32
	caps     capabilities
	tx       float64
	// early holds the media a server sent before the play's status.
	early []message
}

// dial connects, handshakes, connects to the application, and starts a
// publish or a play. ctx bounds the whole setup.
func dial(ctx context.Context, t target, c config, publish bool) (*session, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout+setupTimeout)
	defer cancel()
	nc, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, "tcp", t.address)
	if err != nil {
		return nil, fmt.Errorf("rtmpav: connect %s: %w", t.name, err)
	}
	stop := context.AfterFunc(ctx, func() { nc.Close() })
	defer stop()
	_ = nc.SetDeadline(time.Now().Add(setupTimeout))
	if t.tls {
		config := &tls.Config{}
		if c.tls != nil {
			config = c.tls.Clone()
		}
		if config.ServerName == "" {
			config.ServerName = t.host
		}
		tc := tls.Client(nc, config)
		if err := tc.HandshakeContext(ctx); err != nil {
			nc.Close()
			return nil, fmt.Errorf("rtmpav: tls with %s: %w", t.name, err)
		}
		nc = tc
	}
	s := &session{conn: newConn(nc), target: t, publish: publish}
	if err := s.setup(); err != nil {
		nc.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("rtmpav: %s: %w", t.name, ctxErr)
		}
		return nil, fmt.Errorf("rtmpav: %s: %w", t.name, err)
	}
	_ = nc.SetDeadline(time.Time{})
	return s, nil
}

func (s *session) setup() error {
	if err := clientHandshake(s.nc); err != nil {
		return err
	}
	if err := s.announce(false); err != nil {
		return err
	}
	connect := amf0.Object{
		{Name: "app", Value: s.target.app},
		{Name: "type", Value: "nonprivate"},
		{Name: "flashVer", Value: "FMLE/3.0 (compatible; goav)"},
		{Name: "tcUrl", Value: s.target.tcURL},
		{Name: "fpad", Value: false},
		{Name: "capabilities", Value: 15.0},
		{Name: "audioCodecs", Value: 0x0fff},
		{Name: "videoCodecs", Value: 0x00ff},
		{Name: "videoFunction", Value: 1.0},
		{Name: "objectEncoding", Value: 0.0},
	}
	if s.publish {
		connect = append(connect, enhancedCommand(fourCCCanEncode, capsMultitrack)...)
	} else {
		connect = append(connect, enhancedCommand(fourCCCanDecode|fourCCCanForward, capsMultitrack|capsModEx)...)
	}
	result, err := s.call("connect", connect)
	if err != nil {
		return err
	}
	s.caps = readCapabilities(result.object, result.info())
	if s.publish {
		// The release and FCPublish answers, which some servers never send,
		// are not awaited.
		if err := s.send(0, "releaseStream", nil, s.target.key); err != nil {
			return err
		}
		if err := s.send(0, "FCPublish", nil, s.target.key); err != nil {
			return err
		}
	}
	created, err := s.call("createStream", nil)
	if err != nil {
		return err
	}
	id, ok := created.args[0].(float64)
	if !ok || id < 0 || id > 1<<31 {
		return fmt.Errorf("%w: createStream returned no stream id", errProtocol)
	}
	s.streamID = uint32(id)
	if s.publish {
		if err := s.send(s.streamID, "publish", nil, s.target.key, "live"); err != nil {
			return err
		}
		return s.status("publish", "NetStream.Publish.Start")
	}
	if err := s.userControl(eventSetBufferLength, s.streamID, 3000); err != nil {
		return err
	}
	if err := s.send(s.streamID, "play", nil, s.target.key, -2.0); err != nil {
		return err
	}
	return s.status("play", "NetStream.Play.Start")
}

// send sends a command, numbering it.
func (s *session) send(streamID uint32, name string, values ...any) error {
	s.tx++
	return s.command(csCommand, streamID, append([]any{name, s.tx}, values...)...)
}

// call sends a command and returns its _result, or the *StatusError of
// its _error.
func (s *session) call(name string, values ...any) (command, error) {
	if err := s.send(0, name, values...); err != nil {
		return command{}, err
	}
	tx := s.tx
	for {
		cmd, err := s.nextCommand()
		if err != nil {
			return command{}, err
		}
		if cmd.tx != tx {
			continue
		}
		switch cmd.name {
		case "_result":
			if len(cmd.args) == 0 {
				cmd.args = []any{nil}
			}
			return cmd, nil
		case "_error":
			return command{}, s.refusal(name, cmd.info())
		}
	}
}

// status waits for the onStatus that starts a publish or play.
func (s *session) status(name, start string) error {
	for {
		cmd, err := s.nextCommand()
		if err != nil {
			return err
		}
		if cmd.name != "onStatus" {
			continue
		}
		info := cmd.info()
		code, _ := info.String("code")
		level, _ := info.String("level")
		switch {
		case code == start:
			return nil
		case level == "error" || strings.HasSuffix(code, ".Failed") || strings.HasSuffix(code, ".BadName") || strings.HasSuffix(code, ".Rejected"):
			return s.refusal(name, info)
		}
	}
}

// nextCommand reads up to the next command, keeping media that arrives
// first for the play.
func (s *session) nextCommand() (command, error) {
	for {
		msg, err := s.read(0)
		if err != nil {
			return command{}, err
		}
		switch msg.typ {
		case typeCommandAMF0, typeCommandAMF3:
			return decodeCommand(msg)
		case typeAudio, typeVideo, typeDataAMF0, typeDataAMF3, typeAggregate:
			if !s.publish {
				s.early = append(s.early, msg)
			}
		}
	}
}

// refusal is the *StatusError of a refused command.
func (s *session) refusal(name string, info amf0.Object) error {
	code, _ := info.String("code")
	description, _ := info.String("description")
	return &StatusError{Command: name, Code: s.target.scrub(code), Description: s.target.scrub(description)}
}

// close ends the publish or play and the connection.
func (s *session) close() error {
	_ = s.nc.SetWriteDeadline(time.Now().Add(time.Second))
	if s.publish {
		_ = s.send(0, "FCUnpublish", nil, s.target.key)
	}
	_ = s.send(0, "deleteStream", nil, float64(s.streamID))
	return s.Close()
}
