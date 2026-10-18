package goav

import (
	"context"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

type demuxBuild struct {
	source  pipeline.Source
	streams []av.Stream
}

// openDemuxSource opens input as a demuxing graph source. With a qualifier the
// source stamps every stream id it emits with it (see qualifiedDemuxer); the
// returned streams keep the demuxer's own ids, the ids selection matches.
func (b *builder) openDemuxSource(ctx context.Context, input format.Input, qualifier string) (demuxBuild, error) {
	inputProbe, err := b.runtime.formats.Probe(ctx, inputProbeRequest(input))
	if err != nil {
		return demuxBuild{}, inputFormatProbeError(input, err)
	}
	demuxFactory, err := b.runtime.formats.DemuxerFactory(inputProbe.Format)
	if err != nil {
		return demuxBuild{}, inputDemuxerMissingError(input, inputProbe.Format, err)
	}
	demuxer, err := demuxFactory.NewDemuxer(ctx, inputProbe)
	if err != nil {
		return demuxBuild{}, inputDemuxerMissingError(input, inputProbe.Format, err)
	}
	if demuxer == nil {
		return demuxBuild{}, format.ErrNilDemuxer
	}
	realtime := b.runtime.realtime || input.Realtime
	if err := demuxer.Open(ctx, input, format.OpenOptions{
		Realtime: realtime,
		Metadata: input.Metadata,
	}); err != nil {
		demuxer.Close()
		return demuxBuild{}, err
	}

	streams := demuxer.Streams()
	if len(streams) == 0 {
		streams = inputProbe.Streams
	}
	if qualifier != "" {
		demuxer = newQualifiedDemuxer(demuxer, qualifier)
	}
	source, err := format.NewDemuxSource(format.DemuxSourceConfig{
		Name:    demuxNodeName(input),
		Detail:  inputNodeDetail(input),
		Demuxer: demuxer,
		Result: format.ReadResult{
			Packet: &av.Packet{
				Payload: av.Buffer{Bytes: make([]byte, 0, demuxPacketBufferSize(streams))},
			},
			Events: make([]av.Event, 0, 1),
		},
		// A realtime task plays files paced — packets deliver when their media
		// time is due on the task timeline, which is what makes control.Rate work
		// on file inputs. Offline tasks pump at full speed.
		Realtime: realtime,
		Clock:    b.runtime.clock,
	})
	if err != nil {
		demuxer.Close()
		return demuxBuild{}, err
	}
	if realtime {
		markTimelinePaced(b.runtime.clock)
	}
	return demuxBuild{source: source, streams: streams}, nil
}

func inputProbeRequest(input format.Input) format.ProbeRequest {
	return format.ProbeRequest{
		Name:     input.Name,
		MIMEType: input.MIMEType,
		Input:    input,
	}
}

func demuxPacketBufferSize(streams []av.Stream) int {
	size := 64 * 1024
	for i := range streams {
		if streams[i].Type == av.MediaAudio || streams[i].Codec.Type == av.MediaAudio {
			size = maxInt(size, 4096)
		}
		if streams[i].Type == av.MediaVideo || streams[i].Codec.Type == av.MediaVideo {
			size = maxInt(size, 4<<20)
		}
	}
	return size
}

func maxInt(a int, b int) int {
	if b > a {
		return b
	}
	return a
}

// qualifyStreamID is a demuxed stream's id in a run with several inputs: the
// container's id with its input's qualifier ("1@a.webm"), so track 1 of two
// files stays two streams through selects, muxes, and sinks.
func qualifyStreamID(id av.StreamID, qualifier string) av.StreamID {
	if qualifier == "" || id == "" {
		return id
	}
	return id + av.StreamID(qualifier)
}

func qualifyStream(stream av.Stream, qualifier string) av.Stream {
	stream.ID = qualifyStreamID(stream.ID, qualifier)
	return stream
}

func qualifyStreams(streams []av.Stream, qualifier string) []av.Stream {
	if qualifier == "" {
		return streams
	}
	out := make([]av.Stream, len(streams))
	for i := range streams {
		out[i] = qualifyStream(streams[i], qualifier)
	}
	return out
}

// qualifiedDemuxer stamps the input's qualifier on every stream id its demuxer
// reports: the stream table, each packet, and each event. It rewrites the
// caller-owned read scratch in place, so it costs a map lookup per packet and
// no allocation. A demuxer that seeks keeps seeking (qualifiedSeekDemuxer).
type qualifiedDemuxer struct {
	format.Demuxer
	qualifier string
	ids       map[av.StreamID]av.StreamID
	// streams holds the stamped copies of the streams events carry.
	streams []av.Stream
}

type qualifiedSeekDemuxer struct {
	*qualifiedDemuxer
	seeker format.Seeker
}

func (d qualifiedSeekDemuxer) Seek(ctx context.Context, position time.Duration) error {
	return d.seeker.Seek(ctx, position)
}

func newQualifiedDemuxer(demuxer format.Demuxer, qualifier string) format.Demuxer {
	q := &qualifiedDemuxer{Demuxer: demuxer, qualifier: qualifier, ids: map[av.StreamID]av.StreamID{}}
	for _, stream := range demuxer.Streams() {
		q.id(stream.ID)
	}
	if seeker, ok := demuxer.(format.Seeker); ok {
		return qualifiedSeekDemuxer{qualifiedDemuxer: q, seeker: seeker}
	}
	return q
}

func (d *qualifiedDemuxer) id(id av.StreamID) av.StreamID {
	if id == "" {
		return ""
	}
	if qualified, ok := d.ids[id]; ok {
		return qualified
	}
	qualified := qualifyStreamID(id, d.qualifier)
	d.ids[id] = qualified
	return qualified
}

func (d *qualifiedDemuxer) Streams() []av.Stream {
	return qualifyStreams(d.Demuxer.Streams(), d.qualifier)
}

func (d *qualifiedDemuxer) ReadInto(ctx context.Context, result *format.ReadResult) error {
	err := d.Demuxer.ReadInto(ctx, result)
	if result.PacketReady && result.Packet != nil {
		result.Packet.StreamID = d.id(result.Packet.StreamID)
	}
	if cap(d.streams) < len(result.Events) {
		d.streams = make([]av.Stream, len(result.Events))
	}
	for i := range result.Events {
		event := &result.Events[i]
		event.StreamID = d.id(event.StreamID)
		if event.Stream != nil {
			// The event's stream belongs to the demuxer: stamp a copy.
			d.streams[i] = *event.Stream
			d.streams[i].ID = d.id(d.streams[i].ID)
			event.Stream = &d.streams[i]
		}
	}
	return err
}
