package compile

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

const diagnosticEncodeInserted = "encode_inserted"

// planSinkEncodes writes the .Encode a recipe left out before a media sink
// that takes only packets (an RTP or WHIP sender): a stream that reaches one
// as frames gets an encode to the first codec of its media the sink accepts
// and the runtime can encode, as if the recipe had written it, so
// validation, shape solving, naming, and Describe treat it like any other.
// A stream it cannot fix is left to validateDestinationShapes, which refuses
// it with the fix.
func (s *recipeState) planSinkEncodes() error {
	if !s.env.Present {
		return nil
	}
	g := &s.req.Graph
	inserted := map[string]recipeir.NodeID{}
	var outputs []Output
	for _, out := range s.req.Outputs {
		if g.Nodes[out.From].Kind == recipeir.NodeInput || !endsFrames(s.req, out.From) {
			outputs = append(outputs, out)
			continue
		}
		kept := Output{From: out.From}
		for _, dest := range out.Dests {
			spec, ok := s.sinkEncode(g.Nodes[dest], out.From)
			if !ok {
				kept.Dests = append(kept.Dests, dest)
				continue
			}
			key := fmt.Sprintf("%d/%s", out.From, spec.ID)
			encode, ok := inserted[key]
			if !ok {
				encode = recipeir.NodeID(len(g.Nodes))
				g.Nodes = append(g.Nodes, recipeir.Node{Kind: recipeir.NodeOp, Parents: []recipeir.NodeID{out.From}, Op: recipeir.Operation{Kind: plan.OpEncode, Component: string(spec.ID), Encode: spec}})
				inserted[key] = encode
				s.shapeDiagnostics = append(s.shapeDiagnostics, plan.Diagnostic{
					Code:    diagnosticEncodeInserted,
					Node:    s.req.Dests[g.Nodes[dest].Output].Label,
					Message: fmt.Sprintf("inserted encode-%s before the destination, which takes %s", spec.ID, acceptedCodecs(s.req.Dests[g.Nodes[dest].Output].Accepts, spec.Type)),
				})
			}
			parents := slices.Clone(g.Nodes[dest].Parents)
			for i := range parents {
				if parents[i] == out.From {
					parents[i] = encode
				}
			}
			g.Nodes[dest].Parents = parents
			outputs = append(outputs, Output{From: encode, Dests: []recipeir.NodeID{dest}})
		}
		if len(kept.Dests) != 0 {
			outputs = append(outputs, kept)
		}
	}
	s.req.Outputs = outputs
	return nil
}

// sinkEncode is the encode a frame stream from node from needs to reach the
// destination node dest: the first packet codec of its media dest accepts
// whose encoder the runtime has.
func (s *recipeState) sinkEncode(dest recipeir.Node, from recipeir.NodeID) (codec.Spec, bool) {
	accepts := s.req.Dests[dest.Output].Accepts
	if dest.Kind != recipeir.NodeOutput || !s.req.Dests[dest.Output].Sink || len(accepts) == 0 || acceptsDomain(accepts, shape.DomainFrame) {
		return codec.Spec{}, false
	}
	media := pathMedia(s.req, from)
	if media == "" {
		return codec.Spec{}, false
	}
	for _, accepted := range accepts {
		if accepted.Codec == "" || accepted.MediaKind != "" && accepted.MediaKind != media {
			continue
		}
		if _, err := s.env.Codecs.EncoderFactory(accepted.Codec); err == nil {
			return encodeSpecFor(accepted.Codec, media), true
		}
	}
	return codec.Spec{}, false
}

// pathMedia is the media kind a stream carries at node id, or "" when the
// recipe does not say.
func pathMedia(req *Request, id recipeir.NodeID) av.MediaType {
	var media av.MediaType
	for _, n := range req.Graph.Path(id) {
		node := req.Graph.Nodes[n]
		switch node.Kind {
		case recipeir.NodeSelect:
			media = cmp.Or(node.Select.Type, req.Inputs[node.Input].IR.Codec.Type, req.Inputs[node.Input].Shape.MediaKind)
		case recipeir.NodeJoin:
			switch JoinKind(node.Join.Kind) {
			case JoinMix:
				media = av.MediaAudio
			case JoinComposite:
				media = av.MediaVideo
			}
		case recipeir.NodeOp:
			switch node.Op.Kind {
			case plan.OpDecode:
				media = cmp.Or(node.Op.Decode.Type, media)
			case plan.OpEncode:
				media = cmp.Or(node.Op.Encode.Type, CodecMedia(node.Op.Encode.ID), media)
			}
		}
	}
	return media
}

// encodeSpecFor is the encoder spec the codec constructors build for id.
func encodeSpecFor(id av.CodecID, media av.MediaType) codec.Spec {
	switch id {
	case av.CodecOpus:
		return codec.Opus()
	case av.CodecVP8:
		return codec.VP8()
	case av.CodecVP9:
		return codec.VP9()
	case av.CodecH264:
		return codec.H264()
	case av.CodecHEVC:
		return codec.HEVC()
	case av.CodecAV1:
		return codec.AV1()
	default:
		return codec.Codec(id, media)
	}
}

// encodeConstructor names the codec constructor a fix suggests for id.
func encodeConstructor(id av.CodecID, media av.MediaType) string {
	switch id {
	case av.CodecOpus:
		return "codec.Opus()"
	case av.CodecVP8:
		return "codec.VP8()"
	case av.CodecVP9:
		return "codec.VP9()"
	case av.CodecH264:
		return "codec.H264()"
	case av.CodecHEVC:
		return "codec.HEVC()"
	case av.CodecAV1:
		return "codec.AV1()"
	default:
		return fmt.Sprintf("codec.Codec(%q, %q)", id, media)
	}
}

func acceptsDomain(accepts shape.Set, domain shape.MediaDomain) bool {
	return slices.ContainsFunc(accepts, func(spec shape.Spec) bool { return spec.Domain == "" || spec.Domain == domain })
}

// acceptedCodecs lists the codecs of media a sink accepts, for messages.
func acceptedCodecs(accepts shape.Set, media av.MediaType) string {
	var codecs []string
	for _, spec := range accepts {
		if spec.Codec != "" && (media == "" || spec.MediaKind == "" || spec.MediaKind == media) && !slices.Contains(codecs, string(spec.Codec)) {
			codecs = append(codecs, string(spec.Codec))
		}
	}
	if len(codecs) == 0 {
		return "no " + string(cmp.Or(media, "such")) + " codec"
	}
	return strings.Join(codecs, ", ")
}

// sinkAcceptsShape reports whether a media sink accepts a stream of the
// given shape, judged on the facts the plan knows: a codec it cannot know
// yet (a probe-less live input) is left to the sink when it opens.
func sinkAcceptsShape(accepts shape.Set, actual shape.Spec) bool {
	if len(accepts) == 0 {
		return true
	}
	return slices.ContainsFunc(accepts, func(spec shape.Spec) bool {
		return (spec.Domain == "" || actual.Domain == "" || spec.Domain == actual.Domain) &&
			(spec.MediaKind == "" || actual.MediaKind == "" || spec.MediaKind == actual.MediaKind) &&
			(spec.Codec == "" || actual.Codec == "" || spec.Codec == actual.Codec)
	})
}

// ValidateSinkShape refuses a stream a media sink cannot take, with the
// encode that fixes it: an explicit one for frames (the planner found no
// encoder to insert), a decode and re-encode for packets of another codec.
func ValidateSinkShape(operation string, node string, destinationName string, destination Destination, actual shape.Spec) error {
	if !destination.Sink || sinkAcceptsShape(destination.Accepts, actual) {
		return nil
	}
	media := actual.MediaKind
	var first av.CodecID
	for _, spec := range destination.Accepts {
		if spec.Codec != "" && (media == "" || spec.MediaKind == "" || spec.MediaKind == media) {
			first = spec.Codec
			break
		}
	}
	accepted := acceptedCodecs(destination.Accepts, media)
	var fixes []string
	switch {
	case first == "":
		fixes = append(fixes, "route only the media the destination takes ("+acceptedCodecs(destination.Accepts, "")+") to it")
	case actual.Domain == shape.DomainFrame:
		fixes = append(fixes,
			"encode to a codec the destination takes: .Encode("+encodeConstructor(first, media)+")",
			"register an encoder for one of "+accepted+" with runconfig.WithEncoder, and goav inserts it")
	default:
		fixes = append(fixes,
			"decode the stream so goav encodes it to "+string(first)+": .Decode() before the destination",
			"or name the encoder: .Decode().Encode("+encodeConstructor(first, media)+")")
	}
	return sinkRefusal(operation, node, destinationName, destination, actual, fixes)
}

// sinkRefusal is the destination_shape_mismatch refusal of a media sink.
func sinkRefusal(operation string, node string, destinationName string, destination Destination, actual shape.Spec, fixes []string) error {
	label := cmp.Or(destinationName, destination.Label, destination.SinkName, "destination")
	accepted := acceptedCodecs(destination.Accepts, actual.MediaKind)
	reason := "the destination takes " + accepted + " packets, not " + string(cmp.Or(actual.Codec, "these")) + " " + string(cmp.Or(actual.Domain, shape.DomainPacket)) + "s"
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.DestinationShapeMismatchCode),
		Code:      builderr.DestinationShapeMismatchCode,
		Operation: operation,
		Node:      cmp.Or(node, label),
		Reason:    reason,
		Details:   builderr.Details(builderr.KV("destination", label), builderr.KV("accepted_codecs", accepted), builderr.KV("actual_shape", actual.String())),
		Fixes:     builderr.Fixes(fixes),
		Cause:     builderr.ErrUnsupportedBuild,
	}
}

// validateRemuxSinkShapes refuses an input copied whole (input.To) into a
// media sink that does not take one of its known streams; the fix routes the
// streams one by one, where goav encodes what the sink needs.
func (s *recipeState) validateRemuxSinkShapes() error {
	for _, remux := range s.remux {
		input := s.req.Inputs[remux.input]
		domain := shape.DomainPacket
		if input.Frames {
			domain = shape.DomainFrame
		}
		var streams []av.Stream
		if remux.input < len(s.inputProbes) {
			streams = s.inputProbes[remux.input].Streams
		}
		for _, dest := range remux.dests {
			destination := s.outputs[dest]
			if !destination.Sink {
				continue
			}
			if err := ValidateSinkRemux(s.operation, input.Name, s.outputDestinationNames[dest], destination, streams, domain); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateSinkRemux refuses streams of an input copied whole into a media
// sink that does not take one of them, with the fix that routes the streams
// one by one: the planner checks the streams a probe knows, the build the
// ones the input opened with.
func ValidateSinkRemux(operation string, node string, destinationName string, destination Destination, streams []av.Stream, domain shape.MediaDomain) error {
	if !destination.Sink {
		return nil
	}
	for _, stream := range streams {
		actual := shape.FromStream(stream, domain)
		if sinkAcceptsShape(destination.Accepts, actual) {
			continue
		}
		media := cmp.Or(actual.MediaKind, CodecMedia(actual.Codec))
		selector := cmp.Or(map[av.MediaType]string{av.MediaAudio: ".Audio()", av.MediaVideo: ".Video()"}[media], ".Video()")
		fix := "route the input's streams one by one, and goav encodes what the destination takes: input" + selector + ".To(destination)"
		if domain == shape.DomainPacket {
			fix = "route the input's streams one by one, decoding the " + string(cmp.Or(actual.Codec, "other")) + " one: input" + selector + ".Decode().To(destination)"
		}
		return sinkRefusal(operation, node, destinationName, destination, actual, []string{fix})
	}
	return nil
}
