package format

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"

	"github.com/thesyncim/goav/av"
)

type Magic struct {
	Offset int
	Bytes  []byte
}

type ProbeRule struct {
	Format     av.FormatID
	Protocol   av.ProtocolID
	Extensions []string
	MIMETypes  []string
	Magic      []Magic
	Score      int
	Reason     string
}

type StaticProber struct {
	rules []ProbeRule
}

func NewStaticProber(rules ...ProbeRule) StaticProber {
	return StaticProber{rules: rules}
}

func DefaultProbeRules() []ProbeRule {
	return []ProbeRule{
		{
			Format: av.FormatOgg,
			Extensions: []string{
				".ogg", ".opus",
			},
			MIMETypes: []string{"audio/ogg", "application/ogg", "audio/opus"},
			Magic:     []Magic{{Bytes: []byte("OggS")}},
			Score:     100,
			Reason:    "ogg magic or extension",
		},
		{
			Format:     av.FormatIVF,
			Extensions: []string{".ivf"},
			Magic:      []Magic{{Bytes: []byte("DKIF")}},
			Score:      100,
			Reason:     "ivf magic or extension",
		},
		{
			Format:     av.FormatFLV,
			Extensions: []string{".flv"},
			MIMETypes:  []string{"video/x-flv"},
			Magic:      []Magic{{Bytes: []byte("FLV")}},
			Score:      100,
			Reason:     "flv magic or extension",
		},
		{
			Format:     av.FormatMatroska,
			Extensions: []string{".webm", ".mkv", ".mka"},
			MIMETypes:  []string{"video/webm", "audio/webm", "video/x-matroska", "audio/x-matroska"},
			Magic:      []Magic{{Bytes: []byte{0x1a, 0x45, 0xdf, 0xa3}}},
			Score:      100,
			Reason:     "ebml magic or matroska/webm extension",
		},
		{
			Format:     av.FormatMP4,
			Extensions: []string{".mp4", ".m4a", ".m4v", ".mov"},
			MIMETypes:  []string{"video/mp4", "audio/mp4", "video/quicktime"},
			Magic:      []Magic{{Offset: 4, Bytes: []byte("ftyp")}},
			Score:      100,
			Reason:     "isobmff ftyp box or extension",
		},
		{
			Format:     av.FormatAnnexB,
			Extensions: []string{".h264", ".264", ".annexb"},
			Magic:      []Magic{{Bytes: []byte{0x00, 0x00, 0x01}}, {Bytes: []byte{0x00, 0x00, 0x00, 0x01}}},
			Score:      90,
			Reason:     "h264 annex b start code or extension",
		},
		{
			Format:   av.FormatRTP,
			Protocol: av.ProtocolRTP,
			Score:    80,
			Reason:   "rtp protocol",
		},
		{
			Format:   av.FormatWebRTC,
			Protocol: av.ProtocolWebRTC,
			Score:    80,
			Reason:   "webrtc protocol",
		},
	}
}

func DefaultProber() StaticProber {
	return NewStaticProber(DefaultProbeRules()...)
}

func (p StaticProber) Probe(ctx context.Context, request ProbeRequest) (ProbeResult, error) {
	if err := ctx.Err(); err != nil {
		return ProbeResult{}, err
	}
	var best ProbeResult
	var found bool
	for i := range p.rules {
		result, ok := probeRule(p.rules[i], request)
		if !ok {
			continue
		}
		if !found || result.Score > best.Score {
			best = result
			found = true
		}
	}
	if !found {
		return ProbeResult{}, ErrNotFound
	}
	return best, nil
}

func probeRule(rule ProbeRule, request ProbeRequest) (ProbeResult, bool) {
	score := 0
	reason := rule.Reason
	if rule.Score > 0 {
		score = rule.Score
	}
	if score == 0 {
		score = 1
	}

	if rule.Protocol != "" && request.Input.Protocol == rule.Protocol {
		return ProbeResult{Format: rule.Format, Score: score, Reason: reason}, true
	}
	if matchMIME(rule.MIMETypes, request.MIMEType) || matchMIME(rule.MIMETypes, request.Input.MIMEType) {
		return ProbeResult{Format: rule.Format, Score: score - 10, Reason: reason}, true
	}
	if matchMagic(rule.Magic, request.Header) {
		return ProbeResult{Format: rule.Format, Score: score, Reason: reason}, true
	}
	if matchExtension(rule.Extensions, request.Name) || matchExtension(rule.Extensions, request.Input.Name) || matchExtension(rule.Extensions, request.Input.URI) {
		return ProbeResult{Format: rule.Format, Score: score - 20, Reason: reason}, true
	}
	return ProbeResult{}, false
}

func matchMIME(candidates []string, mimeType string) bool {
	for i := range candidates {
		if strings.EqualFold(candidates[i], mimeType) {
			return true
		}
	}
	return false
}

func matchMagic(candidates []Magic, header []byte) bool {
	for i := range candidates {
		magic := candidates[i]
		if magic.Offset < 0 || magic.Offset+len(magic.Bytes) > len(header) {
			continue
		}
		if bytes.Equal(header[magic.Offset:magic.Offset+len(magic.Bytes)], magic.Bytes) {
			return true
		}
	}
	return false
}

func matchExtension(candidates []string, name string) bool {
	if name == "" {
		return false
	}
	ext := strings.ToLower(filepath.Ext(name))
	for i := range candidates {
		if strings.EqualFold(candidates[i], ext) {
			return true
		}
	}
	return false
}
