package goav

import (
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/compile"
)

func inputNodeDetail(input format.Input) string {
	parts := []string{"demux"}
	if input.URI != "" && input.URI != input.Name {
		parts = append(parts, "uri="+input.URI)
	}
	if input.Protocol != "" {
		parts = append(parts, "protocol="+string(input.Protocol))
	}
	if input.MIMEType != "" {
		parts = append(parts, "mime="+input.MIMEType)
	}
	if input.Realtime {
		parts = append(parts, "realtime")
	}
	return compile.JoinSpecDetail(parts...)
}

func customSourceDetail(input InputSpec) string {
	parts := []string{"custom source"}
	if input.source != nil {
		shape := compile.NormalizeCustomSourceShape(input.inputName("source"), input.source.shape)
		if shape.Domain != "" {
			parts = append(parts, "domain="+string(shape.Domain))
		}
		if shape.MediaKind != "" {
			parts = append(parts, "media="+string(shape.MediaKind))
		}
		if shape.Codec != "" {
			parts = append(parts, "codec="+string(shape.Codec))
		}
		if shape.Realtime {
			parts = append(parts, "realtime")
		}
	}
	return compile.JoinSpecDetail(parts...)
}
