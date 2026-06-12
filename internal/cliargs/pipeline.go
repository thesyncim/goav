package cliargs

import "strings"

// PipelineSyntaxError reports the byte offset of a cold-path CLI pipeline
// syntax failure.
type PipelineSyntaxError struct {
	Offset  int
	Message string
}

func (e *PipelineSyntaxError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

// SplitPipeline splits a quote-aware pipeline string on ! separators. Quotes
// and escapes are preserved for PipelineFields to process inside each step.
func SplitPipeline(text string) ([]string, error) {
	var steps []string
	start := 0
	var quote byte
	escaped := false
	for i := 0; i < len(text); i++ {
		ch := text[i]
		if escaped {
			escaped = false
			continue
		}
		if quote != 0 {
			switch ch {
			case '\\':
				escaped = true
			case quote:
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"':
			quote = ch
		case '!':
			step := strings.TrimSpace(text[start:i])
			if step == "" {
				return nil, &PipelineSyntaxError{Offset: i, Message: "empty pipeline step"}
			}
			steps = append(steps, step)
			start = i + 1
		}
	}
	if escaped {
		return nil, &PipelineSyntaxError{Offset: len(text), Message: "unterminated escape sequence in pipeline"}
	}
	if quote != 0 {
		return nil, &PipelineSyntaxError{Offset: len(text), Message: "unterminated quoted value in pipeline"}
	}
	step := strings.TrimSpace(text[start:])
	if step == "" {
		return nil, &PipelineSyntaxError{Offset: len(text), Message: "empty pipeline step"}
	}
	steps = append(steps, step)
	return steps, nil
}

// PipelineFields tokenizes one pipeline step on unquoted whitespace. It removes
// quote characters and backslash escaping, preserving empty quoted values.
func PipelineFields(step string) ([]string, error) {
	var fields []string
	var field strings.Builder
	var quote byte
	escaped := false
	started := false
	for i := 0; i < len(step); i++ {
		ch := step[i]
		if escaped {
			field.WriteByte(ch)
			escaped = false
			started = true
			continue
		}
		if quote != 0 {
			switch ch {
			case '\\':
				escaped = true
			case quote:
				quote = 0
			default:
				field.WriteByte(ch)
			}
			started = true
			continue
		}
		switch {
		case ch == '\'' || ch == '"':
			quote = ch
			started = true
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r':
			if started {
				fields = append(fields, field.String())
				field.Reset()
				started = false
			}
		default:
			field.WriteByte(ch)
			started = true
		}
	}
	if escaped {
		return nil, &PipelineSyntaxError{Offset: len(step), Message: "unterminated escape sequence in pipeline step"}
	}
	if quote != 0 {
		return nil, &PipelineSyntaxError{Offset: len(step), Message: "unterminated quoted value in pipeline step"}
	}
	if started {
		fields = append(fields, field.String())
	}
	return fields, nil
}
