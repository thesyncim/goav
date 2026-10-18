package launch

import (
	"strings"
)

type tokenKind uint8

const (
	tokEOF     tokenKind = iota
	tokWord              // a bare word: element name, value, path
	tokString            // a quoted value, unquoted
	tokPipe              // !
	tokComma             // ,
	tokLParen            // (
	tokRParen            // )
	tokEquals            // =
	tokAssign            // :=
	tokNewline           // a statement break: newline or ;
)

func (k tokenKind) String() string {
	switch k {
	case tokEOF:
		return "end of program"
	case tokWord:
		return "a word"
	case tokString:
		return "a quoted value"
	case tokPipe:
		return `"!"`
	case tokComma:
		return `","`
	case tokLParen:
		return `"("`
	case tokRParen:
		return `")"`
	case tokEquals:
		return `"="`
	case tokAssign:
		return `":="`
	default:
		return "a statement break"
	}
}

type token struct {
	kind tokenKind
	text string
	pos  int // byte offset of the token's first byte
	end  int // byte offset just past it
}

// special bytes end a bare word.
func special(b byte) bool {
	switch b {
	case '!', ',', '(', ')', '=', ';', '#', '\'', '"', ' ', '\t', '\r', '\n':
		return true
	}
	return false
}

// lex splits a program into tokens. A quoted value keeps its spaces and
// specials; a backslash escapes the next byte inside quotes.
func lex(src string) ([]token, error) {
	var toks []token
	for i := 0; i < len(src); {
		b := src[i]
		switch {
		case b == ' ' || b == '\t' || b == '\r':
			i++
		case b == '#':
			for i < len(src) && src[i] != '\n' {
				i++
			}
		case b == '\n' || b == ';':
			toks = append(toks, token{kind: tokNewline, text: string(b), pos: i, end: i + 1})
			i++
		case b == '!':
			toks = append(toks, token{kind: tokPipe, text: "!", pos: i, end: i + 1})
			i++
		case b == ',':
			toks = append(toks, token{kind: tokComma, text: ",", pos: i, end: i + 1})
			i++
		case b == '(':
			toks = append(toks, token{kind: tokLParen, text: "(", pos: i, end: i + 1})
			i++
		case b == ')':
			toks = append(toks, token{kind: tokRParen, text: ")", pos: i, end: i + 1})
			i++
		case b == '=':
			toks = append(toks, token{kind: tokEquals, text: "=", pos: i, end: i + 1})
			i++
		case b == '\'' || b == '"':
			start := i
			var value strings.Builder
			i++
			closed := false
			for i < len(src) {
				c := src[i]
				if c == '\\' && i+1 < len(src) {
					value.WriteByte(src[i+1])
					i += 2
					continue
				}
				if c == b {
					closed = true
					i++
					break
				}
				value.WriteByte(c)
				i++
			}
			if !closed {
				return nil, &Error{Pos: start, End: len(src), Msg: "unterminated quoted value", Hint: "close it with " + string(b)}
			}
			toks = append(toks, token{kind: tokString, text: value.String(), pos: start, end: i})
		default:
			start := i
			for i < len(src) && !special(src[i]) {
				// ":=" binds even when written without spaces.
				if src[i] == ':' && i+1 < len(src) && src[i+1] == '=' {
					break
				}
				i++
			}
			if i > start {
				toks = append(toks, token{kind: tokWord, text: src[start:i], pos: start, end: i})
			}
			if i+1 < len(src) && src[i] == ':' && src[i+1] == '=' {
				toks = append(toks, token{kind: tokAssign, text: ":=", pos: i, end: i + 2})
				i += 2
			} else if i == start {
				// A lone ':' or other byte that starts no token is a word of its own.
				toks = append(toks, token{kind: tokWord, text: src[i : i+1], pos: i, end: i + 1})
				i++
			}
		}
	}
	toks = append(toks, token{kind: tokEOF, pos: len(src), end: len(src)})
	return toks, nil
}
