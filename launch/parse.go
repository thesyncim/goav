package launch

// The parse tree. A program is statements; a statement optionally binds a
// name; a pipe is segments joined by "!"; a segment is one or more terms
// (several only as a final list of destinations); a term is an element with
// arguments, a reference to a binding, or a call such as mix(a, b).

type stmtNode struct {
	name    string // binding name, or "" for an Output statement
	namePos int
	nameEnd int
	pipe    *pipeNode
}

type pipeNode struct {
	segs [][]*termNode
	pos  int
	end  int
}

type termNode struct {
	name string
	pos  int
	end  int // end of the whole term, arguments included
	args []argNode
	// call marks name(...): args are pipes.
	call     bool
	callArgs []*pipeNode
}

type argNode struct {
	key    string // "" for a positional argument
	keyPos int
	value  string
	pos    int
	end    int
}

type parser struct {
	toks  []token
	i     int
	depth int // parenthesis nesting: newlines inside are insignificant
}

func parse(src string) ([]*stmtNode, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	var stmts []*stmtNode
	for {
		p.skipNewlines()
		if p.peek().kind == tokEOF {
			return stmts, nil
		}
		stmt, err := p.stmt()
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, stmt)
		switch t := p.peek(); t.kind {
		case tokNewline, tokEOF:
		default:
			return nil, errorAt(t, "unexpected "+describeToken(t), "end the statement with a newline or ;, or join steps with !")
		}
	}
}

func (p *parser) peek() token { return p.toks[p.i] }

func (p *parser) peekAt(n int) token {
	if p.i+n < len(p.toks) {
		return p.toks[p.i+n]
	}
	return p.toks[len(p.toks)-1]
}

func (p *parser) next() token {
	t := p.toks[p.i]
	if t.kind != tokEOF {
		p.i++
	}
	return t
}

func (p *parser) skipNewlines() {
	for p.peek().kind == tokNewline {
		p.i++
	}
}

// continueLine skips line breaks that cannot end a statement here: inside
// parentheses, or before a line that continues with "!" or ",".
func (p *parser) continueLine() {
	if p.depth > 0 {
		p.skipNewlines()
		return
	}
	j := p.i
	for p.toks[j].kind == tokNewline {
		j++
	}
	if k := p.toks[j].kind; k == tokPipe || k == tokComma {
		p.i = j
	}
}

func (p *parser) stmt() (*stmtNode, error) {
	stmt := &stmtNode{}
	if t := p.peek(); t.kind == tokWord && p.peekAt(1).kind == tokAssign {
		stmt.name, stmt.namePos, stmt.nameEnd = t.text, t.pos, t.end
		p.i += 2
		p.skipNewlines()
	}
	pipe, err := p.pipe()
	if err != nil {
		return nil, err
	}
	stmt.pipe = pipe
	return stmt, nil
}

func (p *parser) pipe() (*pipeNode, error) {
	pipe := &pipeNode{pos: p.peek().pos}
	for {
		seg, err := p.segment()
		if err != nil {
			return nil, err
		}
		pipe.segs = append(pipe.segs, seg)
		pipe.end = seg[len(seg)-1].end
		p.continueLine()
		if p.peek().kind != tokPipe {
			return pipe, nil
		}
		p.next()
		p.skipNewlines()
	}
}

// segment is one term, or — outside parentheses — a comma-separated list.
func (p *parser) segment() ([]*termNode, error) {
	var seg []*termNode
	for {
		term, err := p.term()
		if err != nil {
			return nil, err
		}
		seg = append(seg, term)
		p.continueLine()
		if p.depth > 0 || p.peek().kind != tokComma {
			return seg, nil
		}
		p.next()
		p.skipNewlines()
	}
}

func (p *parser) term() (*termNode, error) {
	t := p.peek()
	var term *termNode
	switch t.kind {
	case tokWord:
		p.next()
		term = &termNode{name: t.text, pos: t.pos, end: t.end}
		if p.peek().kind == tokLParen && p.peek().pos == t.end {
			return p.call(term)
		}
	case tokString:
		// A bare quoted URL is a uri step: "https://cdn/in.mp4" ! video.
		p.next()
		term = &termNode{name: "uri", pos: t.pos, end: t.end, args: []argNode{{value: t.text, pos: t.pos, end: t.end}}}
	default:
		return nil, errorAt(t, "expected an element, found "+describeToken(t), "start a step with an element name such as file, video, or encode")
	}
	for {
		a := p.peek()
		switch a.kind {
		case tokWord, tokString:
			p.next()
			arg := argNode{value: a.text, pos: a.pos, end: a.end}
			if a.kind == tokWord && p.peek().kind == tokEquals {
				eq := p.next()
				v := p.peek()
				if v.kind != tokWord && v.kind != tokString {
					return nil, errorAt(eq, "expected a value after "+a.text+"=", "write "+a.text+"=<value>; quote values with spaces")
				}
				p.next()
				arg = argNode{key: a.text, keyPos: a.pos, value: v.text, pos: v.pos, end: v.end}
			}
			term.args = append(term.args, arg)
			term.end = arg.end
		case tokEquals:
			return nil, errorAt(a, "expected a setting name before =", "write key=value with no spaces around =")
		default:
			return term, nil
		}
	}
}

func (p *parser) call(term *termNode) (*termNode, error) {
	open := p.next()
	p.depth++
	defer func() { p.depth-- }()
	term.call = true
	p.skipNewlines()
	if p.peek().kind == tokRParen {
		term.end = p.next().end
		return term, nil
	}
	for {
		// key=value inside a call is a setting of the call itself.
		if k := p.peek(); k.kind == tokWord && p.peekAt(1).kind == tokEquals {
			p.next()
			eq := p.next()
			v := p.peek()
			if v.kind != tokWord && v.kind != tokString {
				return nil, errorAt(eq, "expected a value after "+k.text+"=", "write "+k.text+"=<value>")
			}
			p.next()
			term.args = append(term.args, argNode{key: k.text, keyPos: k.pos, value: v.text, pos: v.pos, end: v.end})
		} else {
			arg, err := p.pipe()
			if err != nil {
				return nil, err
			}
			term.callArgs = append(term.callArgs, arg)
		}
		p.skipNewlines()
		switch t := p.peek(); t.kind {
		case tokComma:
			p.next()
			p.skipNewlines()
		case tokRParen:
			term.end = p.next().end
			return term, nil
		default:
			return nil, &Error{Pos: open.pos, End: t.end, Msg: term.name + "( is not closed", Hint: "separate inputs with , and close with )"}
		}
	}
}

func describeToken(t token) string {
	if t.kind == tokWord || t.kind == tokString {
		return "\"" + t.text + "\""
	}
	return t.kind.String()
}
