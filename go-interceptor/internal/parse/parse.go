// Package parse is optional key=value field extraction for the Kafka envelope.
//
// Runs on a tee shard worker, never on the forwarding path, so nothing here can
// affect VP<->FMS latency. A parse failure is reported in the envelope and never
// suppresses the publish -- a message you cannot parse is still evidence.
package parse

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/vynamic/vp-fms-interceptor/internal/config"
)

// KvParser splits a key=value delimited frame into named fields.
//
// Delimiters are resolved to single bytes once, at construction.
type KvParser struct {
	pairDelim byte
	kvDelim   byte
	trim      bool
	maxFields int
}

// New returns nil when parsing is disabled, which is how every caller is
// reminded that "no parser" is a normal state rather than an error.
func New(cfg config.Parse) *KvParser {
	if cfg.Mode != "key_value" {
		return nil
	}
	return &KvParser{
		pairDelim: firstByte(cfg.PairDelimiter, ','),
		kvDelim:   firstByte(cfg.KvDelimiter, '='),
		trim:      cfg.Trim,
		maxFields: cfg.MaxFields,
	}
}

// Parse returns the fields, or an error explaining what was malformed. The
// caller publishes either way.
func (p *KvParser) Parse(frame []byte) (map[string]string, error) {
	// GO: Go's []byte -> string conversion SILENTLY KEEPS invalid UTF-8, and
	// json.Marshal would later replace it with U+FFFD without telling anyone.
	// In a payments context, silently corrupting a byte you could not decode is
	// much worse than being told the frame was not text -- so check first and
	// fail loudly, matching Rust's str::from_utf8.
	if !utf8.Valid(frame) {
		return nil, fmt.Errorf("not valid utf-8")
	}
	text := string(frame)

	out := make(map[string]string)
	for i, pair := range strings.Split(text, string(p.pairDelim)) {
		if p.trim {
			pair = strings.TrimSpace(pair)
		}
		if pair == "" {
			continue // tolerate trailing or doubled delimiters
		}
		if i >= p.maxFields {
			return nil, fmt.Errorf("more than %d fields", p.maxFields)
		}
		// GO: strings.Cut splits at the FIRST occurrence only, which is the
		// whole point: with a plain Split, a base64 value like "YWJjZA==" is
		// mangled into ["YWJjZA", "", ""]. Cut yields the value intact.
		k, v, found := strings.Cut(pair, string(p.kvDelim))
		if !found {
			return nil, fmt.Errorf("segment %d has no '%c' separator", i, p.kvDelim)
		}
		if p.trim {
			k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		}
		if k == "" {
			return nil, fmt.Errorf("empty key in segment %d", i)
		}
		out[k] = v
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("no fields found")
	}
	return out, nil
}

func firstByte(s string, fallback byte) byte {
	if s == "" {
		return fallback
	}
	return s[0]
}
