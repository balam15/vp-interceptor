package kafka

import (
	"strconv"

	"github.com/vynamic/vp-fms-interceptor/internal/config"
)

func (p *Publisher) shouldDrop(frame []byte) bool {
	if len(p.filter.AnyOf) == 0 {
		return false
	}
	mti, de70, ok := extractMTIAndDE70(frame)
	if !ok {
		return false
	}
	return matchKafkaFilter(p.filter, mti, de70)
}

func matchKafkaFilter(filter config.KafkaFilter, mti, de70 string) bool {
	for _, anyOf := range filter.AnyOf {
		if matchAllOf(anyOf.AllOf, mti, de70) {
			return true
		}
	}
	return false
}

func matchAllOf(allOf []config.KafkaFilterAllOf, mti, de70 string) bool {
	for _, clause := range allOf {
		if !matchClause(clause, mti, de70) {
			return false
		}
	}
	return true
}

func matchClause(clause config.KafkaFilterAllOf, mti, de70 string) bool {
	if len(clause.MTI) > 0 && !contains(clause.MTI, mti) {
		return false
	}
	if len(clause.DE70) > 0 && !contains(clause.DE70, de70) {
		return false
	}
	return true
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func extractMTIAndDE70(frame []byte) (mti string, de70 string, ok bool) {
	fields, pos, ok := bitmapFields(frame)
	if !ok {
		return "", "", false
	}
	if pos+4 > len(frame) {
		return "", "", false
	}
	mti = string(frame[:4])
	if !allDigitsBytes(frame[:4]) {
		return "", "", false
	}

	for field := 2; field <= 70; field++ {
		if !fields[field] {
			continue
		}
		switch field {
		case 70:
			if pos+3 > len(frame) {
				return "", "", false
			}
			if !allDigitsBytes(frame[pos : pos+3]) {
				return "", "", false
			}
			de70 = string(frame[pos : pos+3])
			return mti, de70, true
		default:
			next, ok := skipISOField(field, frame, pos)
			if !ok {
				return "", "", false
			}
			pos = next
		}
	}

	return mti, de70, true
}

func bitmapFields(frame []byte) ([129]bool, int, bool) {
	var fields [129]bool
	if len(frame) < 20 {
		return fields, 0, false
	}

	primary, ok := parseBitmapChunk(frame[4:20])
	if !ok {
		return fields, 0, false
	}
	for i := 0; i < 64; i++ {
		if primary&(uint64(1)<<(63-uint(i))) != 0 {
			fields[i+1] = true
		}
	}

	pos := 20
	if fields[1] {
		if len(frame) < 36 {
			return fields, 0, false
		}
		secondary, ok := parseBitmapChunk(frame[20:36])
		if !ok {
			return fields, 0, false
		}
		for i := 0; i < 64; i++ {
			if secondary&(uint64(1)<<(63-uint(i))) != 0 {
				fields[i+65] = true
			}
		}
		pos = 36
	}

	return fields, pos, true
}

func parseBitmapChunk(chunk []byte) (uint64, bool) {
	n, err := strconv.ParseUint(string(chunk), 16, 64)
	return n, err == nil
}

func skipISOField(field int, frame []byte, pos int) (int, bool) {
	switch field {
	case 2:
		return skipLLField(frame, pos, 2, 19)
	case 3:
		return skipFixed(frame, pos, 6)
	case 4, 6:
		return skipFixed(frame, pos, 12)
	case 7:
		return skipFixed(frame, pos, 10)
	case 11:
		return skipFixed(frame, pos, 6)
	case 12:
		return skipFixed(frame, pos, 6)
	case 13, 15:
		return skipFixed(frame, pos, 4)
	case 18:
		return skipFixed(frame, pos, 4)
	case 22:
		return skipFixed(frame, pos, 3)
	case 28:
		return skipFixed(frame, pos, 9)
	case 32:
		return skipLLField(frame, pos, 2, 11)
	case 35:
		return skipLLField(frame, pos, 2, 37)
	case 37:
		return skipFixed(frame, pos, 12)
	case 38:
		return skipFixed(frame, pos, 6)
	case 39:
		return skipFixed(frame, pos, 2)
	case 41:
		return skipFixed(frame, pos, 16)
	case 42:
		return skipFixed(frame, pos, 15)
	case 43:
		return skipFixed(frame, pos, 41)
	case 48:
		return skipLLLField(frame, pos, 999)
	case 49, 50, 51:
		return skipFixed(frame, pos, 3)
	case 61:
		return skipLLLField(frame, pos, 999)
	case 63:
		return skipLLLField(frame, pos, 8)
	case 90:
		return skipFixed(frame, pos, 42)
	case 100:
		return skipLLField(frame, pos, 2, 11)
	case 102, 103:
		return skipLLField(frame, pos, 2, 28)
	case 123, 125, 127:
		return skipLLLField(frame, pos, 255)
	default:
		return 0, false
	}
}

func skipFixed(frame []byte, pos, n int) (int, bool) {
	if pos+n > len(frame) {
		return 0, false
	}
	return pos + n, true
}

func skipLLField(frame []byte, pos, digits, max int) (int, bool) {
	if pos+digits > len(frame) {
		return 0, false
	}
	n, ok := parseLength(frame[pos:pos+digits], max)
	if !ok {
		return 0, false
	}
	start := pos + digits
	if start+n > len(frame) {
		return 0, false
	}
	return start + n, true
}

func skipLLLField(frame []byte, pos, max int) (int, bool) {
	return skipLLField(frame, pos, 3, max)
}

func parseLength(chunk []byte, max int) (int, bool) {
	if !allDigitsBytes(chunk) {
		return 0, false
	}
	n, err := strconv.Atoi(string(chunk))
	if err != nil || n > max {
		return 0, false
	}
	return n, true
}

func allDigitsBytes(b []byte) bool {
	for _, c := range b {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
