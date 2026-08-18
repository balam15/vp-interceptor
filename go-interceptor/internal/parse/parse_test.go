package parse

import (
	"testing"

	"github.com/vynamic/vp-fms-interceptor/internal/config"
)

func cfg() config.Parse {
	return config.Parse{
		Mode:          "key_value",
		PairDelimiter: ",",
		KvDelimiter:   "=",
		Trim:          true,
		MaxFields:     64,
	}
}

func TestParsesTheDocumentedShape(t *testing.T) {
	p := New(cfg())
	got, err := p.Parse([]byte("accountName=Zacky,accountNumber=11020134353,bankCode=1234"))
	if err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]string{
		"accountName":   "Zacky",
		"accountNumber": "11020134353",
		"bankCode":      "1234",
	} {
		if got[k] != want {
			t.Errorf("%s = %q, want %q", k, got[k], want)
		}
	}
}

func TestTrimsWhitespaceAroundPairsAndValues(t *testing.T) {
	p := New(cfg())
	got, err := p.Parse([]byte("accountName = Zacky , bankCode = 1234 "))
	if err != nil {
		t.Fatal(err)
	}
	if got["accountName"] != "Zacky" || got["bankCode"] != "1234" {
		t.Fatalf("got %v", got)
	}
}

func TestToleratesTrailingAndDoubledDelimiters(t *testing.T) {
	p := New(cfg())
	got, err := p.Parse([]byte("a=1,,b=2,"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d fields, want 2: %v", len(got), got)
	}
}

// strings.Cut, not Split: a base64 value ending in '=' must survive.
func TestKeepsEqualsSignsInsideTheValue(t *testing.T) {
	p := New(cfg())
	got, err := p.Parse([]byte("token=YWJjZA==,bankCode=1234"))
	if err != nil {
		t.Fatal(err)
	}
	if got["token"] != "YWJjZA==" {
		t.Fatalf("token = %q, want YWJjZA==", got["token"])
	}
}

func TestReportsMissingSeparatorRatherThanGuessing(t *testing.T) {
	p := New(cfg())
	if _, err := p.Parse([]byte("accountName=Zacky,garbage")); err == nil {
		t.Fatal("expected an error")
	}
}

// THE TEST NAME IS THE CONTRACT. A plain []byte -> string conversion would
// silently keep the invalid bytes and let json.Marshal replace them later.
func TestReportsNonUtf8RatherThanCorrupting(t *testing.T) {
	p := New(cfg())
	if _, err := p.Parse([]byte{0xff, 0xfe, 0x00}); err == nil {
		t.Fatal("expected an error")
	}
}

func TestEmptyKeyIsRejected(t *testing.T) {
	p := New(cfg())
	if _, err := p.Parse([]byte("=value")); err == nil {
		t.Fatal("expected an error")
	}
}

func TestMaxFieldsIsEnforced(t *testing.T) {
	c := cfg()
	c.MaxFields = 2
	p := New(c)
	if _, err := p.Parse([]byte("a=1,b=2,c=3")); err == nil {
		t.Fatal("expected an error once past max_fields")
	}
}

func TestDisabledWhenModeIsNone(t *testing.T) {
	c := cfg()
	c.Mode = "none"
	if New(c) != nil {
		t.Fatal("expected nil parser when parse.mode is none")
	}
}
