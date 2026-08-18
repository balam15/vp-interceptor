package kafka

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/vynamic/vp-fms-interceptor/internal/config"
	"github.com/vynamic/vp-fms-interceptor/internal/parse"
)

func TestHexEncodesLowercaseTwoCharsPerByte(t *testing.T) {
	if got := EncodePayload("hex", []byte{0x00, 0x0f, 0xa5, 0xff}); got != "000fa5ff" {
		t.Fatalf("got %q", got)
	}
}

func TestBase64Roundtrips(t *testing.T) {
	raw := []byte("0200|STAN=00000001")
	dec, err := base64.StdEncoding.DecodeString(EncodePayload("base64", raw))
	if err != nil {
		t.Fatal(err)
	}
	if string(dec) != string(raw) {
		t.Fatalf("got %q", dec)
	}
}

// A real ISO 8583 bitmap is not valid UTF-8; base64 must not lose bytes the way
// the utf8 encoding would.
func TestBase64HandlesNonUtf8Binary(t *testing.T) {
	raw := []byte{0xff, 0xfe, 0x00, 0x80, 0x7f}
	dec, err := base64.StdEncoding.DecodeString(EncodePayload("base64", raw))
	if err != nil {
		t.Fatal(err)
	}
	if string(dec) != string(raw) {
		t.Fatalf("got % x", dec)
	}
}

// THE CROSS-BUILD COMPATIBILITY TEST. This exact string is asserted by
// rust-interceptor/src/kafka.rs, so a consumer cannot tell the two apart.
func TestEnvelopeSerialisesWithExpectedKeysAndOrder(t *testing.T) {
	env := envelope{
		ConnID:    7,
		Direction: "vp_to_fms",
		Seq:       3,
		Peer:      "127.0.0.1:5000",
		TsMs:      1700000000000,
		Length:    2,
		Encoding:  "base64",
		Payload:   EncodePayload("base64", []byte("hi")),
	}
	got, err := marshalEnvelope(&env)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"conn_id":7,"direction":"vp_to_fms","seq":3,"peer":"127.0.0.1:5000","ts_ms":1700000000000,"length":2,"encoding":"base64","payload":"aGk="}`
	if string(got) != want {
		t.Fatalf("\ngot  %s\nwant %s", got, want)
	}
}

func TestEnvelopeIncludesParsedFields(t *testing.T) {
	raw := []byte("accountName=Zacky,accountNumber=11020134353,bankCode=1234")
	fields, err := parse.New(config.Defaults().Parse).Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	body, err := marshalEnvelope(&envelope{
		ConnID: 1, Direction: "vp_to_fms", Seq: 1, Peer: "127.0.0.1:5000",
		TsMs: 1700000000000, Length: len(raw), Fields: fields,
		Encoding: "utf8", Payload: EncodePayload("utf8", raw),
	})
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatal(err)
	}
	f := v["fields"].(map[string]any)
	if f["accountName"] != "Zacky" || f["accountNumber"] != "11020134353" || f["bankCode"] != "1234" {
		t.Fatalf("fields = %v", f)
	}
	if _, present := v["parse_error"]; present {
		t.Fatal("parse_error must be omitted on success")
	}
}

// The point: the bytes survive even when parsing does not.
func TestUnparseableFrameStillCarriesItsPayload(t *testing.T) {
	raw := []byte{0xff, 0xfe, ' ', 'n', 'o', 't', ' ', 'o', 'k'}
	body, err := marshalEnvelope(&envelope{
		ConnID: 1, Direction: "vp_to_fms", Seq: 1, Peer: "p", TsMs: 0,
		Length: len(raw), ParseError: "not valid utf-8",
		Encoding: "base64", Payload: EncodePayload("base64", raw),
	})
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatal(err)
	}
	if _, present := v["fields"]; present {
		t.Fatal("fields must be omitted when parsing failed")
	}
	if v["parse_error"] != "not valid utf-8" {
		t.Fatalf("parse_error = %v", v["parse_error"])
	}
	back, err := base64.StdEncoding.DecodeString(v["payload"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if string(back) != string(raw) {
		t.Fatalf("payload did not round-trip: % x", back)
	}
}

// Guards against anyone "optimising" the envelope into string concatenation:
// this data is attacker-influenced, unlike the admin endpoint's counters.
func TestJsonEscapingSurvivesQuotesAndBackslashes(t *testing.T) {
	raw := []byte(`a"b\c`)
	body, err := marshalEnvelope(&envelope{
		ConnID: 1, Direction: "vp_to_fms", Seq: 1, Peer: "p", TsMs: 0,
		Length: len(raw), Encoding: "utf8", Payload: EncodePayload("utf8", raw),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"payload":"a\"b\\c"`) {
		t.Fatalf("got %s", body)
	}
	var v map[string]any
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatal(err)
	}
	if v["payload"] != `a"b\c` {
		t.Fatalf("payload = %v", v["payload"])
	}
}

// encoding/json escapes <, > and & by default; serde does not. Leaving that on
// would make this build's messages differ from the Rust one's.
func TestHtmlCharactersAreNotEscaped(t *testing.T) {
	raw := []byte(`<a&b>`)
	body, err := marshalEnvelope(&envelope{
		ConnID: 1, Direction: "vp_to_fms", Seq: 1, Peer: "p", TsMs: 0,
		Length: len(raw), Encoding: "utf8", Payload: EncodePayload("utf8", raw),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"payload":"<a&b>"`) {
		t.Fatalf("got %s", body)
	}
}

func TestTimingFieldsAreOmittedWhenAbsentAndPresentWhenZero(t *testing.T) {
	zero := 0.0
	body, err := marshalEnvelope(&envelope{
		ConnID: 1, Direction: "fms_to_vp", Seq: 1, Peer: "p", TsMs: 0, Length: 1,
		ConnAgeMs: &zero, Encoding: "base64", Payload: "AA==",
	})
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatal(err)
	}
	// A measured zero must be published, not dropped -- which is why these are
	// pointers rather than plain float64 with omitempty.
	if got, ok := v["conn_age_ms"]; !ok || got.(float64) != 0 {
		t.Fatalf("conn_age_ms = %v (present=%v)", got, ok)
	}
	if _, present := v["gap_ms"]; present {
		t.Fatal("gap_ms must be omitted when not measured")
	}
	if _, present := v["rtt_ms"]; present {
		t.Fatal("rtt_ms must be omitted on a frame with no pairing")
	}
}

func TestRequiredAcksMapping(t *testing.T) {
	for in, want := range map[string]int{"0": 0, "1": 1, "all": -1, "-1": -1, "": 1} {
		if got := int(requiredAcks(in)); got != want {
			t.Errorf("acks %q -> %d, want %d", in, got, want)
		}
	}
}

func TestSplitBrokers(t *testing.T) {
	got := splitBrokers("a:9092, b:9092 ,,c:9092")
	if len(got) != 3 || got[0] != "a:9092" || got[1] != "b:9092" || got[2] != "c:9092" {
		t.Fatalf("got %q", got)
	}
}
