package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const minimal = `
[listen]
addr = "0.0.0.0:9100"
[upstream]
addr = "127.0.0.1:8583"
[kafka]
brokers = "localhost:9092"
topic = "vp.fms.iso8583"
`

// Absent sections must come back fully populated, so nothing downstream ever
// nil-checks a config value.
func TestAbsentSectionsGetDefaults(t *testing.T) {
	cfg, err := Load(writeTemp(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Proxy.ReadBufferBytes != 16384 || !cfg.Proxy.Nodelay {
		t.Errorf("proxy defaults wrong: %+v", cfg.Proxy)
	}
	if cfg.Tee.Shards != 4 || cfg.Tee.QueueCapacity != 8192 || cfg.Tee.PublishDirections != "both" {
		t.Errorf("tee defaults wrong: %+v", cfg.Tee)
	}
	if cfg.Framing.PrefixBytes != 2 || !cfg.Framing.BigEndian || cfg.Framing.MaxFrameBytes != 65536 {
		t.Errorf("framing defaults wrong: %+v", cfg.Framing)
	}
	if !cfg.Timing.Enabled || cfg.Timing.MaxPending != 256 {
		t.Errorf("timing defaults wrong: %+v", cfg.Timing)
	}
	if cfg.Admin.Addr != "127.0.0.1:9101" {
		t.Errorf("admin default wrong: %+v", cfg.Admin)
	}
}

// A present key must win over the default, including when it sets a bool false
// -- the case a naive "if zero, use default" implementation gets wrong.
func TestPresentKeysOverrideDefaults(t *testing.T) {
	cfg, err := Load(writeTemp(t, minimal+`
[proxy]
nodelay = false
[timing]
enabled = false
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Proxy.Nodelay {
		t.Error("proxy.nodelay = false must survive")
	}
	if cfg.Timing.Enabled {
		t.Error("timing.enabled = false must survive")
	}
}

func TestMissingRequiredKeysAreRejected(t *testing.T) {
	for _, body := range []string{
		"[upstream]\naddr = \"x:1\"\n[kafka]\nbrokers=\"b\"\ntopic=\"t\"\n",
		"[listen]\naddr = \"x:1\"\n[kafka]\nbrokers=\"b\"\ntopic=\"t\"\n",
		"[listen]\naddr=\"x:1\"\n[upstream]\naddr=\"y:1\"\n[kafka]\ntopic=\"t\"\n",
	} {
		if _, err := Load(writeTemp(t, body)); err == nil {
			t.Errorf("expected an error for:\n%s", body)
		}
	}
}

func TestValidateRejectsBadValues(t *testing.T) {
	cases := map[string]func(*Config){
		"tee.shards":              func(c *Config) { c.Tee.Shards = 0 },
		"tee.queue_capacity":      func(c *Config) { c.Tee.QueueCapacity = 0 },
		"proxy.read_buffer_bytes": func(c *Config) { c.Proxy.ReadBufferBytes = 100 },
		"framing.prefix_bytes":    func(c *Config) { c.Framing.PrefixBytes = 3 },
		"framing.mode":            func(c *Config) { c.Framing.Mode = "sideways" },
		"tee.publish_directions":  func(c *Config) { c.Tee.PublishDirections = "neither" },
		"parse.mode":              func(c *Config) { c.Parse.Mode = "xml" },
		"parse.delimiters":        func(c *Config) { c.Parse.KvDelimiter = "" },
		"parse.max_fields":        func(c *Config) { c.Parse.MaxFields = 0 },
		"kafka.value_format":      func(c *Config) { c.Kafka.ValueFormat = "protobuf" },
		"kafka.payload_encoding":  func(c *Config) { c.Kafka.PayloadEncoding = "rot13" },
		"timing.max_pending":      func(c *Config) { c.Timing.MaxPending = 0 },
	}
	for name, mutate := range cases {
		cfg := Defaults()
		mutate(cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

// raw framing does not use a length prefix, so prefix_bytes is not constrained.
func TestRawFramingSkipsPrefixValidation(t *testing.T) {
	cfg := Defaults()
	cfg.Framing.Mode = "raw"
	cfg.Framing.PrefixBytes = 0
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

// The shipped config.toml is shared with the Rust and Java builds. If it stops
// loading here, the three have diverged.
func TestSharedProjectConfigLoads(t *testing.T) {
	path := filepath.Join("..", "..", "..", "config.toml")
	if _, err := os.Stat(path); err != nil {
		t.Skip("shared config.toml not present")
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	// Spot-check the settings the project README calls out.
	if cfg.Parse.Mode != "none" {
		t.Errorf("parse.mode = %q; the shared config sets none because this link carries ISO 8583", cfg.Parse.Mode)
	}
	if cfg.Framing.Mode != "length_prefix" || cfg.Framing.PrefixBytes != 2 {
		t.Errorf("framing = %+v", cfg.Framing)
	}
}
