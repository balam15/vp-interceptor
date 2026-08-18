#!/usr/bin/env python3
"""Decode the interceptor's JSON envelopes.

Reads JSON lines on stdin so it needs no Kafka client library -- pipe
kafka-console-consumer into it:

    docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 --topic vp.fms.iso8583 --from-beginning \
      | python3 examples/consume_json.py

    ... | python3 examples/consume_json.py --direction vp_to_fms
    ... | python3 examples/consume_json.py --raw          # payload bytes only

For a real consumer use confluent-kafka or kafka-python; `decode_envelope`
below is the only part you actually need, and it has no dependencies.
"""

import argparse
import base64
import json
import sys


def decode_envelope(envelope: dict) -> bytes:
    """Return the original wire bytes from an interceptor JSON envelope.

    Honours the `encoding` field rather than assuming base64, so it keeps
    working if the interceptor is reconfigured to hex or utf8.
    """
    encoding = envelope.get("encoding", "base64")
    payload = envelope.get("payload", "")
    if encoding == "base64":
        raw = base64.b64decode(payload)
    elif encoding == "hex":
        raw = bytes.fromhex(payload)
    elif encoding == "utf8":
        raw = payload.encode()
    else:
        raise ValueError(f"unknown encoding {encoding!r}")

    declared = envelope.get("length")
    if declared is not None and declared != len(raw):
        # A mismatch means the envelope was truncated or rewritten in transit.
        raise ValueError(f"length mismatch: declared {declared}, decoded {len(raw)}")
    return raw


def main() -> None:
    ap = argparse.ArgumentParser(description="Decode interceptor JSON envelopes from stdin")
    ap.add_argument("--direction", choices=["vp_to_fms", "fms_to_vp"], help="filter by direction")
    ap.add_argument("--conn-id", type=int, help="filter by connection id")
    ap.add_argument("--raw", action="store_true", help="print only the decoded payload")
    ap.add_argument("--field", metavar="KEY=VALUE",
                    help="filter on a parsed field, e.g. --field accountNumber=11020134353")
    opts = ap.parse_args()

    seen = bad = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        # kafka-console-consumer with print.key=true emits "key\tvalue".
        if "\t" in line:
            line = line.split("\t", 1)[1]
        try:
            env = json.loads(line)
        except json.JSONDecodeError:
            bad += 1
            continue

        if opts.direction and env.get("direction") != opts.direction:
            continue
        if opts.conn_id is not None and env.get("conn_id") != opts.conn_id:
            continue
        if opts.field:
            fk, _, fv = opts.field.partition("=")
            if (env.get("fields") or {}).get(fk) != fv:
                continue

        try:
            raw = decode_envelope(env)
        except ValueError as e:
            print(f"  !! {e}", file=sys.stderr)
            bad += 1
            continue

        seen += 1
        if opts.raw:
            print(raw.decode(errors="replace"))
            continue

        arrow = "->" if env.get("direction") == "vp_to_fms" else "<-"
        timing = ""
        if env.get("rtt_ms") is not None:
            timing += f" rtt={env['rtt_ms']:.3f}ms"
        if env.get("gap_ms") is not None:
            timing += f" gap={env['gap_ms']:.3f}ms"
        if env.get("conn_age_ms") is not None:
            timing += f" age={env['conn_age_ms']:.3f}ms"
        print(
            f"conn={env.get('conn_id')} seq={env.get('seq')} "
            f"{arrow} {env.get('direction')} len={env.get('length')}{timing}"
        )
        if env.get("fields"):
            for k, v in env["fields"].items():
                print(f"   {k:16} = {v}")
        if env.get("parse_error"):
            # The payload is still intact -- only the field extraction failed.
            print(f"   !! parse_error: {env['parse_error']}")
            print(f"   raw: {raw!r}")

    print(f"\ndecoded {seen} envelope(s), {bad} unreadable", file=sys.stderr)


if __name__ == "__main__":
    main()
