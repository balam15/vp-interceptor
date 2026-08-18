#!/usr/bin/env python3
"""ISO 8583 codec for the EIS_FMS spec (EIS_FMS_ISO8583_jPOS_Packager.xml).

Everything in this file is transcribed from that packager. If the XML changes,
change SPEC below and nothing else.

WHY HAND-ROLLED. The `iso8583` package on PyPI models flat fields only; this
spec has four composite fields (DE 3, 22, 43, 61) whose subfields are packed
positionally with no bitmap of their own. Encoding those correctly is most of
the work here, so the dependency would buy very little. Stdlib only.

WHAT THIS IS FOR
  - building spec-accurate traffic to push through the interceptor
  - reading a real VP<->FMS capture against the spec (`--dump`)
  - as the reference for a future Rust field-parser (see RUNBOOK.md section 10)

WHAT THIS IS NOT. It encodes and decodes the message *body*, starting at the
MTI. It knows nothing about transport framing -- the 2-byte length prefix lives
in send_iso8583.py, and whether that prefix is even correct is still the biggest
open question in this project (HANDOFF.md, "Assumed, NOT verified").

    python3 iso8583.py --selftest
    python3 iso8583.py --dump --template transfer
    python3 iso8583.py --hex --template echo
"""

from __future__ import annotations

import argparse
import sys

# ---------------------------------------------------------------------------
# Field codecs -- one per jPOS class used by the packager.
#
# Each knows how to turn a Python value into its ASCII wire form and back. They
# VALIDATE rather than truncate: a value that does not fit raises, because a
# silently-truncated PAN produces a frame FMS rejects for reasons that are very
# hard to trace back to here.
# ---------------------------------------------------------------------------


class FieldError(ValueError):
    """Raised when a value cannot be represented per the spec."""


class Numeric:
    """org.jpos.iso.IFA_NUMERIC -- fixed width, right-justified, zero-padded."""

    jpos = "IFA_NUMERIC"

    def __init__(self, name: str, length: int) -> None:
        self.name = name
        self.length = length

    def pack(self, value) -> str:
        s = str(value)
        if not s.isdigit():
            raise FieldError(f"{self.name}: {s!r} is not numeric")
        if len(s) > self.length:
            raise FieldError(f"{self.name}: {len(s)} digits exceeds {self.length}")
        return s.zfill(self.length)

    def unpack(self, s: str, pos: int) -> tuple[str, int]:
        end = pos + self.length
        if end > len(s):
            raise FieldError(f"{self.name}: truncated, need {self.length} at {pos}")
        # Returned zero-padded, exactly as it appeared. Stripping would break
        # round-tripping and would also destroy meaning -- an amount field of
        # "0000" is not the same document as "0".
        return s[pos:end], end


class Char:
    """org.jpos.iso.IFA_CHAR -- fixed width, left-justified, space-padded."""

    jpos = "IFA_CHAR"

    def __init__(self, name: str, length: int) -> None:
        self.name = name
        self.length = length

    def pack(self, value) -> str:
        s = str(value)
        if len(s) > self.length:
            raise FieldError(f"{self.name}: {len(s)} chars exceeds {self.length}")
        return s.ljust(self.length)

    def unpack(self, s: str, pos: int) -> tuple[str, int]:
        end = pos + self.length
        if end > len(s):
            raise FieldError(f"{self.name}: truncated, need {self.length} at {pos}")
        # rstrip: pad spaces are an artefact of the encoding, not data. The cost
        # is that genuine trailing spaces do not survive a round trip.
        return s[pos:end].rstrip(" "), end


class _Var:
    """Shared base for the LLVAR / LLLVAR families.

    `digits` is the number of ASCII length characters that precede the data:
    2 for LL, 3 for LLL. `length` is the MAXIMUM data length from the spec.
    """

    def __init__(self, name: str, length: int) -> None:
        self.name = name
        self.length = length

    def pack(self, value) -> str:
        s = str(value)
        if self.numeric_only and not s.isdigit():
            raise FieldError(f"{self.name}: {s!r} is not numeric")
        if len(s) > self.length:
            raise FieldError(f"{self.name}: {len(s)} exceeds max {self.length}")
        if len(s) >= 10 ** self.digits:
            raise FieldError(f"{self.name}: {len(s)} needs more than {self.digits} length digits")
        return str(len(s)).zfill(self.digits) + s

    def unpack(self, s: str, pos: int) -> tuple[str, int]:
        head = s[pos:pos + self.digits]
        if len(head) < self.digits or not head.isdigit():
            raise FieldError(f"{self.name}: bad length indicator {head!r} at {pos}")
        n = int(head)
        if n > self.length:
            # Nearly always means the stream is desynced, not that VP sent an
            # over-long field, so say both.
            raise FieldError(
                f"{self.name}: declared {n} exceeds spec max {self.length} "
                f"(at offset {pos} -- likely a desync, not a long field)"
            )
        start = pos + self.digits
        end = start + n
        if end > len(s):
            raise FieldError(f"{self.name}: truncated, declared {n} but {len(s) - start} remain")
        return s[start:end], end


class LLChar(_Var):
    """org.jpos.iso.IFA_LLCHAR -- 2 ASCII length digits, then the data."""

    jpos = "IFA_LLCHAR"
    digits = 2
    numeric_only = False


class LLNum(_Var):
    """org.jpos.iso.IFA_LLNUM -- as LLCHAR, but the data must be digits."""

    jpos = "IFA_LLNUM"
    digits = 2
    numeric_only = True


class LLLChar(_Var):
    """org.jpos.iso.IFA_LLLCHAR -- 3 ASCII length digits, then the data."""

    jpos = "IFA_LLLCHAR"
    digits = 3
    numeric_only = False


class Composite:
    """A fixed-length field carrying positional subfields (DE 3, 22, 43).

    jPOS models these as <isofieldpackager> + GenericSubFieldPackager. There is
    no bitmap and no delimiter: subfields are simply concatenated in order, so
    every subfield MUST be present and MUST be exactly its declared width. An
    error in any subfield width shifts everything after it.

    The value is a dict keyed by subfield id, matching jPOS numbering:
        {1: "40", 2: "10", 3: "20"}
    """

    jpos = "composite"

    def __init__(self, name: str, length: int, subfields: list) -> None:
        self.name = name
        self.length = length
        self.subfields = subfields
        total = sum(f.length for f in subfields)
        if total != length:
            # Checked at import, so a bad transcription of the XML fails loudly
            # on startup instead of producing subtly wrong frames at runtime.
            raise FieldError(
                f"{name}: subfields sum to {total} but field is declared {length}"
            )

    def pack(self, value) -> str:
        if not isinstance(value, dict):
            raise FieldError(f"{self.name}: expected dict of subfields, got {type(value).__name__}")
        out = []
        for i, sub in enumerate(self.subfields, start=1):
            if i not in value:
                raise FieldError(f"{self.name}: subfield {i} ({sub.name}) is missing")
            out.append(sub.pack(value[i]))
        return "".join(out)

    def unpack(self, s: str, pos: int) -> tuple[dict, int]:
        end = pos + self.length
        if end > len(s):
            raise FieldError(f"{self.name}: truncated, need {self.length} at {pos}")
        return self._split(s[pos:end]), end

    def _split(self, body: str) -> dict:
        out = {}
        at = 0
        for i, sub in enumerate(self.subfields, start=1):
            out[i], at = sub.unpack(body, at)
        if at != len(body):
            raise FieldError(
                f"{self.name}: {len(body) - at} trailing bytes after the last "
                f"subfield -- VP may be sending data this spec does not document"
            )
        return out


class LLLComposite(Composite):
    """DE 61: an LLLVAR wrapper around positional subfields.

    The spec declares max 999 but the six subfields sum to a fixed 10, so the
    length indicator should always read "010". If VP ever appends undocumented
    reserved data past subfield 6, _split() raises rather than silently
    dropping it -- that is a question for the VP team, not something to swallow.
    """

    jpos = "IFA_LLLCHAR + composite"

    def __init__(self, name: str, length: int, subfields: list) -> None:
        # Deliberately skips Composite.__init__'s sum check: here `length` is a
        # maximum, not the packed width.
        self.name = name
        self.length = length
        self.subfields = subfields
        self.fixed_width = sum(f.length for f in subfields)

    def pack(self, value) -> str:
        body = Composite.pack(self, value)
        return str(len(body)).zfill(3) + body

    def unpack(self, s: str, pos: int) -> tuple[dict, int]:
        head = s[pos:pos + 3]
        if len(head) < 3 or not head.isdigit():
            raise FieldError(f"{self.name}: bad length indicator {head!r} at {pos}")
        n = int(head)
        start = pos + 3
        end = start + n
        if end > len(s):
            raise FieldError(f"{self.name}: truncated, declared {n}")
        return self._split(s[start:end]), end


# ---------------------------------------------------------------------------
# THE SPEC -- a direct transcription of EIS_FMS_ISO8583_jPOS_Packager.xml.
#
# Field names are copied verbatim, including the spec's own spelling of
# AQUIRING_INSTITUTAION_ID and AUTHORIATION_ID_RESPONSE. Do not "fix" them:
# they are how the VP team's configuration names these fields.
# ---------------------------------------------------------------------------

SPEC: dict[int, object] = {
    2: LLChar("PAN", 19),
    3: Composite("AKZ", 6, [
        Numeric("TRANSACTION_TYPE", 2),
        Numeric("ACCOUNT_TYPE_FROM", 2),
        Numeric("ACCOUNT_TYPE_TO", 2),
    ]),
    4: Numeric("TRANSACTION_AMOUNT", 12),
    6: Numeric("TRANSACTION_AMOUNT_IDR", 12),
    7: Numeric("DATETIME", 10),
    11: Numeric("TRANSACTION_NUMBER", 6),
    12: Numeric("TIME", 6),
    13: Numeric("DATE", 4),
    15: Numeric("SETTLEMENT_DATE", 4),
    18: Numeric("MERCHANT_TYPE", 4),
    22: Composite("POSENTRYMODE", 3, [
        Numeric("PAN_ENTRY_MODE", 2),
        Numeric("PIN_ENTRY_CAPABILITY", 1),
    ]),
    28: Char("TRANSACTION_AMOUNT_FEE", 9),
    32: LLChar("AQUIRING_INSTITUTAION_ID", 11),
    35: LLChar("TRACK2", 37),
    37: Char("RRN", 12),
    38: Char("AUTHORIATION_ID_RESPONSE", 6),
    39: Char("RESPONSECODE", 2),
    41: Char("TERMINAL_DATA", 16),
    42: Char("CARD_ACCEPTOR_ID", 15),
    43: Composite("CARD_ACCEPTOR_NAME_AND_LOCATION", 41, [
        Char("NAME", 25),
        Char("CITY", 10),
        Char("STATE", 3),
        Char("COUNTRY", 3),
    ]),
    48: LLLChar("ADDITIONAL_DATA", 999),
    49: Numeric("TRANSACTION_CURRENCY_CODE", 3),
    50: Numeric("BENEFICIARY_CURRENCY_CODE", 3),
    51: Numeric("ISSUER_CURRENCY_CODE", 3),
    61: LLLComposite("RESERVED1", 999, [
        Char("SERVICE_CODE", 3),
        Numeric("PIN_INDICATOR", 1),
        Numeric("CVV2_INDICATOR", 1),
        Numeric("ECI_INDICATOR", 3),
        Numeric("ASI_PREAUTH_FLAG", 1),
        Numeric("CVV2_RESULT_FLAG", 1),
    ]),
    63: LLLChar("RESERVED2", 8),
    70: Numeric("NETWORK_MANAGEMENT_INFORMATION_CODE", 3),
    90: Char("ORIGINAL_DATA_ELEMENT", 42),
    100: LLNum("ISSUER_IDENTIFICATION_CODE", 11),
    102: LLChar("FROM_ACCOUNT_NUMBER", 28),
    103: LLChar("DESTINATION_ACCOUNT_NUMBER", 28),
    123: LLLChar("CARD_TYPE_CODE", 255),
    125: LLLChar("TRANSFER_INDICATOR", 255),
    127: LLLChar("DESTINATION_INSTITUTION_IDENTIFICATION_CODE", 255),
}

MAX_VALID_FIELD = 127  # maxValidField in the packager


# ---------------------------------------------------------------------------
# Bitmap
#
# The packager says IFA_BITMAP length="16" -- 16 BYTES, i.e. 128 bits, i.e. up
# to 32 ASCII hex characters. The comment in the XML says "16 hex characters",
# which would cap the message at 64 fields and make DE 100/102/103/123/125/127
# unreachable. Those fields are defined, so the comment and the field list
# contradict each other.
#
# This codec follows the field list: a secondary bitmap is emitted if and only
# if some field above 64 is present. `force_secondary` exists so both readings
# can be generated and tried against the real FMS.
# ---------------------------------------------------------------------------


def pack_bitmap(field_ids, force_secondary: bool = False) -> str:
    secondary = force_secondary or any(f > 64 for f in field_ids)
    nbits = 128 if secondary else 64
    bits = ["0"] * nbits
    if secondary:
        bits[0] = "1"  # bit 1 IS the "secondary bitmap follows" flag
    for f in field_ids:
        if not 2 <= f <= MAX_VALID_FIELD:
            raise FieldError(f"field {f} outside 2..{MAX_VALID_FIELD}")
        bits[f - 1] = "1"
    return "".join("%X" % int("".join(bits[i:i + 4]), 2) for i in range(0, nbits, 4))


def unpack_bitmap(s: str, pos: int) -> tuple[list[int], int]:
    def hex_to_bits(chunk: str, where: int) -> str:
        if len(chunk) < 16:
            raise FieldError(f"bitmap truncated at offset {where}")
        try:
            return bin(int(chunk, 16))[2:].zfill(64)
        except ValueError:
            raise FieldError(f"bitmap {chunk!r} at offset {where} is not ASCII hex") from None

    bits = hex_to_bits(s[pos:pos + 16], pos)
    pos += 16
    if bits[0] == "1":
        bits += hex_to_bits(s[pos:pos + 16], pos)
        pos += 16
    # index 0 is the secondary-bitmap flag, never a data field
    return [i + 1 for i, b in enumerate(bits) if b == "1" and i != 0], pos


# ---------------------------------------------------------------------------
# Message
# ---------------------------------------------------------------------------


class Message:
    """One ISO 8583 message body: an MTI plus a {field number: value} dict."""

    def __init__(self, mti: str, fields: dict | None = None, force_secondary: bool = False) -> None:
        if len(str(mti)) != 4 or not str(mti).isdigit():
            raise FieldError(f"MTI must be 4 digits, got {mti!r}")
        self.mti = str(mti)
        self.fields: dict[int, object] = dict(fields or {})
        self.force_secondary = force_secondary

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, Message)
            and self.mti == other.mti
            and self.fields == other.fields
        )

    def __repr__(self) -> str:
        return f"<Message {self.mti} fields={sorted(self.fields)}>"

    # -- encoding --------------------------------------------------------

    def layout(self) -> list[tuple]:
        """Pack, returning (field, name, jpos class, encoded) per element.

        pack() and dump() both go through this so the printed breakdown can
        never drift from the bytes actually produced.
        """
        ids = sorted(self.fields)
        unknown = [f for f in ids if f not in SPEC]
        if unknown:
            raise FieldError(f"field(s) {unknown} are not defined in this packager")

        rows = [(0, "MESSAGETYPE", "IFA_NUMERIC", self.mti)]
        rows.append((1, "BITMAP", "IFA_BITMAP", pack_bitmap(ids, self.force_secondary)))
        for f in ids:
            spec = SPEC[f]
            rows.append((f, spec.name, spec.jpos, spec.pack(self.fields[f])))
        return rows

    def pack(self) -> bytes:
        # ASCII, not UTF-8: every field in this spec is an IFA_* class, so a
        # non-ASCII byte means bad input, and .encode("ascii") says so loudly.
        return "".join(r[3] for r in self.layout()).encode("ascii")

    # -- decoding --------------------------------------------------------

    @classmethod
    def unpack(cls, data: bytes | str) -> "Message":
        s = data.decode("ascii") if isinstance(data, bytes) else data
        if len(s) < 20:
            raise FieldError(f"body of {len(s)} bytes is too short for MTI + bitmap")
        mti = s[0:4]
        if not mti.isdigit():
            # The single cheapest desync check there is: in an all-ASCII spec
            # the four bytes after the length prefix are always digits.
            raise FieldError(f"MTI {mti!r} is not numeric -- wrong framing offset?")

        ids, pos = unpack_bitmap(s, 4)
        secondary_seen = pos == 36
        fields: dict[int, object] = {}
        for f in ids:
            if f not in SPEC:
                raise FieldError(
                    f"bitmap says field {f} is present but the packager does "
                    f"not define it -- the rest of the message cannot be read"
                )
            fields[f], pos = SPEC[f].unpack(s, pos)

        if pos != len(s):
            raise FieldError(
                f"{len(s) - pos} trailing bytes after the last field "
                f"(a MAC, or a field the packager is missing)"
            )
        return cls(mti, fields, force_secondary=secondary_seen and not any(f > 64 for f in ids))

    # -- human output ----------------------------------------------------

    def dump(self, out=sys.stdout) -> None:
        rows = self.layout()
        body_len = sum(len(r[3]) for r in rows)
        print(f"MTI {self.mti}   body {body_len} bytes", file=out)
        print(f"{'DE':>4}  {'NAME':<38} {'CLASS':<22} {'LEN':>4}  VALUE", file=out)
        print("-" * 110, file=out)
        for de, name, jpos, enc in rows:
            shown = enc.replace(" ", "·")  # make trailing pad visible
            print(f"{de:>4}  {name:<38} {jpos:<22} {len(enc):>4}  {shown}", file=out)
        print("-" * 110, file=out)
        print(f"{'':>4}  {'TOTAL':<38} {'':<22} {body_len:>4}", file=out)


# ---------------------------------------------------------------------------
# Templates
#
# Synthetic data only. The PANs are in the 4111111111111111 test range so a
# capture of this traffic is never mistaken for a real cardholder record.
#
# DE 35 carries full track 2 because that is what the spec says VP sends, and
# because the point of this example is to show exactly what reaches Kafka.
# ---------------------------------------------------------------------------

TEST_PAN = "4111111111111111"
TEST_TRACK2 = TEST_PAN + "=3012101123450000"


def transfer(stan: int = 4521, amount_minor: int = 150000000) -> Message:
    """0200 -- account-to-account transfer, the workhorse message."""
    return Message("0200", {
        2: TEST_PAN,
        3: {1: "40", 2: "10", 3: "20"},          # transfer, savings -> savings
        4: f"{amount_minor:012d}",
        7: "0814032241",                          # MMddHHmmss GMT
        11: f"{stan:06d}",
        12: "102241",                             # local time, GMT+7
        13: "0814",
        15: "0814",
        18: "6011",                               # financial institution / ATM
        22: {1: "02", 2: "1"},                    # magstripe, PIN capable
        32: "451234",
        35: TEST_TRACK2,
        # DE 37 is a fixed 12: a 6-char acquirer prefix plus the 6-digit STAN.
        # DE 11 is 6 digits wide, so the prefix cannot be any longer.
        37: f"622603{stan:06d}",
        41: "ATM00123",
        42: "BNI001",
        43: {1: "ATM JAKARTA PUSAT 001", 2: "JAKARTA", 3: "DKI", 4: "IDN"},
        49: "360",                                # IDR
        102: "0115476291",
        103: "0298813355",
    })


def transfer_response(request: Message, response_code: str = "00") -> Message:
    """0210 -- echoes the request's identity fields, adds DE 38/39.

    Track 2 is deliberately NOT echoed: a response has no reason to carry it.
    """
    echoed = {f: request.fields[f] for f in (2, 3, 4, 7, 11, 12, 13, 15, 18, 22, 32, 37, 41, 42, 49, 102, 103)
              if f in request.fields}
    echoed[38] = "A12345" if response_code == "00" else ""
    echoed[39] = response_code
    return Message("0210", echoed)


def echo(stan: int = 4522) -> Message:
    """0800 -- network management echo test. Note DE 70 alone forces a
    secondary bitmap, which makes this the shortest message that proves the
    32-hex-character bitmap works."""
    return Message("0800", {
        7: "0814032300",
        11: f"{stan:06d}",
        70: "301",
    })


def echo_response(request: Message) -> Message:
    """0810 -- network management response."""
    return Message("0810", {
        7: request.fields[7],
        11: request.fields[11],
        39: "00",
        70: request.fields[70],
    })


def reversal(stan: int = 4523, original_stan: int = 4521) -> Message:
    """0420 -- reversal advice. Exercises DE 90, the only 42-byte fixed field.

    DE 90 layout: original MTI(4) + STAN(6) + datetime(10) + acquirer(11) +
    forwarder(11) = 42. Only the first four components are written; the unused
    forwarder and the acquirer's own right-padding are both supplied by
    IFA_CHAR. They are indistinguishable on the wire anyway, and IFA_CHAR
    strips trailing spaces on the way back, so writing them out explicitly
    would not survive a round trip.
    """
    original = "0200" + f"{original_stan:06d}" + "0814032241" + "451234"
    return Message("0420", {
        2: TEST_PAN,
        3: {1: "40", 2: "10", 3: "20"},
        4: "000150000000",
        7: "0814032410",
        11: f"{stan:06d}",
        12: "102410",
        13: "0814",
        15: "0814",
        18: "6011",
        22: {1: "02", 2: "1"},
        32: "451234",
        37: f"622603{stan:06d}",
        41: "ATM00123",
        42: "BNI001",
        49: "360",
        61: {1: "101", 2: "1", 3: "0", 4: "000", 5: "0", 6: "0"},
        90: original,
        102: "0115476291",
        103: "0298813355",
    })


TEMPLATES = {
    "transfer": transfer,
    "echo": echo,
    "reversal": reversal,
}


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------


def selftest() -> int:
    failures = 0

    def check(label: str, cond: bool, detail: str = "") -> None:
        nonlocal failures
        if cond:
            print(f"  ok   {label}")
        else:
            failures += 1
            print(f"  FAIL {label} {detail}")

    print("round trip")
    req = transfer()
    for label, msg in [
        ("0200 transfer", req),
        ("0210 response", transfer_response(req)),
        ("0800 echo", echo()),
        ("0810 echo response", echo_response(echo())),
        ("0420 reversal", reversal()),
    ]:
        raw = msg.pack()
        back = Message.unpack(raw)
        check(f"{label} ({len(raw)} bytes)", back == msg,
              f"\n       sent {msg.fields}\n       got  {back.fields}")
        check(f"{label} re-packs identically", back.pack() == raw)

    print("\ntemplates hold at the edges of the STAN range")
    # DE 11 is 6 digits, so every template must still pack at stan=999999.
    # Found the hard way: a 12-char RRN built as "7-char prefix + 5-digit stan"
    # silently worked until connection 1 reached a 6-digit STAN.
    for name, build in sorted(TEMPLATES.items()):
        for stan in (0, 1, 999999):
            try:
                build(stan).pack()
                check(f"{name} at stan={stan}", True)
            except FieldError as e:
                check(f"{name} at stan={stan}", False, f"-- {e}")

    print("\nbitmap")
    check("no field > 64 -> 16 hex chars", len(pack_bitmap([2, 3, 4])) == 16)
    check("field > 64 -> 32 hex chars", len(pack_bitmap([2, 102])) == 32)
    check("force_secondary widens to 32", len(pack_bitmap([2, 3], force_secondary=True)) == 32)
    check("transfer bitmap is the expected value",
          pack_bitmap(sorted(req.fields)) == "F23A440128E080000000000006000000",
          pack_bitmap(sorted(req.fields)))

    print("\nvalidation rejects bad input")
    for label, thunk in [
        ("over-long PAN", lambda: SPEC[2].pack("1" * 20)),
        ("non-numeric amount", lambda: SPEC[4].pack("12.50")),
        ("missing composite subfield", lambda: SPEC[3].pack({1: "40", 2: "10"})),
        ("undefined field number", lambda: Message("0200", {5: "x"}).pack()),
        ("non-numeric MTI", lambda: Message.unpack(b"ABCD" + b"0" * 32)),
    ]:
        try:
            thunk()
            check(label, False, "-- expected FieldError, none raised")
        except FieldError:
            check(label, True)

    print("\ntrailing bytes are reported, not ignored")
    try:
        Message.unpack(req.pack() + b"MACX")
        check("trailing MAC detected", False, "-- expected FieldError")
    except FieldError as e:
        check("trailing MAC detected", "trailing" in str(e))

    print()
    if failures:
        print(f"{failures} FAILED")
    else:
        print("all passed")
    return 1 if failures else 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--selftest", action="store_true", help="round-trip every template")
    ap.add_argument("--dump", action="store_true", help="print the field-by-field breakdown")
    ap.add_argument("--hex", action="store_true", help="print the body as hex")
    ap.add_argument("--template", choices=sorted(TEMPLATES), default="transfer")
    opts = ap.parse_args()

    if opts.selftest:
        sys.exit(selftest())

    msg = TEMPLATES[opts.template]()
    if opts.dump:
        msg.dump()
    elif opts.hex:
        print(msg.pack().hex(" "))
    else:
        print(msg.pack().decode("ascii"))


if __name__ == "__main__":
    main()
