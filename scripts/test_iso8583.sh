#!/usr/bin/env bash
# End-to-end test of the ISO 8583 examples against a real interceptor.
#
#   ./scripts/test_iso8583.sh
#   ./scripts/test_iso8583.sh --count 200 --connections 16
#   SKIP_KAFKA=1 ./scripts/test_iso8583.sh      # no Docker / no broker
#   KEEP_UP=1 ./scripts/test_iso8583.sh         # leave the rig running to poke at
#
# Brings up Kafka, a mock FMS, and the interceptor; pushes spec-accurate
# ISO 8583 through them; then reads every frame back out of Kafka and re-parses
# it with the same codec that built it. Tears down whatever it started.
#
# WHAT A PASS DOES AND DOES NOT PROVE
#   Does:     the codec round-trips, the reassembler handles real frame sizes
#             under concurrency, and the interceptor's Kafka copy is
#             byte-identical to what crossed the link.
#   Does NOT: validate the [framing] length-prefix guess. This script's client
#             and the interceptor read the same setting, so agreement between
#             them is circular. Only a real VP<->FMS capture settles that --
#             see HANDOFF.md, "Assumed, NOT verified".

set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
ROOT="$PWD"

COUNT=50
CONNECTIONS=8
BIN="${BIN:-$ROOT/rust-interceptor/target/release/vp-fms-interceptor}"
CONFIG="${CONFIG:-$ROOT/config.toml}"
LISTEN_ADDR="${LISTEN_ADDR:-127.0.0.1:9100}"
ADMIN_ADDR="${ADMIN_ADDR:-127.0.0.1:9101}"
FMS_PORT="${FMS_PORT:-8583}"
TOPIC="${TOPIC:-vp.fms.iso8583}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-vp-testkit-kafka}"
SKIP_KAFKA="${SKIP_KAFKA:-0}"
KEEP_UP="${KEEP_UP:-0}"

while [ $# -gt 0 ]; do
  case "$1" in
    --count)       COUNT="$2"; shift 2 ;;
    --connections) CONNECTIONS="$2"; shift 2 ;;
    --skip-kafka)  SKIP_KAFKA=1; shift ;;
    --keep-up)     KEEP_UP=1; shift ;;
    -h|--help)     sed -n '2,20p' "$0"; exit 0 ;;
    *)             echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

green() { printf "\033[32m%s\033[0m\n" "$1"; }
red()   { printf "\033[31m%s\033[0m\n" "$1"; }
dim()   { printf "\033[2m%s\033[0m\n" "$1"; }

rc=0
fail() { red "   FAIL $1"; rc=1; }
pass() { green "   OK   $1"; }

WORK=$(mktemp -d)
FMS_PID=""
INT_PID=""
KAFKA_STARTED=0

cleanup() {
  if [ "$KEEP_UP" = "1" ]; then
    echo
    dim "KEEP_UP=1 -- leaving the rig running. Stop it with:"
    [ -n "$INT_PID" ] && dim "   kill $INT_PID   # interceptor"
    [ -n "$FMS_PID" ] && dim "   kill $FMS_PID   # mock FMS"
    [ "$KAFKA_STARTED" = "1" ] && dim "   (cd testkit && docker compose down -v)"
    dim "   logs in $WORK"
    return
  fi
  echo
  echo "== cleanup =="
  # Only ever kill what this script started. A stray pkill would take out an
  # interceptor the user is running on purpose.
  [ -n "$INT_PID" ] && kill "$INT_PID" 2>/dev/null && dim "   stopped interceptor ($INT_PID)"
  [ -n "$FMS_PID" ] && kill "$FMS_PID" 2>/dev/null && dim "   stopped mock FMS ($FMS_PID)"
  if [ "$KAFKA_STARTED" = "1" ]; then
    # -v drops the volume too. The topic holds full track 2 at this point;
    # leaving it on disk is the thing this project is trying to stop doing.
    (cd "$ROOT/testkit" && docker compose down -v >/dev/null 2>&1) \
      && dim "   removed Kafka container and its data"
  fi
  rm -rf "$WORK"
}
# EXIT alone is not enough: piping this script into `head` kills it with
# SIGPIPE, which terminates without running the EXIT trap and leaks the
# interceptor and mock FMS holding their ports. Same for Ctrl-C.
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 141' PIPE HUP

port_busy() { lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; }

# ---------------------------------------------------------------------------

echo "== 1. prerequisites =="
command -v python3 >/dev/null || { red "   python3 not found"; exit 1; }
pass "python3 $(python3 -V 2>&1 | awk '{print $2}')"

if [ ! -x "$BIN" ]; then
  red "   interceptor binary not found at $BIN"
  dim "   build it: cd rust-interceptor && cargo build --release"
  dim "   (rustup.rs is blocked on this network -- install Rust via Homebrew)"
  exit 1
fi
pass "interceptor binary"

for p in "${LISTEN_ADDR##*:}" "${ADMIN_ADDR##*:}" "$FMS_PORT"; do
  if port_busy "$p"; then
    red "   port $p is already in use"
    dim "   something is already running there; stop it or override the *_ADDR vars"
    exit 1
  fi
done
pass "ports ${LISTEN_ADDR##*:}, ${ADMIN_ADDR##*:}, $FMS_PORT are free"

if [ "$SKIP_KAFKA" != "1" ] && ! docker info >/dev/null 2>&1; then
  dim "   Docker not available -- continuing without Kafka verification"
  SKIP_KAFKA=1
fi

# ---------------------------------------------------------------------------

echo "== 2. codec self-test =="
if python3 "$ROOT/examples/iso8583.py" --selftest > "$WORK/selftest.log" 2>&1; then
  pass "$(grep -c '^  ok' "$WORK/selftest.log") checks passed"
else
  fail "self-test failed"
  grep -E '^  FAIL' "$WORK/selftest.log" | sed 's/^/   /'
  exit 1
fi

# ---------------------------------------------------------------------------

echo "== 3. start the rig =="

# Is anything already serving the broker port? The interceptor publishes to
# whatever answers on localhost:9092, so if you are running your own Kafka,
# that is where the data goes -- starting testkit's container alongside it
# would fight for the port and then read from the wrong broker.
broker_listening() { (exec 3<>/dev/tcp/127.0.0.1/9092) 2>/dev/null; }

if [ "$SKIP_KAFKA" != "1" ]; then
  if broker_listening; then
    if docker ps --format '{{.Names}}' | grep -qx "$KAFKA_CONTAINER"; then
      dim "   reusing running $KAFKA_CONTAINER (will not be torn down)"
    else
      # Someone else's broker owns 9092. Find a container to run the console
      # consumer in -- excluding UI sidecars, which have no kafka-*.sh scripts.
      found=$(docker ps --format '{{.Names}} {{.Image}}' \
                | grep -i kafka | grep -viE 'console|ui|ksql|connect|schema' \
                | head -1 | cut -d' ' -f1)
      if [ -n "$found" ]; then
        KAFKA_CONTAINER="$found"
        dim "   a broker already owns 9092 -- using container '$KAFKA_CONTAINER'"
        dim "   (not starting testkit's Kafka; override with KAFKA_CONTAINER=...)"
      else
        dim "   a broker already owns 9092 but no Kafka container was found;"
        dim "   traffic will still publish there. Set KAFKA_CONTAINER=<name> to"
        dim "   let this script read it back."
        SKIP_KAFKA=1
      fi
    fi
  else
    (cd "$ROOT/testkit" && docker compose up -d >/dev/null 2>&1) || {
      fail "could not start Kafka"; SKIP_KAFKA=1; }
    [ "$SKIP_KAFKA" != "1" ] && KAFKA_STARTED=1
  fi
fi

if [ "$SKIP_KAFKA" != "1" ]; then
  # `docker compose up -d` returns when the CONTAINER starts, which is well
  # before the BROKER accepts connections. Sending traffic in that window makes
  # the producer buffer and then time out, and the whole run reports zero
  # delivered for a reason that has nothing to do with the interceptor.
  kready=0
  for _ in $(seq 1 60); do
    if docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
         --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
      kready=1; break
    fi
    sleep 1
  done
  if [ "$kready" = "1" ]; then
    pass "Kafka broker accepting connections ($KAFKA_CONTAINER)"
  else
    fail "broker on 9092 never answered via container '$KAFKA_CONTAINER'"
    dim "   set KAFKA_CONTAINER=<name> if the broker runs elsewhere"
    SKIP_KAFKA=1
  fi
fi

# NOT wrapped in a subshell: `( ... ) &` sets $! to the subshell, and killing
# that leaves the python child orphaned holding the port -- which then fails
# the port check on the next run. Python puts the script's own directory on
# sys.path, so mock_fms.py finds protocol.py without needing a cd.
python3 "$ROOT/testkit/mock_fms.py" --port "$FMS_PORT" > "$WORK/fms.log" 2>&1 &
FMS_PID=$!
RUST_LOG=info,rdkafka=warn "$BIN" "$CONFIG" > "$WORK/interceptor.log" 2>&1 &
INT_PID=$!
# Drop both from the job table so bash does not print "Terminated: 15" over the
# results when cleanup kills them. They stay killable by PID.
disown "$FMS_PID" "$INT_PID" 2>/dev/null

# Poll rather than sleep: the interceptor is ready when admin answers.
ready=0
for _ in $(seq 1 50); do
  if [ "$(curl -s --max-time 1 "http://$ADMIN_ADDR/healthz" 2>/dev/null)" = "ok" ]; then
    ready=1; break
  fi
  sleep 0.2
done
if [ "$ready" != "1" ]; then
  fail "interceptor did not become ready"
  tail -20 "$WORK/interceptor.log" | sed 's/^/   /'
  exit 1
fi
pass "mock FMS ($FMS_PID) and interceptor ($INT_PID) up"

# ---------------------------------------------------------------------------

echo "== 4. send ISO 8583 =="
# Every envelope is stamped with ts_ms by the interceptor, on this host. Record
# the start so step 6 can verify THIS run's frames and ignore whatever the
# topic already held -- on a shared broker, --from-beginning otherwise
# "verifies" messages from previous runs.
RUN_START_MS=$(python3 -c 'import time; print(int(time.time() * 1000))')
sent_total=0
for tpl in transfer reversal echo; do
  if [ "$tpl" = "transfer" ]; then n=$COUNT; c=$CONNECTIONS; else n=20; c=1; fi
  if out=$(python3 "$ROOT/examples/send_iso8583.py" \
             --target "$LISTEN_ADDR" --template "$tpl" \
             --count "$n" --connections "$c" --quiet 2>&1); then
    ok=$(echo "$out" | awk -F'ok=| errors=' '/^ok=/{print $2}')
    lat=$(echo "$out" | grep -o 'p50=[0-9.]*ms' | head -1)
    pass "$tpl: $ok sent, 0 errors  $lat"
    sent_total=$((sent_total + ok))
  else
    fail "$tpl: $(echo "$out" | tail -3 | tr '\n' ' ')"
  fi
done
dim "   $sent_total requests total (expect $((sent_total * 2)) frames: request + response)"

# ---------------------------------------------------------------------------

echo "== 5. interceptor counters =="
val() { echo "$metrics" | awk -v k="vp_interceptor_$1" '$1==k{print $2}'; }

# Publishing is fire-and-forget with linger_ms=5, so delivery callbacks land
# slightly after the last response. Give them a moment before judging the
# counters, otherwise a fast run reports delivered=0 on a healthy system.
for _ in $(seq 1 40); do
  metrics=$(curl -s --max-time 2 "http://$ADMIN_ADDR/metrics" 2>/dev/null)
  [ "$SKIP_KAFKA" = "1" ] && break
  d=$(val kafka_delivered); f=$(val kafka_delivery_failed); e=$(val frames_emitted)
  [ $(( ${d:-0} + ${f:-0} )) -ge "${e:-1}" ] && break
  sleep 0.25
done

desyncs=$(val framer_desyncs)
frames=$(val frames_emitted)
dropped=$(val tee_dropped)
delivered=$(val kafka_delivered)
failed=$(val kafka_delivery_failed)

echo "   frames_emitted=$frames framer_desyncs=${desyncs:-?} tee_dropped=${dropped:-0}"
echo "   kafka_delivered=${delivered:-0} kafka_delivery_failed=${failed:-0}"

[ "${desyncs:-1}" = "0" ] && pass "no framer desyncs" \
  || fail "framer_desyncs=$desyncs -- [framing] does not match the wire"
[ "$frames" = "$((sent_total * 2))" ] && pass "frame count matches exactly" \
  || fail "expected $((sent_total * 2)) frames, got $frames"
[ "${dropped:-0}" = "0" ] && pass "nothing dropped by the tee" \
  || dim "   note: tee_dropped=$dropped (queue pressure, harmless to VP<->FMS)"
if [ "$SKIP_KAFKA" != "1" ]; then
  [ "${delivered:-0}" = "$frames" ] && pass "all $frames frames delivered to Kafka" \
    || fail "kafka_delivered=${delivered:-0} of $frames (delivery_failed=${failed:-0})"
fi

# ---------------------------------------------------------------------------

echo "== 6. read it back out of Kafka =="
if [ "$SKIP_KAFKA" = "1" ]; then
  dim "   skipped (no broker)"
else
  # Read every offset currently in the topic, then filter by timestamp. Capping
  # --max-messages at the true end offset means the consumer returns
  # immediately instead of idling until --timeout-ms on every run.
  total=$(docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-get-offsets.sh \
            --bootstrap-server localhost:9092 --topic "$TOPIC" 2>/dev/null \
            | awk -F: '{s+=$3} END{print s+0}')
  [ -z "$total" ] || [ "$total" = "0" ] && total="$frames"

  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC" --from-beginning \
    --max-messages "$total" --timeout-ms 20000 > "$WORK/kafka.jsonl" 2>/dev/null

  all=$(wc -l < "$WORK/kafka.jsonl" | tr -d ' ')
  got=$(awk -v t="$RUN_START_MS" -F'"ts_ms":' \
          'NF>1 {split($2,a,","); if (a[1]+0 >= t) n++} END{print n+0}' "$WORK/kafka.jsonl")
  if [ "$got" != "$frames" ]; then
    fail "drained $got envelopes from this run, expected $frames (topic holds $all)"
  else
    pass "drained all $got envelopes from this run ($all in the topic)"
  fi

  if [ "$got" = "0" ]; then
    dim "   nothing to verify -- skipping the parse check"
  elif ! python3 - "$WORK/kafka.jsonl" "$ROOT" "$RUN_START_MS" <<'PY'
import base64, json, sys
sys.path.insert(0, sys.argv[2] + "/examples")
from iso8583 import Message, FieldError

since = int(sys.argv[3])
envs = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
envs = [e for e in envs if e.get("ts_ms", 0) >= since]  # this run only
vp = [e for e in envs if e["direction"] == "vp_to_fms"]

parsed = mismatched = failed = track2 = 0
mtis = {}
for e in vp:
    raw = base64.b64decode(e["payload"])
    if len(raw) != e["length"]:
        mismatched += 1
        continue
    if b"4111111111111111=" in raw:
        track2 += 1
    try:
        m = Message.unpack(raw)
    except (FieldError, UnicodeDecodeError):
        failed += 1
        continue
    # The real assertion: what the interceptor copied to Kafka re-encodes to
    # exactly the bytes that crossed the link. Anything else means the tee is
    # altering traffic, which is the one thing it must never do.
    if m.pack() != raw:
        mismatched += 1
        continue
    parsed += 1
    mtis[m.mti] = mtis.get(m.mti, 0) + 1

# `parsed == len(vp)` is trivially true when both are zero, so require that
# something was actually checked -- an empty topic must not read as a pass.
ok = bool(vp) and failed == 0 and mismatched == 0 and parsed == len(vp)
tag = "\033[32m   OK  \033[0m" if ok else "\033[31m   FAIL\033[0m"
print(f"{tag} {parsed}/{len(vp)} vp_to_fms envelopes re-parsed byte-identical"
      f"  MTIs {mtis}")
if failed or mismatched:
    print(f"        unparsed={failed} mismatched={mismatched}")

if track2:
    print(f"\033[31m   PCI \033[0m {track2} envelopes contain full track 2 at rest in Kafka.")
    print("        Synthetic here (4111...), real in production. Sensitive")
    print("        Authentication Data must not be retained after authorization")
    print("        -- mask in the tee worker before publish. HANDOFF.md item 2.")

# The shipped [parse] mode is key_value, which is meaningless on ISO 8583 and
# says so nowhere in the envelope. Show it rather than describe it.
f = envs[0].get("fields") if envs else None
if f and len(f) == 1 and len(next(iter(f))) > 40:
    print(f"\033[33m   NOTE\033[0m [parse] mode=\"key_value\" split an ISO frame on the '='")
    print(f"        inside track 2 and reported parse_error={envs[0].get('parse_error')!r}.")
    print("        Set mode = \"none\" before trusting the envelope's `fields`.")
elif envs and not f:
    print("\033[32m   OK  \033[0m [parse] is off -- no bogus `fields`, `payload` is the record")

sys.exit(0 if ok else 1)
PY
  then
    rc=1
  fi
fi

# ---------------------------------------------------------------------------

echo
if [ "$rc" = "0" ]; then
  green "PASS"
else
  red "FAIL -- logs in $WORK"
  [ "$KEEP_UP" != "1" ] && dim "   re-run with KEEP_UP=1 to keep the rig and logs around"
fi
exit "$rc"
