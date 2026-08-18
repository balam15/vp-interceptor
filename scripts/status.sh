#!/usr/bin/env bash
# Is the interceptor running -- and is it actually working?
#
#   ./scripts/status.sh
#
# Checks 1-3 prove the process is alive. Check 4 is the one that matters: it
# pushes a real frame through to FMS and back, which is the only way to catch
# "listening but upstream is dead".

set -uo pipefail

ADMIN_ADDR="${ADMIN_ADDR:-127.0.0.1:9101}"
LISTEN_ADDR="${LISTEN_ADDR:-127.0.0.1:9100}"
LISTEN_PORT="${LISTEN_ADDR##*:}"

# The probe frame. Comma-separated key=value so it matches [parse] in
# config.toml and lands in Kafka as clean fields rather than a parse_error.
#
# NOTE: every health check publishes two messages (request + response) to the
# topic. `msgType=healthcheck` is what consumers should filter on to drop them:
#     if msg["fields"].get("msgType") == "healthcheck": continue
# Override for a different wire format:
#     PROBE_BODY='0800|STAN=0|NETMGMT=ECHO' ./scripts/status.sh
PROBE_BODY="${PROBE_BODY:-msgType=healthcheck,source=status.sh,txnRef=00000000}"

green() { printf "\033[32m%s\033[0m\n" "$1"; }
red()   { printf "\033[31m%s\033[0m\n" "$1"; }
dim()   { printf "\033[2m%s\033[0m\n" "$1"; }

rc=0

echo "== 1. process =="
if pgrep -fl vp-fms-interceptor 2>/dev/null; then
  green "   UP"
else
  red "   DOWN - no vp-fms-interceptor process"; rc=1
fi

echo "== 2. listening on $LISTEN_PORT =="
if lsof -nP -iTCP:"$LISTEN_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  green "   UP"
else
  red "   DOWN - nothing listening on $LISTEN_PORT"; rc=1
fi

echo "== 3. admin /healthz =="
if [ "$(curl -s --max-time 2 "http://$ADMIN_ADDR/healthz" 2>/dev/null)" = "ok" ]; then
  green "   UP"
else
  red "   DOWN - admin endpoint not responding on $ADMIN_ADDR"; rc=1
fi

echo "== 4. end-to-end probe (VP -> interceptor -> FMS -> back) =="
probe=$(python3 - "$LISTEN_ADDR" "$PROBE_BODY" <<'PY' 2>&1
import socket, struct, sys
host, port = sys.argv[1].rsplit(":", 1)
body = sys.argv[2].encode()
try:
    s = socket.create_connection((host, int(port)), timeout=3)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.settimeout(3)
    s.sendall(struct.pack(">H", len(body)) + body)
    hdr = s.recv(2)
    if len(hdr) < 2:
        print("FAIL no response header (upstream FMS reachable?)"); sys.exit(1)
    (n,) = struct.unpack(">H", hdr)
    got = b""
    while len(got) < n:
        c = s.recv(n - len(got))
        if not c:
            break
        got += c
    s.close()
    print("OK round-trip %d bytes: %s" % (len(got), got[:60].decode(errors="replace")))
except Exception as e:
    print("FAIL %s" % e); sys.exit(1)
PY
)
if [[ "$probe" == OK* ]]; then
  green "   $probe"
else
  red "   $probe"
  dim "   process is listening but traffic is not completing -- check [upstream] addr and FMS"
  rc=1
fi

echo "== 5. counters =="
metrics=$(curl -s --max-time 2 "http://$ADMIN_ADDR/metrics" 2>/dev/null)
if [ -n "$metrics" ]; then
  echo "$metrics" | grep -vE '^#' \
    | grep -E 'conns_active|conns_accepted|upstream_connect_failed|bytes_vp_to_fms|tee_dropped|framer_desyncs|kafka_delivered|kafka_delivery_failed' \
    | sed 's/^/   /'
  desync=$(echo "$metrics" | awk '$1=="vp_interceptor_framer_desyncs"{print $2}')
  upfail=$(echo "$metrics" | awk '$1=="vp_interceptor_upstream_connect_failed"{print $2}')
  [ "${desync:-0}" != "0" ] && red "   WARNING framer_desyncs=$desync - [framing] config is wrong, Kafka feed is dead"
  [ "${upfail:-0}" != "0" ] && red "   WARNING upstream_connect_failed=$upfail - FMS unreachable (PAYMENTS incident)"
else
  dim "   (metrics unavailable)"
fi

echo
[ $rc -eq 0 ] && green "RESULT: interceptor is running and passing traffic" \
              || red   "RESULT: interceptor is NOT healthy"
exit $rc
