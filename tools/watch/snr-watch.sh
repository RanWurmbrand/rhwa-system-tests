#!/usr/bin/env bash
#
# Snapshot of everything an SNR remediation touches: node Ready/cordon/boot ID
# /taints, any SelfNodeRemediation or NodeHealthCheck CR, the live
# minPeersForRemediation, and the agent pods with their ages.
#
#   watch -n 5 -d tools/watch/snr-watch.sh
#
# Designed for `watch -d`, which highlights whatever changed since the last
# tick -- a boot ID flipping, a node going Unknown, a taint or cordon
# appearing. Useful for any destructive medik8s spec, not just one: record the
# boot IDs of the nodes under test before the run and watch which ones move.
#
# A rebooting node shows up in three stages, slowest last: Ready becomes
# Unknown (~40s, stale heartbeat, not False), then the unreachable/not-ready
# taints land, then the boot ID changes -- which only happens once the kubelet
# re-registers, so it marks the node coming *back*.
#
# Each tick is one `oc get nodes -o json` plus three small reads. Raise -n if
# you would rather not add API traffic to a spec about api-server connectivity.
NS=openshift-workload-availability

echo "=== nodes ($(date +%H:%M:%S)) ==="
oc get nodes -o json 2>/dev/null | python3 -c '
import json, sys
try:
    nodes = json.load(sys.stdin)["items"]
except Exception:
    print("  <api unreachable>"); sys.exit(0)
fmt = "%-10s %-9s %-7s %-10s %s"
print(fmt % ("NAME", "READY", "CORDON", "BOOTID", "TAINTS"))
for n in nodes:
    spec, status = n["spec"], n["status"]
    ready = next((c["status"] for c in status.get("conditions", [])
                  if c["type"] == "Ready"), "?")
    taints = ",".join(t["key"].split("/")[-1] for t in spec.get("taints", [])) or "-"
    print(fmt % (n["metadata"]["name"], ready,
                 "YES" if spec.get("unschedulable") else "-",
                 status.get("nodeInfo", {}).get("bootID", "?")[:8], taints))
'

echo
echo "=== remediation CRs ==="
oc get selfnoderemediation,nodehealthcheck -A --no-headers 2>/dev/null | head -10 || true
[ -z "$(oc get selfnoderemediation -A --no-headers 2>/dev/null)" ] && echo "  none"

echo
echo "=== snrc ==="
oc get selfnoderemediationconfig -n "$NS" \
  -o jsonpath='  minPeersForRemediation={.items[0].spec.minPeersForRemediation}{"\n"}' 2>/dev/null

echo
echo "=== agent pods ==="
oc get pods -n "$NS" --no-headers -o wide 2>/dev/null |
  awk '{printf "  %-58s %-5s %-9s %-6s %s\n", $1, $2, $3, $5, $7}'
