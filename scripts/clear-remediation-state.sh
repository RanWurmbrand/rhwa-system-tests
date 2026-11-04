#!/usr/bin/env bash
# Clear every node marker and remediation CR that a medik8s operator (NHC, SNR,
# FAR, MDR, SBR, NMO) or this test suite can leave behind, so a cluster that was
# left mid-remediation comes back fully healthy.
#
#   scripts/clear-remediation-state.sh            # dry run, prints what it would do
#   scripts/clear-remediation-state.sh --apply    # make the changes
#   scripts/clear-remediation-state.sh --apply --force-finalizers
#
# --force-finalizers also strips finalizers from remediation CRs that will not
# delete on their own. A remediation CR whose controller is gone or has no
# config CR (e.g. an SBR with no StorageBasedRemediationConfig) strands on its
# finalizer forever and blocks NHC deletion; that is the case this is for. It
# skips the controller's own cleanup, so only use it once a plain delete hangs.
set -euo pipefail

APPLY=false
FORCE_FINALIZERS=false
for arg in "$@"; do
    case "$arg" in
        --apply) APPLY=true ;;
        --force-finalizers) FORCE_FINALIZERS=true ;;
        -h|--help) sed -n '2,18p' "$0" | sed 's|^# \?||'; exit 0 ;;
        *) echo "unknown argument: $arg (see --help)" >&2; exit 2 ;;
    esac
done
command -v oc >/dev/null || { echo "oc not found in PATH" >&2; exit 1; }
oc whoami >/dev/null 2>&1 || { echo "not logged in to a cluster" >&2; exit 1; }

CHANGES=0
run() {
    if $APPLY; then
        echo "  + $*"
        "$@"
    else
        echo "  would run: $*"
    fi
    CHANGES=$((CHANGES + 1))
}

echo "cluster: $(oc whoami --show-server)"
$APPLY || echo "DRY RUN — nothing will be changed. Re-run with --apply."
echo

# --- 1. Taints -------------------------------------------------------------
# out-of-service: SBR and SNR fencing. Until it is gone the CSI node plugin
#   stays evicted and PVC mounts hang in ContainerCreating.
# medik8s.io/drain: NMO maintenance.
# medik8s.io/remediation: older SNR builds.
REMEDIATION_TAINTS=(
    "node.kubernetes.io/out-of-service"
    "medik8s.io/drain"
    "medik8s.io/remediation"
)

echo "== taints =="
for taint in "${REMEDIATION_TAINTS[@]}"; do
    # One node per line, only nodes actually carrying the taint.
    mapfile -t tainted < <(oc get nodes -o json |
        jq -r --arg k "$taint" '.items[] | select((.spec.taints // [])[]? .key == $k) | .metadata.name')
    for node in "${tainted[@]}"; do
        [[ -n $node ]] || continue
        echo "$node has $taint"
        run oc adm taint node "$node" "${taint}-"
    done
done

# --- 2. Cordons ------------------------------------------------------------
echo
echo "== cordons =="
mapfile -t cordoned < <(oc get nodes -o json |
    jq -r '.items[] | select(.spec.unschedulable == true) | .metadata.name')
for node in "${cordoned[@]}"; do
    [[ -n $node ]] || continue
    echo "$node is cordoned"
    run oc adm uncordon "$node"
done

# --- 3. Node labels --------------------------------------------------------
# exclude-from-remediation makes NHC permanently skip a node; the rest are test
# run markers this suite adds to scope its own fixtures.
REMEDIATION_LABELS=(
    "remediation.medik8s.io/exclude-from-remediation"
    "e2e.medik8s.io/far-nhc-target"
    "test.medik8s.io/fbc-upgrade-run"
    "test.medik8s.io/nhc-upgrade-run"
    "test.medik8s.io/sbr-upgrade-run"
)

echo
echo "== node labels =="
for label in "${REMEDIATION_LABELS[@]}"; do
    mapfile -t labelled < <(oc get nodes -o json |
        jq -r --arg k "$label" '.items[] | select(.metadata.labels | has($k)) | .metadata.name')
    for node in "${labelled[@]}"; do
        [[ -n $node ]] || continue
        echo "$node has label $label"
        run oc label node "$node" "${label}-"
    done
done

# --- 4. Node annotations ---------------------------------------------------
# Matched by prefix so an annotation a newer operator build adds is still caught.
# The SNR agent owns is-reboot-capable and watchdog-timeout and re-adds them
# within a minute if SNR is installed — clearing them is safe either way, and
# they are pure litter once SNR is uninstalled.
ANNOTATION_PREFIXES=(
    "remediation.medik8s.io/"
    "self-node-remediation.medik8s.io/"
    "is-reboot-capable.self-node-remediation.medik8s.io"
    "fence-agents-remediation.medik8s.io/"
    "machine-deletion-remediation.medik8s.io/"
    "storage-based-remediation.medik8s.io/"
    "nodemaintenance.medik8s.io/"
    "system-tests.medik8s.io/"
    "test.medik8s.io/"
)

echo
echo "== node annotations =="
# node<TAB>annotation-key for every annotation matching a prefix above.
while IFS=$'\t' read -r node key; do
    [[ -n $node ]] || continue
    echo "$node has annotation $key"
    run oc annotate node "$node" "${key}-"
done < <(oc get nodes -o json | jq -r --argjson prefixes "$(printf '%s\n' "${ANNOTATION_PREFIXES[@]}" | jq -R . | jq -s .)" '
    .items[]
    | .metadata.name as $n
    | (.metadata.annotations // {})
    | to_entries[]
    | select(.key as $k | $prefixes | any(. as $p | $k | startswith($p)))
    | "\($n)\t\(.key)"')

# --- 5. Remediation CRs ----------------------------------------------------
# Deleted before NHC CRs: NHC will not finish deleting while it still owns an
# in-flight remediation. Only kinds whose CRD is installed are touched.
REMEDIATION_KINDS=(
    "selfnoderemediations.self-node-remediation.medik8s.io"
    "fenceagentsremediations.fence-agents-remediation.medik8s.io"
    "machinedeletionremediations.machine-deletion-remediation.medik8s.io"
    "storagebasedremediations.storage-based-remediation.medik8s.io"
    "nodemaintenances.nodemaintenance.medik8s.io"
)

echo
echo "== remediation CRs =="
for kind in "${REMEDIATION_KINDS[@]}"; do
    oc get crd "$kind" >/dev/null 2>&1 || continue
    # "-" marks a cluster-scoped CR. A literal empty first field would be eaten
    # by read, since tab counts as IFS whitespace, and NMO is cluster-scoped.
    while IFS=$'\t' read -r ns name; do
        [[ -n $name ]] || continue
        if [[ $ns != "-" ]]; then
            echo "$kind $ns/$name"
            run oc delete "$kind" "$name" -n "$ns" --wait=false
            $FORCE_FINALIZERS && run oc patch "$kind" "$name" -n "$ns" \
                --type=merge -p '{"metadata":{"finalizers":null}}'
        else
            echo "$kind $name"
            run oc delete "$kind" "$name" --wait=false
            $FORCE_FINALIZERS && run oc patch "$kind" "$name" \
                --type=merge -p '{"metadata":{"finalizers":null}}'
        fi
    done < <(oc get "$kind" -A -o json 2>/dev/null |
        jq -r '.items[] | "\(.metadata.namespace // "-")\t\(.metadata.name)"')
done

echo
if [[ $CHANGES -eq 0 ]]; then
    echo "No remediation state found — the cluster is already clean."
elif $APPLY; then
    echo "Applied $CHANGES change(s). Verify with:"
    echo "  oc get nodes"
    echo "  oc get nodehealthcheck -o wide"
else
    echo "$CHANGES change(s) pending. Re-run with --apply to make them."
fi
