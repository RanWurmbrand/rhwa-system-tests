package tests

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/pod"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/reportxml"

	"github.com/medik8s/system-tests/tests/internal/labels"
	. "github.com/medik8s/system-tests/tests/internal/medik8sinittools"
	"github.com/medik8s/system-tests/tests/internal/medik8sparams"
	"github.com/medik8s/system-tests/tests/sbr-operator/internal/sbrparams"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// splitBrainState is the victim's heartbeat slot, identified in the on-device node map while the
// victim is cut off from storage, and the UnixNano timestamp of the last heartbeat it wrote. The
// timestamp anchors the ordering assertion: fencing may only be proven after the victim actually
// went silent, so the FencingSucceeded transition must be later than this.
type splitBrainState struct {
	victimNodeID uint16
	heartbeatTS  int64
}

// splitBrainBaseline is the pre-partition state the test proves the fencing left the witnesses in:
// their boot-ids and self-fence counters must be untouched when only the victim is isolated.
type splitBrainBaseline struct {
	victimBootID    string
	witnessBootIDs  map[string]string  // witness node name -> boot-id
	witnessAgents   map[string]string  // witness node name -> its SBR agent pod name
	witnessFenced   map[string]float64 // witness node name -> sbr_self_fenced_total
	witnessRestarts map[string]int32   // witness node name -> agent container restart count
}

var _ = Describe(
	"SBR Block Storage — Ceph RBD Split-Brain Prevention (Storage Arbitration)",
	Ordered,
	ContinueOnFailure,
	Label(labels.OperatorSBR, labels.ComponentRemediation), func() {
		var (
			testSBRC        *unstructured.Unstructured
			victimNodeName  string
			witnessNodes    []string
			observerPodName string
		)

		BeforeAll(func() {
			By("Checking the NodeHealthCheck CRD is installed")

			if !isNHCCRDInstalled() {
				Skip("NodeHealthCheck CRD not found; NHC operator not installed — skipping block split-brain test")
			}

			// Remove this test's own NodeHealthCheck from a crashed prior run first: it references the
			// SBR template, so requireNoSBRNodeHealthCheck would otherwise see it and skip the test.
			By("Removing any NodeHealthCheck left by a previous run of this test")
			deleteNHCAndWait(sbrparams.NHCBlockSplitBrainTestName)

			By("Verifying no other NodeHealthCheck already remediates through the SBR template")
			requireNoSBRNodeHealthCheck()

			By(fmt.Sprintf("Selecting a victim and witnesses from the schedulable worker nodes (need at least %d)",
				sbrparams.MinWorkersForBlockSplitBrain))

			victimNodeName, witnessNodes = pickSplitBrainNodes()
			if victimNodeName == "" {
				Skip(fmt.Sprintf("Need at least %d schedulable worker nodes, one of them not hosting an SBR "+
					"controller pod", sbrparams.MinWorkersForBlockSplitBrain))
			}

			GinkgoWriter.Printf("Victim node: %q; witness nodes: %v\n", victimNodeName, witnessNodes)

			By("Verifying iptables is usable in the victim node's host namespaces")

			probePhase, probeLog := runHostCommandPod(victimNodeName, sbrparams.BlockPartitionProbePodName,
				cephRBDIPTablesProbeScript(), sbrparams.BlockPartitionPodTimeout)
			if probePhase != corev1.PodSucceeded {
				Skip(fmt.Sprintf("iptables is not usable in the host namespaces of node %q (probe pod %s): %s",
					victimNodeName, probePhase, probeLog))
			}

			// Set up the Block-mode SBRC from its sub-helpers rather than setupBlockModeSBRC so the init
			// Job can be waited on before the DaemonSet readiness wait: an init failure then surfaces as
			// "init Job failed" instead of an opaque "DaemonSet not ready" timeout.
			failIfSNRRunning()

			By("Discovering the Ceph RBD block storage class")
			rbdStorageClass := discoverCephRBDStorageClass()

			By(fmt.Sprintf("Pre-cleaning any stale SBRC %q", sbrparams.SBRCBlockSplitBrainTestName))
			deleteSBRCAndWait(
				buildSBRC(sbrparams.SBRCBlockSplitBrainTestName, map[string]interface{}{}),
				sbrparams.SBRCBlockSplitBrainTestName)

			By("Creating the Block-mode StorageBasedRemediationConfig on Ceph RBD")
			testSBRC = buildSBRC(sbrparams.SBRCBlockSplitBrainTestName,
				buildBlockModeSBRC(rbdStorageClass, nil))
			Expect(APIClient.Create(context.TODO(), testSBRC)).To(Succeed(),
				"StorageBasedRemediationConfig %q must be created", sbrparams.SBRCBlockSplitBrainTestName)

			By("Waiting for the Block device-init Job to succeed")
			waitForBlockInitJobSucceeded(sbrparams.SBRCBlockSplitBrainTestName, testSBRC.GetUID())

			By("Waiting for the agent DaemonSet to be ready")
			waitForSBRCReady(sbrparams.SBRCBlockSplitBrainTestName)

			By("Asserting the SBRC reports concurrent-writeable shared storage")

			Eventually(func() (bool, error) {
				return sbrcConcurrentWriteable(sbrparams.SBRCBlockSplitBrainTestName)
			}, sbrparams.StorageInjectionTimeout, sbrparams.DefaultPollInterval).Should(BeTrue(),
				"SBRC %q must report status.storageValidation.concurrentWriteable=true before fencing "+
					"can proceed", sbrparams.SBRCBlockSplitBrainTestName)

			By("Selecting an observer agent pod on a witness node to read the shared device through")

			var observerErr error

			for _, witness := range witnessNodes {
				observerPodName, observerErr = agentPodNameByNode(sbrparams.SBRCBlockSplitBrainTestName, witness, true)
				if observerErr == nil {
					break
				}
			}

			Expect(observerErr).ToNot(HaveOccurred(),
				"An agent pod on one of the witness nodes %v must be available to read the shared device",
				witnessNodes)
			GinkgoWriter.Printf("Observer agent pod: %q\n", observerPodName)

			By("Creating the NodeHealthCheck CR that remediates SBRStorageUnhealthy nodes through SBR")

			ensureSBRTemplate()
			// maxUnhealthy: 1 (rather than the shared builder's minHealthy: "51%") so exactly one node
			// may be unhealthy regardless of cluster size — "51%" can block remediation on a small cluster.
			nhc := buildNHCWithMaxUnhealthy(sbrparams.NHCBlockSplitBrainTestName, 1)
			Expect(APIClient.Create(context.TODO(), nhc)).To(Succeed(),
				"NodeHealthCheck CR %q must be created", sbrparams.NHCBlockSplitBrainTestName)
		})

		AfterAll(func() {
			By("Removing the NodeHealthCheck CR")
			cleanupNHCCR(context.Background(), sbrparams.NHCBlockSplitBrainTestName)

			By("Removing the split-brain injector pod if still present")
			deleteHostCommandPod(sbrparams.BlockSplitBrainInjectorPodName)

			if victimNodeName != "" {
				By("Flushing any leftover Ceph REJECT rule on the victim node")
				flushCephRBDRules(victimNodeName)

				By("Uncordoning the victim node if it is still cordoned")
				uncordonNodeIfNeeded(victimNodeName)

				By("Removing any StorageBasedRemediation CR left for the involved nodes")

				for _, nodeName := range append([]string{victimNodeName}, witnessNodes...) {
					cleanupSBRCR(context.Background(), nodeName)
				}
			}

			if testSBRC != nil {
				By(fmt.Sprintf("Removing StorageBasedRemediationConfig %q", sbrparams.SBRCBlockSplitBrainTestName))
				deleteSBRCAndWait(testSBRC, sbrparams.SBRCBlockSplitBrainTestName)
			}
		})

		It("Split-brain: only the storage-isolated Ceph RBD node is fenced; its witnesses are untouched",
			reportxml.ID("1934"),
			Label(
				labels.OperatorSBR,
				labels.TierAcceptance,
				labels.ComponentRemediation,
				labels.DisruptionDestructive,
				labels.PlatformAny,
				labels.FrequencyNightly,
			), func() {
				baseline := captureSplitBrainBaseline(victimNodeName, witnessNodes)

				By(fmt.Sprintf("Starting a privileged injector pod that blocks Ceph ports on the victim "+
					"node %q", victimNodeName))

				startSplitBrainPartition(victimNodeName)
				state := awaitVictimIsolatedWitnessesUntouched(victimNodeName, witnessNodes, observerPodName)

				By("Waiting for NHC to create a StorageBasedRemediation CR for the victim node")

				Eventually(func() error {
					_, pullErr := pullSBRCR(victimNodeName)
					if k8serrors.IsNotFound(pullErr) {
						return fmt.Errorf("StorageBasedRemediation/%s not yet created by NHC", victimNodeName)
					}

					return pullErr
				}, sbrparams.NHCSBRCRCreationTimeout, sbrparams.NHCSBRCRCreationPollInterval).Should(Succeed(),
					"NHC must create a StorageBasedRemediation CR for isolated node %q", victimNodeName)

				// The proof is awaited before the reboot, not after it: once the victim is back and
				// heartbeating, NHC deletes the CR and the reconciler removes the out-of-service taint, so
				// a check that waits out the reboot first can find neither. Death is proven from storage
				// while the victim is still cut off, so the proof always precedes the victim's recovery.
				By("Waiting for FencingSucceeded=True and Succeeded=True on the victim's StorageBasedRemediation CR")

				var provenCR *unstructured.Unstructured

				Eventually(func() error {
					var provenErr error

					provenCR, provenErr = fencingProven(victimNodeName)

					return provenErr
				}, sbrparams.FencingProvenTimeout, sbrparams.DefaultPollInterval).Should(Succeed(),
					"StorageBasedRemediation/%s must reach %s=True and %s=True",
					victimNodeName, sbrparams.FencingSucceededCondition, sbrparams.SucceededCondition)

				By("Asserting the FencingSucceeded transition happened after the victim's last heartbeat")

				assertFencingProvenAfterHeartbeat(provenCR, state)

				By("Asserting the victim node carries the out-of-service taint")

				fencedNode, fencedGetErr := APIClient.CoreV1Interface.Nodes().Get(
					context.TODO(), victimNodeName, metav1.GetOptions{})
				Expect(fencedGetErr).ToNot(HaveOccurred(),
					"Must read node %q to check the %s taint", victimNodeName, sbrparams.OutOfServiceTaintKey)
				Expect(hasOutOfServiceTaint(fencedNode)).To(BeTrue(),
					"Fenced node %q must carry the %s taint", victimNodeName, sbrparams.OutOfServiceTaintKey)

				By("Waiting for the victim node's boot-id to change (confirms it actually rebooted)")

				Eventually(func() (string, error) {
					return getNodeBootID(victimNodeName)
				}, sbrparams.NodeRebootTimeout, sbrparams.NodeRebootPollInterval).ShouldNot(
					Equal(baseline.victimBootID),
					"Isolated node %q must reboot (boot-id must change from %q)",
					victimNodeName, baseline.victimBootID)

				newVictimBootID, bootErr := getNodeBootID(victimNodeName)
				Expect(bootErr).ToNot(HaveOccurred(),
					"Must read the victim node %q boot-id after the reboot", victimNodeName)
				GinkgoWriter.Printf("Victim node %q rebooted: boot-id %q -> %q\n",
					victimNodeName, baseline.victimBootID, newVictimBootID)

				By(fmt.Sprintf("Waiting for the victim node %q to come back Ready", victimNodeName))

				Eventually(func() error {
					return nodeConditionIs(victimNodeName, string(corev1.NodeReady), corev1.ConditionTrue, "")
				}, sbrparams.NodeRebootTimeout, sbrparams.NodeRebootPollInterval).Should(Succeed(),
					"Node %q must return Ready after the fencing reboot", victimNodeName)

				By("Asserting every witness node is Ready, schedulable and keeps its original boot-id")

				assertWitnessesUntouched(witnessNodes, baseline)

				By("Waiting for the victim node's agent pod to be Ready again")

				Eventually(func() error {
					return agentPodReadyOnNode(sbrparams.SBRCBlockSplitBrainTestName, victimNodeName)
				}, sbrparams.BlockFencingStateCheckTimeout,
					sbrparams.BlockFencingStateCheckInterval).Should(Succeed(),
					"The SBRC %q agent pod on node %q must become Ready after it rebooted",
					sbrparams.SBRCBlockSplitBrainTestName, victimNodeName)

				By(fmt.Sprintf("Consistently asserting for %s that the victim is not fenced again "+
					"(stable boot-id and Ready)", sbrparams.BlockNoFencingCheckDuration))

				// The boot-id alone is a weak signal: it only changes once kubelet reports in after a
				// reboot finishes, so a second fence starting inside this window would not show as a
				// boot-id change. Asserting the node also stays Ready catches a re-fence in progress.
				Consistently(func() error {
					bootID, bootIDErr := getNodeBootID(victimNodeName)
					if bootIDErr != nil {
						return bootIDErr
					}

					if bootID != newVictimBootID {
						return fmt.Errorf("node %s rebooted again: boot-id %s -> %s",
							victimNodeName, newVictimBootID, bootID)
					}

					return nodeConditionIs(victimNodeName, string(corev1.NodeReady), corev1.ConditionTrue, "")
				}, sbrparams.BlockNoFencingCheckDuration, sbrparams.BlockNoFencingCheckInterval).Should(Succeed(),
					"Node %q must not be fenced again after recovery (boot-id must stay %q and it must stay Ready)",
					victimNodeName, newVictimBootID)

				By("Waiting for NHC to delete the StorageBasedRemediation CR")

				Eventually(func() error {
					_, pullErr := pullSBRCR(victimNodeName)
					if k8serrors.IsNotFound(pullErr) {
						return nil
					}

					if pullErr != nil {
						return pullErr
					}

					return fmt.Errorf("StorageBasedRemediation/%s still exists; waiting for NHC cleanup", victimNodeName)
				}, sbrparams.SBRCRCleanupTimeout, sbrparams.SBRCRCleanupPollInterval).Should(Succeed(),
					"NHC must delete the StorageBasedRemediation CR for node %q after recovery", victimNodeName)

				By("Asserting the victim node is schedulable again and the out-of-service taint is gone")

				Eventually(func() error {
					node, getErr := APIClient.CoreV1Interface.Nodes().Get(
						context.TODO(), victimNodeName, metav1.GetOptions{})
					if getErr != nil {
						return fmt.Errorf("get node %s: %w", victimNodeName, getErr)
					}

					if !isNodeSchedulable(node) {
						return fmt.Errorf("node %s is not schedulable yet (cordoned or tainted)", victimNodeName)
					}

					if hasOutOfServiceTaint(node) {
						return fmt.Errorf("node %s still carries the %s taint",
							victimNodeName, sbrparams.OutOfServiceTaintKey)
					}

					return nil
				}, sbrparams.NodeRebootTimeout, sbrparams.NodeRebootPollInterval).Should(Succeed(),
					"Node %q must be schedulable with no %s taint after the CR is deleted",
					victimNodeName, sbrparams.OutOfServiceTaintKey)

				GinkgoWriter.Printf("Block-mode split-brain complete: victim %q fenced and recovered; "+
					"witnesses %v untouched\n", victimNodeName, witnessNodes)
			})
	})

// pickSplitBrainNodes returns one victim and every other schedulable worker node as a witness. The
// victim must not host an SBR controller pod, so the fencing reboot does not take the operator down
// mid-test; witnesses may, since a spurious fence on any worker is exactly what the test looks for.
// It returns ("", nil) when fewer than MinWorkersForBlockSplitBrain workers are schedulable or none of
// them can be the victim. The workers are sorted so the victim is chosen deterministically across runs.
func pickSplitBrainNodes() (string, []string) {
	controllerNodes := controllerPodNodes()
	nodeList, err := APIClient.CoreV1Interface.Nodes().List(context.TODO(), metav1.ListOptions{
		LabelSelector: "node-role.kubernetes.io/worker",
	})
	Expect(err).ToNot(HaveOccurred(), "Failed to list worker nodes")

	var schedulable []string

	for nodeIdx := range nodeList.Items {
		if isNodeSchedulable(&nodeList.Items[nodeIdx]) {
			schedulable = append(schedulable, nodeList.Items[nodeIdx].Name)
		}
	}

	if len(schedulable) < sbrparams.MinWorkersForBlockSplitBrain {
		return "", nil
	}

	sort.Strings(schedulable)

	victimIdx := slices.IndexFunc(schedulable, func(name string) bool { return !controllerNodes[name] })
	if victimIdx < 0 {
		return "", nil
	}

	victim := schedulable[victimIdx]

	return victim, slices.Delete(schedulable, victimIdx, victimIdx+1)
}

// captureSplitBrainBaseline records the victim's boot-id and, for every witness, its boot-id, agent
// pod name and sbr_self_fenced_total — the state fencing the isolated victim must leave untouched —
// and asserts the victim starts from a healthy state.
func captureSplitBrainBaseline(victimNodeName string, witnessNodes []string) splitBrainBaseline {
	By("Recording the victim node's boot-id before the partition")

	victimBootID, bootErr := getNodeBootID(victimNodeName)
	Expect(bootErr).ToNot(HaveOccurred(),
		"Must read the boot-id of victim node %q before the partition", victimNodeName)

	By("Asserting SBRStorageUnhealthy is not True on the victim node")

	// Absent, False or Unknown all pass; only True is a failed precondition.
	Expect(transientNodeHasCondition(victimNodeName, sbrparams.SBRStorageUnhealthyCondition, false)).To(Succeed(),
		"Node %q must not report %s=True before its Ceph traffic is blocked",
		victimNodeName, sbrparams.SBRStorageUnhealthyCondition)

	By("Recording every witness node's boot-id and its agent's sbr_self_fenced_total counter and restart count")

	baseline := splitBrainBaseline{
		victimBootID:    victimBootID,
		witnessBootIDs:  make(map[string]string, len(witnessNodes)),
		witnessAgents:   make(map[string]string, len(witnessNodes)),
		witnessFenced:   make(map[string]float64, len(witnessNodes)),
		witnessRestarts: make(map[string]int32, len(witnessNodes)),
	}

	for _, witness := range witnessNodes {
		witnessBootID, witnessBootErr := getNodeBootID(witness)
		Expect(witnessBootErr).ToNot(HaveOccurred(),
			"Must read the boot-id of witness node %q", witness)
		baseline.witnessBootIDs[witness] = witnessBootID
		agentPodName, agentErr := agentPodNameByNode(sbrparams.SBRCBlockSplitBrainTestName, witness, true)
		Expect(agentErr).ToNot(HaveOccurred(), "An SBR agent pod must be Running on witness node %q", witness)
		baseline.witnessAgents[witness] = agentPodName
		selfFenced, metricErr := readAgentCounter(agentPodName, sbrparams.SelfFencedMetricName)
		Expect(metricErr).ToNot(HaveOccurred(),
			"Must read %s from witness agent pod %q", sbrparams.SelfFencedMetricName, agentPodName)
		baseline.witnessFenced[witness] = selfFenced
		restarts, restartErr := agentRestartCount(agentPodName)
		Expect(restartErr).ToNot(HaveOccurred(), "Must read the restart count of witness agent pod %q", agentPodName)
		baseline.witnessRestarts[witness] = restarts
	}

	return baseline
}

// startSplitBrainPartition starts a privileged pod on the victim and blocks its Ceph traffic for the
// whole scenario. The block persists (no self-terminating window) so the victim stays isolated until
// fencing reboots it, which clears the host's iptables rules. The rules are the same RBD REJECT set
// the transient partition test uses, so flushCephRBDRules removes exactly what was inserted.
func startSplitBrainPartition(victimNodeName string) {
	deleteHostCommandPod(sbrparams.BlockSplitBrainInjectorPodName)
	injectorPod, createErr := pod.NewBuilder(
		APIClient, sbrparams.BlockSplitBrainInjectorPodName, medik8sparams.OperatorNs, sbrparams.InjectorImage).
		DefineOnNode(victimNodeName).
		WithHostPid(true).
		WithPrivilegedFlag().
		CreateAndWaitUntilRunning(medik8sparams.DefaultTimeout)
	Expect(createErr).ToNot(HaveOccurred(),
		"Privileged injector pod must start on victim node %q", victimNodeName)

	DeferCleanup(func() {
		By("DeferCleanup: removing the split-brain injector pod and flushing leftover Ceph REJECT rules")
		deleteHostCommandPod(sbrparams.BlockSplitBrainInjectorPodName)
		flushCephRBDRules(victimNodeName)
	})

	injectOut, injectErr := injectorPod.ExecCommand([]string{"sh", "-c", "set -e\n" + cephRBDRuleScript("-I")})
	Expect(injectErr).ToNot(HaveOccurred(),
		"Ceph REJECT rules must be inserted on victim node %q (output: %s)", victimNodeName, injectOut.String())
	GinkgoWriter.Printf("Ceph port REJECT rules applied on victim node %q only\n", victimNodeName)
}

// awaitVictimIsolatedWitnessesUntouched waits for a peer to report the victim storage-unhealthy,
// reads the victim's heartbeat slot through the observer (its node ID and last-written timestamp come
// from the on-device node map, not the Kubernetes API), and then holds for the healthy-node window
// proving no witness is dragged into remediation.
func awaitVictimIsolatedWitnessesUntouched(
	victimNodeName string, witnessNodes []string, observerPodName string,
) splitBrainState {
	By(fmt.Sprintf("Waiting for SBRStorageUnhealthy=True (reason %s) on the victim node",
		sbrparams.SBRStorageUnhealthyReasonHeartbeatTimeout))

	Eventually(func() error {
		return nodeConditionIs(victimNodeName, sbrparams.SBRStorageUnhealthyCondition,
			corev1.ConditionTrue, sbrparams.SBRStorageUnhealthyReasonHeartbeatTimeout)
	}, sbrparams.StorageInjectionTimeout, sbrparams.StorageInjectionPollInterval).Should(Succeed(),
		"A peer must report isolated node %q %s=True (reason %s)",
		victimNodeName, sbrparams.SBRStorageUnhealthyCondition,
		sbrparams.SBRStorageUnhealthyReasonHeartbeatTimeout)

	By("Looking up the victim's heartbeat node ID in the node map and recording its last heartbeat")

	nodeMap, jsonPayload, mapErr := readNodeMapFromPod(observerPodName)
	Expect(mapErr).ToNot(HaveOccurred(),
		"Must read the node map through observer pod %q (payload: %s)", observerPodName, jsonPayload)
	victimEntry, mapped := nodeMap.Entries[victimNodeName]
	Expect(mapped).To(BeTrue(),
		"The node map must hold an entry for victim node %q", victimNodeName)
	heartbeatTS, tsErr := victimHeartbeatTimestamp(observerPodName, victimEntry.NodeID)
	Expect(tsErr).ToNot(HaveOccurred(),
		"Must read the victim's heartbeat slot (node ID %d) through observer pod %q",
		victimEntry.NodeID, observerPodName)
	GinkgoWriter.Printf("Victim node %q owns heartbeat slot node ID %d, last heartbeat at %s\n",
		victimNodeName, victimEntry.NodeID, time.Unix(0, heartbeatTS).UTC().Format(time.RFC3339Nano))

	By(fmt.Sprintf("Consistently asserting for %s that no witness becomes unhealthy or gets an SBR CR",
		sbrparams.SplitBrainHealthyNodeCheckDuration))

	// Runs while NHC decides to act on the victim. If any witness is flagged or remediated during
	// this window the fence landed on the wrong geometry and the test fails immediately.
	Consistently(func() error {
		for _, witness := range witnessNodes {
			if condErr := transientNodeHasCondition(witness,
				sbrparams.SBRStorageUnhealthyCondition, false); condErr != nil {
				return condErr
			}

			_, pullErr := pullSBRCR(witness)
			if pullErr == nil {
				return fmt.Errorf("witness node %q unexpectedly received a StorageBasedRemediation CR "+
					"— split-brain isolation is broken", witness)
			}

			if !k8serrors.IsNotFound(pullErr) {
				return fmt.Errorf("get StorageBasedRemediation/%s: %w", witness, pullErr)
			}
		}

		return nil
	}, sbrparams.SplitBrainHealthyNodeCheckDuration,
		sbrparams.SplitBrainHealthyNodeCheckInterval).Should(Succeed(),
		"No witness node must be flagged unhealthy or remediated while only the victim is isolated")

	return splitBrainState{victimNodeID: victimEntry.NodeID, heartbeatTS: heartbeatTS}
}

// assertFencingProvenAfterHeartbeat checks that the victim's FencingSucceeded condition transitioned
// to True only after the last heartbeat it wrote — death is proven from the heartbeat going stale, so
// the proof cannot predate the silence. It reads the CR snapshot awaitFencingProven returned rather
// than re-pulling, so NHC deleting the CR after recovery cannot race it.
func assertFencingProvenAfterHeartbeat(provenCR *unstructured.Unstructured, state splitBrainState) {
	lastHeartbeat := time.Unix(0, state.heartbeatTS)

	transition, transitionErr := fencingSucceededTransitionTime(provenCR)
	Expect(transitionErr).ToNot(HaveOccurred(),
		"Must read the FencingSucceeded transition time from StorageBasedRemediation/%s", provenCR.GetName())

	Expect(transition.After(lastHeartbeat)).To(BeTrue(),
		"FencingSucceeded for node %q transitioned at %s, which must be after its last heartbeat at %s",
		provenCR.GetName(), transition.UTC().Format(time.RFC3339), lastHeartbeat.UTC().Format(time.RFC3339Nano))
}

// assertWitnessesUntouched proves the fence hit only the victim: every witness is Ready, schedulable,
// still on its original boot-id, and its agent never self-fenced. The agent must also be the same pod
// with the same restart count, because a restarted or replaced agent starts its counter from zero and
// would read as "unchanged".
func assertWitnessesUntouched(witnessNodes []string, baseline splitBrainBaseline) {
	for _, witness := range witnessNodes {
		node, getErr := APIClient.CoreV1Interface.Nodes().Get(context.TODO(), witness, metav1.GetOptions{})
		Expect(getErr).ToNot(HaveOccurred(), "Must read witness node %q for post-fence verification", witness)
		Expect(isNodeSchedulable(node)).To(BeTrue(),
			"Witness node %q must remain schedulable while only the victim is fenced", witness)
		Expect(nodeConditionIs(witness, string(corev1.NodeReady), corev1.ConditionTrue, "")).To(Succeed(),
			"Witness node %q must stay Ready while only the victim is fenced", witness)
		Expect(node.Status.NodeInfo.BootID).To(Equal(baseline.witnessBootIDs[witness]),
			"Witness node %q boot-id must not change (pre=%q, post=%q — it was spuriously fenced)",
			witness, baseline.witnessBootIDs[witness], node.Status.NodeInfo.BootID)
		agentPodName, agentErr := agentPodNameByNode(sbrparams.SBRCBlockSplitBrainTestName, witness, true)
		Expect(agentErr).ToNot(HaveOccurred(), "An SBR agent pod must still be Running on witness node %q", witness)
		Expect(agentPodName).To(Equal(baseline.witnessAgents[witness]),
			"Witness node %q agent pod must not be replaced (pre=%q, post=%q)",
			witness, baseline.witnessAgents[witness], agentPodName)
		restarts, restartErr := agentRestartCount(agentPodName)
		Expect(restartErr).ToNot(HaveOccurred(), "Must read the restart count of witness agent pod %q", agentPodName)
		Expect(restarts).To(Equal(baseline.witnessRestarts[witness]),
			"Witness agent pod %q must not restart: restart count went from %d to %d",
			agentPodName, baseline.witnessRestarts[witness], restarts)
		selfFenced, metricErr := readAgentCounter(baseline.witnessAgents[witness], sbrparams.SelfFencedMetricName)
		Expect(metricErr).ToNot(HaveOccurred(),
			"Must read %s from witness agent pod %q", sbrparams.SelfFencedMetricName, baseline.witnessAgents[witness])
		Expect(selfFenced).To(Equal(baseline.witnessFenced[witness]),
			"Witness agent on node %q must not self-fence: %s went from %v to %v",
			witness, sbrparams.SelfFencedMetricName, baseline.witnessFenced[witness], selfFenced)
	}
}
