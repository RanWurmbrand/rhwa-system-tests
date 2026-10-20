package tests

import (
	"context"
	"fmt"

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
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe(
	"SBR Block Storage — Ceph RBD Node Hang (NHC-driven remediation)",
	Ordered,
	ContinueOnFailure,
	Label(labels.OperatorSBR, labels.ComponentRemediation), func() {
		var (
			testSBRC       *unstructured.Unstructured
			victimNodeName string
		)

		BeforeAll(func() {
			By("Checking the NodeHealthCheck CRD is installed")

			if !isNHCCRDInstalled() {
				Skip("NodeHealthCheck CRD not found; NHC operator not installed — skipping block node hang test")
			}

			// Remove this test's own NodeHealthCheck from a crashed prior run first: it references the
			// SBR template, so requireNoSBRNodeHealthCheck would otherwise see it and skip the test.
			By("Removing any NodeHealthCheck left by a previous run of this test")
			deleteNHCAndWait(sbrparams.NHCBlockNodeHangTestName)

			By("Verifying no other NodeHealthCheck already remediates through the SBR template")
			requireNoSBRNodeHealthCheck()

			// Set up the Block-mode SBRC from its sub-helpers rather than setupBlockModeSBRC so the
			// init Job can be waited on before the DaemonSet readiness wait: an init failure then
			// surfaces as "init Job failed" instead of an opaque "DaemonSet not ready" timeout.
			failIfSNRRunning()

			By("Discovering the Ceph RBD block storage class")
			rbdStorageClass := discoverCephRBDStorageClass()

			By(fmt.Sprintf("Pre-cleaning any stale SBRC %q", sbrparams.SBRCBlockNodeHangTestName))
			deleteSBRCAndWait(
				buildSBRC(sbrparams.SBRCBlockNodeHangTestName, map[string]interface{}{}),
				sbrparams.SBRCBlockNodeHangTestName)

			By("Creating the Block-mode StorageBasedRemediationConfig on Ceph RBD")
			testSBRC = buildSBRC(sbrparams.SBRCBlockNodeHangTestName,
				buildBlockModeSBRC(rbdStorageClass, nil))
			Expect(APIClient.Create(context.TODO(), testSBRC)).To(Succeed(),
				"StorageBasedRemediationConfig %q must be created", sbrparams.SBRCBlockNodeHangTestName)

			By("Waiting for the Block device-init Job to succeed")
			waitForBlockInitJobSucceeded(sbrparams.SBRCBlockNodeHangTestName, testSBRC.GetUID())

			By("Waiting for the agent DaemonSet to be ready")
			waitForSBRCReady(sbrparams.SBRCBlockNodeHangTestName)

			By("Asserting the SBRC reports concurrent-writeable shared storage")
			assertSBRCConcurrentWriteable(sbrparams.SBRCBlockNodeHangTestName)

			By("Creating the NodeHealthCheck CR that remediates SBRStorageUnhealthy nodes through SBR")

			ensureSBRTemplate()
			// maxUnhealthy: 1 (rather than the shared builder's minHealthy: "51%") so exactly one node
			// may be unhealthy regardless of cluster size — "51%" can block remediation on a small
			// cluster.
			nhc := buildNHCWithMaxUnhealthy(sbrparams.NHCBlockNodeHangTestName, 1)
			Expect(APIClient.Create(context.TODO(), nhc)).To(Succeed(),
				"NodeHealthCheck CR %q must be created", sbrparams.NHCBlockNodeHangTestName)

			By("Selecting a victim worker node that hosts no SBR controller pod")

			victimNodeName = pickTargetWorkerNode()
			if victimNodeName == "" {
				Skip("No schedulable worker node found that does not host an SBR controller pod")
			}

			GinkgoWriter.Printf("Victim node for the Ceph RBD node hang: %q\n", victimNodeName)
		})

		AfterAll(func() {
			By("Removing the NodeHealthCheck CR")
			cleanupNHCCR(context.Background(), sbrparams.NHCBlockNodeHangTestName)

			if victimNodeName != "" {
				By("Removing the injector pod if still present")
				deleteHostCommandPod(sbrparams.BlockNodeHangInjectorPodName)

				By("Removing any StorageBasedRemediation CR left for the victim node")
				cleanupSBRCR(context.Background(), victimNodeName)

				By("Uncordoning the victim node if it is still cordoned")
				uncordonNodeIfNeeded(victimNodeName)
			}

			if testSBRC != nil {
				By(fmt.Sprintf("Removing StorageBasedRemediationConfig %q", sbrparams.SBRCBlockNodeHangTestName))
				deleteSBRCAndWait(testSBRC, sbrparams.SBRCBlockNodeHangTestName)
			}
		})

		It("Node hang: kernel panic on a Ceph RBD block-mode node triggers NHC fencing and recovery",
			reportxml.ID("1931"),
			Label(
				labels.OperatorSBR,
				labels.TierAcceptance,
				labels.ComponentRemediation,
				labels.DisruptionDestructive,
				labels.PlatformAny,
				labels.FrequencyNightly,
			), func() {
				By("Recording the victim node's boot-id before the kernel panic")

				preCrashBootID, bootErr := getNodeBootID(victimNodeName)
				Expect(bootErr).ToNot(HaveOccurred(),
					"Must read the boot-id of node %q before the kernel panic", victimNodeName)
				GinkgoWriter.Printf("Pre-crash boot-id: %s\n", preCrashBootID)

				By("Asserting SBRStorageUnhealthy is not True on the victim node")

				// "not True" per the plan: absent, False or Unknown all pass. Unknown is a normal
				// state for a node whose condition recently cycled, so it must not fail the precondition.
				// A failed API read is surfaced, not silently treated as "not True".
				victimNode, victimGetErr := APIClient.CoreV1Interface.Nodes().Get(
					context.TODO(), victimNodeName, metav1.GetOptions{})
				Expect(victimGetErr).ToNot(HaveOccurred(), "Must read node %q before the kernel panic", victimNodeName)

				for _, cond := range victimNode.Status.Conditions {
					if string(cond.Type) == sbrparams.SBRStorageUnhealthyCondition {
						Expect(cond.Status).ToNot(Equal(corev1.ConditionTrue),
							"Node %q must not report %s=True before the kernel panic",
							victimNodeName, sbrparams.SBRStorageUnhealthyCondition)
					}
				}
				triggerKernelPanic(victimNodeName)

				By(fmt.Sprintf("Waiting for SBRStorageUnhealthy=True (reason %s) on the victim node",
					sbrparams.SBRStorageUnhealthyReasonHeartbeatTimeout))

				Eventually(func() error {
					return nodeConditionIs(victimNodeName, sbrparams.SBRStorageUnhealthyCondition,
						corev1.ConditionTrue, sbrparams.SBRStorageUnhealthyReasonHeartbeatTimeout)
				}, sbrparams.StorageInjectionTimeout, sbrparams.StorageInjectionPollInterval).Should(Succeed(),
					"A peer must report node %q %s=True (reason %s) after its kernel panic",
					victimNodeName, sbrparams.SBRStorageUnhealthyCondition,
					sbrparams.SBRStorageUnhealthyReasonHeartbeatTimeout)

				By("Waiting for NHC to create a StorageBasedRemediation CR for the victim node")

				Eventually(func() error {
					_, pullErr := pullSBRCR(victimNodeName)
					if k8serrors.IsNotFound(pullErr) {
						return fmt.Errorf("StorageBasedRemediation/%s not yet created by NHC", victimNodeName)
					}

					return pullErr
				}, sbrparams.NHCSBRCRCreationTimeout, sbrparams.NHCSBRCRCreationPollInterval).Should(Succeed(),
					"NHC must create a StorageBasedRemediation CR for node %q", victimNodeName)

				GinkgoWriter.Printf("StorageBasedRemediation/%s created by NHC\n", victimNodeName)

				By("Waiting for FencingSucceeded=True and Succeeded=True on the victim's StorageBasedRemediation CR")

				awaitFencingProven(victimNodeName)

				By("Asserting the victim node carries the out-of-service taint")

				Expect(nodeHasTaint(victimNodeName, sbrparams.OutOfServiceTaintKey)).To(BeTrue(),
					"Node %q must carry the %s taint after fencing is proven",
					victimNodeName, sbrparams.OutOfServiceTaintKey)

				By(fmt.Sprintf("Waiting for the victim node %q to come back Ready", victimNodeName))

				Eventually(func() error {
					return nodeConditionIs(victimNodeName, string(corev1.NodeReady), corev1.ConditionTrue, "")
				}, sbrparams.NodeRebootTimeout, sbrparams.NodeRebootPollInterval).Should(Succeed(),
					"Node %q must return Ready after the fencing reboot", victimNodeName)

				By("Asserting the victim node's boot-id changed (the node actually rebooted)")

				postCrashBootID, postBootErr := getNodeBootID(victimNodeName)
				Expect(postBootErr).ToNot(HaveOccurred(),
					"Must read the boot-id of node %q after the reboot", victimNodeName)
				Expect(postCrashBootID).ToNot(Equal(preCrashBootID),
					"Boot-id of node %q must change after the reboot (stayed %q)", victimNodeName, preCrashBootID)
				GinkgoWriter.Printf("Boot-id changed: %q -> %q\n", preCrashBootID, postCrashBootID)

				By("Waiting for the victim node's agent pod to be Ready again")

				awaitAgentPodReady(sbrparams.SBRCBlockNodeHangTestName, victimNodeName)

				By(fmt.Sprintf("Consistently asserting for %s that the victim is not fenced again "+
					"(stable boot-id and Ready)", sbrparams.BlockNoFencingCheckDuration))

				// The boot-id alone is a weak signal: it only changes once kubelet reports in after a
				// reboot finishes, which usually takes longer than this window, so a second fence that
				// starts inside the window would not show as a boot-id change. Asserting the node also
				// stays Ready catches a re-fence in progress.
				Consistently(func() error {
					bootID, bootIDErr := getNodeBootID(victimNodeName)
					if bootIDErr != nil {
						return bootIDErr
					}

					if bootID != postCrashBootID {
						return fmt.Errorf("node %s rebooted again: boot-id %s -> %s",
							victimNodeName, postCrashBootID, bootID)
					}

					return nodeConditionIs(victimNodeName, string(corev1.NodeReady), corev1.ConditionTrue, "")
				}, sbrparams.BlockNoFencingCheckDuration, sbrparams.BlockNoFencingCheckInterval).Should(Succeed(),
					"Node %q must not be fenced again after recovery (boot-id must stay %q and it must stay Ready)",
					victimNodeName, postCrashBootID)

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

					if nodeTaintPresent(node, sbrparams.OutOfServiceTaintKey) {
						return fmt.Errorf("node %s still carries the %s taint",
							victimNodeName, sbrparams.OutOfServiceTaintKey)
					}

					return nil
				}, sbrparams.NodeRebootTimeout, sbrparams.NodeRebootPollInterval).Should(Succeed(),
					"Node %q must be schedulable with no %s taint after the CR is deleted",
					victimNodeName, sbrparams.OutOfServiceTaintKey)

				GinkgoWriter.Printf("Block-mode node hang remediation complete for node %q\n", victimNodeName)
			})
	})

// assertSBRCConcurrentWriteable waits until the SBRC reports
// status.storageValidation.concurrentWriteable=true, the gate the reconciler reads before it will
// fence anything. The operator sets it once min(2, desired) agents are Ready; the DaemonSet is already
// ready by this point, but the status write can lag slightly, so this polls rather than reads once.
func assertSBRCConcurrentWriteable(sbrcName string) {
	Eventually(func() (bool, error) {
		sbrc := buildSBRC(sbrcName, map[string]interface{}{})
		if getErr := APIClient.Get(context.TODO(),
			types.NamespacedName{Name: sbrcName, Namespace: medik8sparams.OperatorNs}, sbrc); getErr != nil {
			return false, fmt.Errorf("get SBRC %s: %w", sbrcName, getErr)
		}

		writeable, _, nestedErr := unstructured.NestedBool(
			sbrc.Object, "status", "storageValidation", "concurrentWriteable")

		return writeable, nestedErr
	}, sbrparams.StorageInjectionTimeout, sbrparams.DefaultPollInterval).Should(BeTrue(),
		"SBRC %q must report status.storageValidation.concurrentWriteable=true before fencing can proceed",
		sbrcName)
}

// triggerKernelPanic starts a privileged host-PID pod on nodeName and crashes the node through the
// SysRq trigger. The exec is fire-and-forget: the node reboots immediately, so the command never
// returns cleanly. The pod is removed in AfterAll; the panic clears any state it leaves behind.
func triggerKernelPanic(nodeName string) {
	By(fmt.Sprintf("Starting a privileged injector pod on node %q", nodeName))

	deleteHostCommandPod(sbrparams.BlockNodeHangInjectorPodName)
	injectorPod, createErr := pod.NewBuilder(
		APIClient, sbrparams.BlockNodeHangInjectorPodName, medik8sparams.OperatorNs, sbrparams.InjectorImage).
		DefineOnNode(nodeName).
		WithHostPid(true).
		WithPrivilegedFlag().
		CreateAndWaitUntilRunning(medik8sparams.DefaultTimeout)
	Expect(createErr).ToNot(HaveOccurred(),
		"Privileged injector pod must start on node %q", nodeName)

	By(fmt.Sprintf("Probing that nsenter into PID 1 works and /proc/sysrq-trigger is writable on node %q",
		nodeName))

	// This test enables SysRq itself (echo 1 > /proc/sys/kernel/sysrq), so the current enabled value
	// does not matter — what must hold is that we can enter the host namespaces and write the trigger.
	// A failure here is a real inability to crash the node, so fail rather than skip: a silent skip
	// would hide that the node-hang path was never exercised.
	probeOut, probeErr := injectorPod.ExecCommand([]string{
		"nsenter", "-t", "1", "--pid", "--mount", "--", "sh", "-c",
		"test -w /proc/sysrq-trigger",
	})
	Expect(probeErr).ToNot(HaveOccurred(),
		"nsenter into PID 1 and a writable /proc/sysrq-trigger are required to kernel-panic node %q "+
			"(probe output: %s)", nodeName, probeOut.String())

	By(fmt.Sprintf("Triggering a kernel panic on node %q via /proc/sysrq-trigger", nodeName))
	GinkgoWriter.Printf("Executing SysRq kernel panic on node %s\n", nodeName)

	// Fire-and-forget: the node reboots immediately, so this exec is expected to fail.
	_, _ = injectorPod.ExecCommand([]string{
		"nsenter", "-t", "1", "--pid", "--mount", "--", "sh", "-c",
		"echo 1 > /proc/sys/kernel/sysrq && echo c > /proc/sysrq-trigger",
	})
}

// awaitFencingProven waits until the victim's StorageBasedRemediation CR reports both
// FencingSucceeded=True and Succeeded=True. The CR is looked up by node name on every poll because it
// can be deleted and recreated mid-remediation when the node's SBRStorageUnhealthy condition cycles to
// Unknown; a transient NotFound is therefore just "keep waiting". The CR persists after the conditions
// are set (NHC only deletes it once the node recovers), so there is no "proven then deleted" race to
// special-case here.
func awaitFencingProven(nodeName string) {
	Eventually(func() error {
		sbrCR, pullErr := pullSBRCR(nodeName)
		if pullErr != nil {
			if k8serrors.IsNotFound(pullErr) {
				return fmt.Errorf("StorageBasedRemediation/%s not present yet "+
					"(it may be recreated as the node's condition cycles)", nodeName)
			}

			return fmt.Errorf("get StorageBasedRemediation/%s: %w", nodeName, pullErr)
		}

		if conditionTrue(sbrCR, sbrparams.FencingSucceededCondition) &&
			conditionTrue(sbrCR, sbrparams.SucceededCondition) {
			return nil
		}

		return fmt.Errorf("StorageBasedRemediation/%s: %s and %s are not both True yet",
			nodeName, sbrparams.FencingSucceededCondition, sbrparams.SucceededCondition)
	}, sbrparams.FencingProvenTimeout, sbrparams.DefaultPollInterval).Should(Succeed(),
		"StorageBasedRemediation/%s must reach %s=True and %s=True",
		nodeName, sbrparams.FencingSucceededCondition, sbrparams.SucceededCondition)
}

// conditionTrue reports whether the named status condition on an SBR CR is present and True.
func conditionTrue(sbrCR *unstructured.Unstructured, condType string) bool {
	cond := getSBRCRCondition(sbrCR, condType)
	if cond == nil {
		return false
	}

	status, _, _ := unstructured.NestedString(cond, "status")

	return status == string(corev1.ConditionTrue)
}

// awaitAgentPodReady waits for the SBRC's own agent pod on nodeName to report Ready again after the
// reboot. agentPodNameByNode restricts the match to this SBRC's Running agents, and the Ready
// condition is what matters: it is gated on the agent's preflight-ok file, so Ready means the block
// write test passed after the node came back. A missing Ready condition counts as not ready.
func awaitAgentPodReady(sbrcName, nodeName string) {
	Eventually(func() error {
		podName, nameErr := agentPodNameByNode(sbrcName, nodeName, true)
		if nameErr != nil {
			// No Running agent on the node yet: expected while it is still rebooting.
			return nameErr
		}

		agentPod, pullErr := pod.Pull(APIClient, podName, medik8sparams.OperatorNs)
		if pullErr != nil {
			return fmt.Errorf("pull agent pod %s: %w", podName, pullErr)
		}

		for _, cond := range agentPod.Object.Status.Conditions {
			if cond.Type == corev1.PodReady {
				if cond.Status == corev1.ConditionTrue {
					return nil
				}

				return fmt.Errorf("agent pod %s is not Ready (status=%s)", podName, cond.Status)
			}
		}

		return fmt.Errorf("agent pod %s has no Ready condition yet", podName)
	}, sbrparams.BlockFencingStateCheckTimeout, sbrparams.BlockFencingStateCheckInterval).Should(Succeed(),
		"The SBRC %q agent pod on node %q must become Ready after it rebooted", sbrcName, nodeName)
}

// nodeHasTaint reports whether nodeName currently carries a taint with the given key.
func nodeHasTaint(nodeName, taintKey string) bool {
	node, getErr := APIClient.CoreV1Interface.Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	if getErr != nil {
		GinkgoWriter.Printf("Warning: get node %s to check taint %s: %v\n", nodeName, taintKey, getErr)

		return false
	}

	return nodeTaintPresent(node, taintKey)
}

// nodeTaintPresent reports whether the node object carries a taint with the given key.
func nodeTaintPresent(node *corev1.Node, taintKey string) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Key == taintKey {
			return true
		}
	}

	return false
}

// uncordonNodeIfNeeded clears the unschedulable flag on a node that is still cordoned. Failures are
// logged rather than asserted so teardown never masks the test result.
func uncordonNodeIfNeeded(nodeName string) {
	node, getErr := APIClient.CoreV1Interface.Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	if getErr != nil {
		GinkgoWriter.Printf("Warning: get node %s to uncordon: %v\n", nodeName, getErr)

		return
	}

	if !node.Spec.Unschedulable {
		return
	}

	patch := []byte(`{"spec":{"unschedulable":false}}`)
	if _, patchErr := APIClient.CoreV1Interface.Nodes().Patch(
		context.TODO(), nodeName, types.MergePatchType, patch, metav1.PatchOptions{}); patchErr != nil {
		GinkgoWriter.Printf("Warning: uncordon node %s: %v\n", nodeName, patchErr)
	}
}
