package tests

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/pod"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/reportxml"

	"github.com/medik8s/system-tests/tests/internal/helpers"
	"github.com/medik8s/system-tests/tests/internal/labels"
	. "github.com/medik8s/system-tests/tests/internal/medik8sinittools"
	"github.com/medik8s/system-tests/tests/internal/medik8sparams"
	"github.com/medik8s/system-tests/tests/sbr-operator/internal/sbrparams"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var _ = Describe(
	"SBR Block Storage — Ceph RBD",
	Ordered,
	ContinueOnFailure,
	Label(labels.OperatorSBR), func() {
		var testSBRC *unstructured.Unstructured

		BeforeAll(func() {
			testSBRC = setupBlockModeSBRC(sbrparams.SBRCBlockTestName)
		})

		AfterAll(func() {
			if testSBRC != nil {
				By(fmt.Sprintf("Removing StorageBasedRemediationConfig %q", sbrparams.SBRCBlockTestName))
				deleteSBRCAndWait(testSBRC, sbrparams.SBRCBlockTestName)
			}
		})

		It("Verify SBR agents write concurrently to Ceph RBD block device without conflicts",
			reportxml.ID("1915"),
			Label(
				labels.OperatorSBR,
				labels.TierAcceptance,
				labels.ComponentDaemonSet,
				labels.DisruptionNonDestructive,
				labels.PlatformAny,
				labels.FrequencyNightly,
			), func() {
				testCephRBDConcurrentWrites()
			})
	})

// testCephRBDConcurrentWrites validates that multiple SBR agents can write
// concurrently to a Ceph RBD block device without conflicts.
func testCephRBDConcurrentWrites() {
	By("Verifying all agent pods reach Ready state")
	var agentPods []*pod.Builder

	Eventually(func() error {
		var err error
		agentPods, err = pod.List(APIClient, medik8sparams.OperatorNs,
			metav1.ListOptions{LabelSelector: sbrparams.AgentPodLabelSelector})
		if err != nil {
			return err
		}

		runningPods := helpers.FilterRunningPods(agentPods)
		readyCount := len(runningPods)
		expectedCount := getWorkerNodeCount()

		if readyCount < expectedCount {
			return fmt.Errorf("only %d/%d agents ready", readyCount, expectedCount)
		}

		return nil
	}, medik8sparams.DefaultTimeout, sbrparams.DefaultPollInterval).Should(Succeed(),
		"All agent pods must reach Ready state")

	By("Validating agent pod count matches worker node count")
	runningPods := helpers.FilterRunningPods(agentPods)
	match, actualCount, expectedCount, err := agentPodCountMatchesWorkerCount()
	Expect(err).ToNot(HaveOccurred(), "Failed to compare agent pod count to worker node count")
	Expect(match).To(BeTrue(),
		"Agent pod count (%d) should match worker node count (%d)", actualCount, expectedCount)
	GinkgoWriter.Printf("Successfully verified %d agent pods running on %d worker nodes\n",
		actualCount, expectedCount)
	GinkgoWriter.Printf("Agent pods are using Ceph RBD block storage in Block volume mode\n")

	By("Confirming the cluster has at least two agents to exercise multi-attach")
	// Agent health is already asserted above (pod count equals worker count). This is a
	// precondition on the cluster, not a health check: there must be more than one concurrent
	// writer for the multi-attach assertions below to mean anything, since a single-node cluster
	// would pass every check without ever proving shared access.
	Expect(actualCount).To(BeNumerically(">=", 2),
		"At least 2 agents must be Ready to validate multi-attach capability")
	GinkgoWriter.Printf("✓ Validated concurrent block device access: %d agents Ready\n", actualCount)
	GinkgoWriter.Printf("✓ Ceph RBD block storage supports true RWX multi-attach\n")

	By("Reading the heartbeat region through every agent pod")

	firstRead, readErr := readSlotDataFromAllPods(runningPods)
	Expect(readErr).ToNot(HaveOccurred(), "Must read the heartbeat region through every agent pod")

	By("Comparing slot data across all agent pods for consistency")
	Expect(validateSlotDataConsistency(firstRead)).To(Succeed())

	By("Re-reading until every node's heartbeat sequence has advanced")
	// The read above is a single moment in time: every pod sees the same slots, so it cannot show
	// whether anything is still being written. The agents write a heartbeat every
	// sbrTimeoutSeconds/2 (15s on the default 30s timeout), so a second read taken a little later
	// must sit ahead of the first for every node that is alive. Retrying here is what gives the
	// agents that time, without sleeping for a fixed interval.
	Eventually(func() error {
		secondRead, err := readSlotDataFromAllPods(runningPods)
		if err != nil {
			return err
		}

		return validateSequenceNumbers([]map[string][]SlotData{firstRead, secondRead}, expectedCount)
	}, medik8sparams.DefaultTimeout, sbrparams.DefaultPollInterval).Should(Succeed(),
		"Every node's heartbeat sequence must advance between two reads of the device")

	GinkgoWriter.Printf("✓ Block device maintains data integrity across all agents\n")
	GinkgoWriter.Printf("✓ Sequence numbers increment correctly without conflicts\n")
}

// readSlotDataFromAllPods reads the whole heartbeat region through each of the given agent pods
// and returns one set of parsed slots per pod, keyed by pod name. This is one round of reading
// the device: every pod in it sees the same slot contents, give or take the moment of the read.
func readSlotDataFromAllPods(agentPods []*pod.Builder) (map[string][]SlotData, error) {
	slotDataByPod := make(map[string][]SlotData, len(agentPods))

	for _, agentPod := range agentPods {
		podName := agentPod.Object.Name
		slotData, err := readSlotDataFromPod(podName)
		if err != nil {
			return nil, fmt.Errorf("read slot data from pod %s: %w", podName, err)
		}

		slotDataByPod[podName] = slotData
		GinkgoWriter.Printf("  Read %d slot entries from pod %s\n", len(slotData), podName)
	}

	return slotDataByPod, nil
}
