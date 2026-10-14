package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/pod"

	. "github.com/medik8s/system-tests/tests/internal/medik8sinittools"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/remotecommand"
)

// discoverCephRBDStorageClass returns a Ceph RBD block storage class name.
// Checks the SBR_RBD_STORAGE_CLASS env var first; falls back to listing StorageClasses
// by provisioner pattern. Skips the test when no Ceph RBD class is found because these
// tests specifically validate block storage behavior on Ceph RBD.
func discoverCephRBDStorageClass() (string, error) {
	if storageClassName := os.Getenv("SBR_RBD_STORAGE_CLASS"); storageClassName != "" {
		obj, getErr := APIClient.StorageV1Interface.StorageClasses().Get(
			context.TODO(), storageClassName, metav1.GetOptions{})
		Expect(getErr).ToNot(HaveOccurred(), "SBR_RBD_STORAGE_CLASS=%q not found", storageClassName)

		if !strings.Contains(obj.Provisioner, "rbd.csi.ceph.com") {
			err := fmt.Errorf("SBR_RBD_STORAGE_CLASS=%q uses provisioner %q (not Ceph RBD); "+
				"block storage tests require a Ceph RBD-backed class", storageClassName, obj.Provisioner)
			return "", err
		}

		return storageClassName, nil
	}

	scList, err := APIClient.StorageV1Interface.StorageClasses().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	for _, sc := range scList.Items {
		if strings.Contains(sc.Provisioner, "rbd.csi.ceph.com") {
			GinkgoWriter.Printf("Auto-discovered Ceph RBD StorageClass: %s (provisioner: %s)\n", sc.Name, sc.Provisioner)
			return sc.Name, nil
		}
	}

	err = fmt.Errorf("No Ceph RBD StorageClass found; install ODF or set SBR_RBD_STORAGE_CLASS env var")

	return "", err
}

// getWorkerNodeCount returns the number of worker nodes in the cluster.
func getWorkerNodeCount() int {
	nodeList, err := APIClient.CoreV1Interface.Nodes().List(
		context.TODO(),
		metav1.ListOptions{LabelSelector: "node-role.kubernetes.io/worker"})
	Expect(err).ToNot(HaveOccurred(), "Failed to list worker nodes")

	return len(nodeList.Items)
}

// checkSNRRunning checks if Self Node Remediation (SNR) DaemonSet pods are running.
// Returns (true, podCount) if SNR pods are running, (false, 0) otherwise.
func checkSNRRunning() (bool, int) {
	snrPodList, err := APIClient.CoreV1Interface.Pods("openshift-workload-availability").List(
		context.TODO(),
		metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=self-node-remediation"})
	if err != nil {
		GinkgoWriter.Printf("Warning: could not check for SNR pods: %v\n", err)

		return false, 0
	}

	runningCount := 0
	for idx := range snrPodList.Items {
		pod := &snrPodList.Items[idx]
		if pod.Status.Phase == corev1.PodRunning {
			runningCount++
		}
	}

	return runningCount > 0, runningCount
}

// sbdHeartbeatMsgType is SBD_MSG_TYPE_HEARTBEAT from internal/sbdprotocol.
const sbdHeartbeatMsgType byte = 0x01

// SlotData represents a single heartbeat slot entry from the SBR device.
type SlotData struct {
	NodeID    uint16
	Timestamp int64
	Sequence  uint64
	HasData   bool
}

// readSlotDataFromPod reads slot data from an SBR agent pod by executing
// dd to read the block device and parsing the heartbeat messages.
func readSlotDataFromPod(podName, namespace string) ([]SlotData, error) {
	// Heartbeat slots live in the heartbeat region, not at the start of the device.
	// Layout (internal/blockformat/superblock.go): heartbeat region starts at
	// BlockHeartbeatRegionOffset (135168) and holds BlockMaxNodes (255) slots of
	// BlockSlotSize (4096) bytes each. 135168 / 4096 == 33, so skip 33 blocks.
	// iflag=direct bypasses the page cache so peer writes are actually observed.
	sbrDevicePath := "/sbr-block"
	cmd := []string{
		"dd", "if=" + sbrDevicePath,
		"bs=4096", "skip=33", "count=255",
		"iflag=direct", "status=none",
	}
	podBuilder, err := pod.Pull(APIClient, podName, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to pull pod %s: %w", podName, err)
	}

	var output bytes.Buffer

	// TTY must stay off: a pseudo-terminal translates every 0x0A byte into 0x0D 0x0A
	// on output, which shifts the raw device bytes and corrupts the slot layout.
	// ExecCommand and ExecCommandWithTimeout both force TTY on, so use the
	// context-based variant that lets the exec options through unchanged.
	err = podBuilder.ExecCommandWithContext(
		context.TODO(),
		corev1.PodExecOptions{
			Container: podBuilder.Definition.Spec.Containers[0].Name,
			Command:   cmd,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		},
		remotecommand.StreamOptions{
			Stdout: &output,
			Stderr: os.Stderr,
			Tty:    false,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to exec dd command in pod %s: %w", podName, err)
	}

	// Parse the raw block device output into slot data
	slots := parseSlotData(output.Bytes())

	return slots, nil
}

// parseSlotData parses raw heartbeat-region bytes into SlotData entries.
//
// Each slot is BlockSlotSize (4096) bytes and starts with a packed 33-byte SBD
// message header, little-endian throughout
// (internal/sbdprotocol/message.go Marshal):
//
//	[0:8]   Magic "SBDMSG01"
//	[8:10]  Version   uint16
//	[10]    Type      byte (0x01 heartbeat, 0x02 fence)
//	[11:13] NodeID    uint16
//	[13:21] Timestamp uint64 (UnixNano)
//	[21:29] Sequence  uint64
//	[29:33] Checksum  uint32 (CRC32 over bytes 0:29)
//
// The slot for node N is at index N-1 in Block mode, so the returned SlotData
// carries the NodeID read from the payload rather than the slot index.
func parseSlotData(data []byte) []SlotData {
	const (
		slotSize   = 4096
		headerSize = 33
		maxNodes   = 255
	)

	sbdMagic := []byte("SBDMSG01")

	var slots []SlotData

	for i := 0; i < len(data)/slotSize && i < maxNodes; i++ {
		slotBytes := data[i*slotSize : (i+1)*slotSize][:headerSize]

		// A slot only counts as written if it carries a valid heartbeat header.
		if !bytes.Equal(slotBytes[0:8], sbdMagic) {
			continue
		}

		if slotBytes[10] != sbdHeartbeatMsgType {
			continue
		}

		want := crc32.ChecksumIEEE(slotBytes[0:29])
		if got := binary.LittleEndian.Uint32(slotBytes[29:33]); got != want {
			GinkgoWriter.Printf("  Slot %d: checksum mismatch (got %d, want %d), skipping\n", i, got, want)

			continue
		}

		slots = append(slots, SlotData{
			NodeID:    binary.LittleEndian.Uint16(slotBytes[11:13]),
			Timestamp: int64(binary.LittleEndian.Uint64(slotBytes[13:21])),
			Sequence:  binary.LittleEndian.Uint64(slotBytes[21:29]),
			HasData:   true,
		})
	}

	return slots
}

// validateSlotDataConsistency compares slot data across all pods to ensure
// they all see the same block device content (data integrity).
func validateSlotDataConsistency(slotDataByPod map[string][]SlotData) error {
	if len(slotDataByPod) == 0 {
		return fmt.Errorf("No slot data collected from any pods")
	}

	// Get reference slot count from first pod
	var referencePodName string
	var referenceSlotCount int

	for podName, slots := range slotDataByPod {
		referencePodName = podName
		referenceSlotCount = len(slots)

		break
	}

	GinkgoWriter.Printf("Using pod %s as reference with %d slots\n", referencePodName, referenceSlotCount)

	// Compare slot counts across all pods
	totalSlots := 0
	minSlots := referenceSlotCount
	maxSlots := referenceSlotCount

	for podName, slots := range slotDataByPod {
		slotCount := len(slots)
		totalSlots += slotCount

		if slotCount < minSlots {
			minSlots = slotCount
		}
		if slotCount > maxSlots {
			maxSlots = slotCount
		}

		GinkgoWriter.Printf("  Pod %s: %d slots with data\n", podName, slotCount)
	}

	// Allow some variance due to timing (agents writing at different times)
	// but slots should be relatively consistent
	variance := maxSlots - minSlots
	maxAllowedVariance := 5 // Allow up to 5 slot difference

	if variance > maxAllowedVariance {
		return fmt.Errorf("Slot count variance too high (%d). Min=%d, Max=%d. "+
			"This suggests inconsistent block device reads across pods.",
			variance, minSlots, maxSlots)
	}
	GinkgoWriter.Printf("✓ Slot counts are consistent across pods (variance: %d, max allowed: %d)\n",
		variance, maxAllowedVariance)
	return nil
}

// validateSequenceNumbers checks that sequence numbers are incrementing
// correctly and there are no conflicts between different agents.
func validateSequenceNumbers(slotDataByPod map[string][]SlotData) error {
	// Collect all slots with sequence numbers across all pods
	type sequenceEntry struct {
		PodName  string
		NodeID   uint16
		Sequence uint64
	}

	var allSequences []sequenceEntry

	for podName, slots := range slotDataByPod {
		for _, slot := range slots {
			if slot.HasData && slot.Sequence > 0 {
				allSequences = append(allSequences, sequenceEntry{
					PodName:  podName,
					NodeID:   slot.NodeID,
					Sequence: slot.Sequence,
				})
			}
		}
	}

	if len(allSequences) == 0 {
		GinkgoWriter.Printf("⚠ No sequence numbers found in slot data (agents may still be initializing)\n")

		return nil
	}

	GinkgoWriter.Printf("Found %d slot entries with sequence numbers\n", len(allSequences))

	// Group by NodeID and check sequence progression
	sequencesByNode := make(map[uint16][]uint64)

	for _, entry := range allSequences {
		sequencesByNode[entry.NodeID] = append(sequencesByNode[entry.NodeID], entry.Sequence)
	}

	GinkgoWriter.Printf("Detected %d unique node IDs writing heartbeats\n", len(sequencesByNode))

	// For each node, sequences should be incrementing
	for nodeID, sequences := range sequencesByNode {
		if len(sequences) == 0 {
			continue
		}

		// Check that we have incrementing sequences (allowing for some reads to miss recent writes)
		minSeq := sequences[0]
		maxSeq := sequences[0]

		for _, seq := range sequences {
			if seq < minSeq {
				minSeq = seq
			}
			if seq > maxSeq {
				maxSeq = seq
			}
		}

		GinkgoWriter.Printf("  Node ID %d: sequence range [%d - %d], samples: %d\n",
			nodeID, minSeq, maxSeq, len(sequences))

		// Sequences should be incrementing (max > min unless only 1 sample)
		if len(sequences) > 1 && maxSeq <= minSeq {
			return fmt.Errorf("Sequences for node %d should be incrementing", nodeID)
		}
	}

	// Verify we see heartbeats from multiple nodes (proves concurrent writes)
	if len(sequencesByNode) < 2 {
		return fmt.Errorf("Expected heartbeats from at least 2 different nodes, but found %d",
			len(sequencesByNode))
	}
	GinkgoWriter.Printf("✓ Sequence numbers are incrementing correctly\n")
	GinkgoWriter.Printf("✓ Detected concurrent writes from %d different nodes\n", len(sequencesByNode))
	return nil
}
