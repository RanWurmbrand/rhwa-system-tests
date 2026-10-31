package tests

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/deployment"
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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

const (
	ghostPeerTestLabel = "test.medik8s.io/ghost-peer"
	// Filesystem slot offsets are nodeID * 512, unlike the block-mode layout.
	preflightSlotSize = 512
)

var _ = Describe("SBR startup preflight heartbeat", Ordered, Serial, func() {
	var storageClass string

	BeforeAll(func() {
		By("Verifying SBR is Ready")
		controller, err := deployment.Pull(APIClient, sbrparams.OperatorDeploymentName, medik8sparams.OperatorNs)
		Expect(err).ToNot(HaveOccurred(), "SBR controller must exist")
		Expect(controller.IsReady(medik8sparams.DefaultTimeout)).To(BeTrue(), "SBR controller must be Ready")
		images := make([]string, 0, len(controller.Object.Spec.Template.Spec.Containers))
		for _, container := range controller.Object.Spec.Template.Spec.Containers {
			images = append(images, container.Image)
		}
		AddReportEntry("SBR controller images", images)
		requireNoSBRNodeHealthCheck()
		requireStoppedSNRForGhostPeer()
		removeStaleGhostPeerConfigs()

		By("Checking worker and RWX storage prerequisites")
		nodes, err := APIClient.CoreV1Interface.Nodes().List(context.TODO(),
			metav1.ListOptions{LabelSelector: "node-role.kubernetes.io/worker"})
		Expect(err).ToNot(HaveOccurred(), "Worker nodes must be readable")
		readyWorkers := 0
		for idx := range nodes.Items {
			if isNodeSchedulable(&nodes.Items[idx]) {
				readyWorkers++
			}
		}
		if readyWorkers < 2 {
			Skip("Ghost peer regression requires at least two Ready, schedulable workers")
		}
		storageClass = discoverRWXStorageClass()
	})

	It("Verify SBR startup clears the unassigned preflight slot",
		reportxml.ID("1964"),
		Label(
			labels.OperatorSBR,
			labels.TierAcceptance,
			labels.ComponentDaemonSet,
			labels.DisruptionNonDestructive,
			labels.PlatformAny,
			labels.FrequencyNightly,
			labels.TopologyMinimalWorker,
		), func() {
			By("Creating a filesystem SBRC and waiting for all agents")
			config := buildSBRC("", map[string]interface{}{"sharedStorageClass": storageClass})
			config.SetGenerateName("test-sbrc-ghost-peer-")
			config.SetLabels(map[string]string{ghostPeerTestLabel: "true"})
			created, err := ghostPeerConfigs().Create(context.TODO(), config, metav1.CreateOptions{})
			Expect(err).ToNot(HaveOccurred(), "Test SBRC must be created")
			DeferCleanup(func() {
				By("Removing the test configuration and waiting for its operands")
				cleanupGhostPeerConfig(created)
			})
			waitForSBRCReady(created.GetName())
			agents, err := ghostPeerAgents(created.GetName())
			Expect(err).ToNot(HaveOccurred(), "Every scheduled SBR agent must be Ready")

			By("Verifying slot 1 has no real owner in the shared node map")
			verifyGhostPeerNodeMap(agents)

			By("Observing the unused slot and agent stability for one minute")
			Consistently(func() error {
				return checkGhostPeerStability(created.GetName(), agents)
			}, sbrparams.GhostPeerObservationDuration, sbrparams.DefaultPollInterval).Should(Succeed(),
				"SBRC %q agents must stay Ready without restarts and read an empty slot 1", created.GetName())
			AddReportEntry("Unused filesystem slot 1", "512 zero bytes through every agent for one minute")
		})
})

func ghostPeerConfigs() dynamic.ResourceInterface {
	return APIClient.Resource(schema.GroupVersionResource{
		Group: sbrparams.CRDGroup, Version: sbrparams.CRDVersion,
		Resource: "storagebasedremediationconfigs",
	}).Namespace(medik8sparams.OperatorNs)
}

func requireStoppedSNRForGhostPeer() {
	By("Verifying SNR agents are stopped")
	_, err := APIClient.DaemonSets(medik8sparams.OperatorNs).Get(context.TODO(),
		"self-node-remediation-ds", metav1.GetOptions{})
	if err == nil {
		Skip("Stop SNR agents before running the SBR ghost peer test to avoid sharing the watchdog")
	}
	Expect(k8serrors.IsNotFound(err)).To(BeTrue(), "SNR DaemonSet lookup must succeed or return NotFound")
	pods, err := APIClient.Pods(medik8sparams.OperatorNs).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=self-node-remediation,app.kubernetes.io/component=agent",
	})
	Expect(err).ToNot(HaveOccurred(), "SNR agent pods must be readable")
	if len(pods.Items) != 0 {
		Skip("Wait for all SNR agent pods to terminate before running the SBR ghost peer test")
	}
}

func removeStaleGhostPeerConfigs() {
	By("Removing only this test's stale configurations")
	configs, err := ghostPeerConfigs().List(context.TODO(), metav1.ListOptions{})
	Expect(err).ToNot(HaveOccurred(), "Existing SBRCs must be readable")
	for idx := range configs.Items {
		config := &configs.Items[idx]
		if config.GetLabels()[ghostPeerTestLabel] == "true" {
			cleanupGhostPeerConfig(config)
		}
	}
	remaining, err := ghostPeerConfigs().List(context.TODO(), metav1.ListOptions{})
	Expect(err).ToNot(HaveOccurred(), "Remaining SBRCs must be readable")
	if len(remaining.Items) > 0 {
		Skip("Remove existing SBRCs before running the isolated ghost peer regression")
	}
}

// ghostPeerAgents includes unready pods so a crashing agent cannot disappear from the check.
func ghostPeerAgents(configName string) ([]corev1.Pod, error) {
	ctx, cancel := context.WithTimeout(context.Background(), sbrparams.GhostPeerReadTimeout)
	defer cancel()
	set, err := APIClient.DaemonSets(medik8sparams.OperatorNs).Get(ctx,
		sbrparams.SBRAgentDaemonSetPrefix+configName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get agent DaemonSet: %w", err)
	}
	if set.Status.ObservedGeneration < set.Generation || set.Status.DesiredNumberScheduled < 2 ||
		set.Status.NumberReady != set.Status.DesiredNumberScheduled {
		return nil, fmt.Errorf("agent DaemonSet is not fully Ready: %+v", set.Status)
	}
	pods, err := APIClient.Pods(medik8sparams.OperatorNs).List(ctx,
		metav1.ListOptions{LabelSelector: "app=sbr-agent,sbrconfig=" + configName})
	if err != nil {
		return nil, fmt.Errorf("list agents: %w", err)
	}
	if len(pods.Items) != int(set.Status.DesiredNumberScheduled) {
		return nil, fmt.Errorf("found %d pods for %d scheduled agents", len(pods.Items), set.Status.DesiredNumberScheduled)
	}
	nodes := make(map[string]bool)
	for idx := range pods.Items {
		agent := &pods.Items[idx]
		ready := false
		for _, condition := range agent.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		if !ready || agent.Status.Phase != corev1.PodRunning || agent.DeletionTimestamp != nil ||
			!ownedBy(agent.OwnerReferences, set.UID) || len(agent.Status.ContainerStatuses) == 0 ||
			agent.Spec.NodeName == "" || nodes[agent.Spec.NodeName] {
			return nil, fmt.Errorf("agent %s must be Ready on its own node and owned by the DaemonSet", agent.Name)
		}
		nodes[agent.Spec.NodeName] = true
	}

	return pods.Items, nil
}

// readGhostPeerFile encodes exec output because the pod helper uses a TTY.
func readGhostPeerFile(agentName string, command []string) []byte {
	agent, err := pod.Pull(APIClient, agentName, medik8sparams.OperatorNs)
	Expect(err).ToNot(HaveOccurred(), "Agent %q must exist for device inspection", agentName)
	output, err := agent.ExecCommandWithTimeout(command, sbrparams.GhostPeerReadTimeout, "sbr-agent")
	Expect(err).ToNot(HaveOccurred(), "Device read through agent %q must succeed", agentName)
	data, err := base64.StdEncoding.DecodeString(strings.TrimSpace(output.String()))
	Expect(err).ToNot(HaveOccurred(), "Agent %q device output must be valid base64", agentName)

	return data
}

func verifyGhostPeerNodeMap(agents []corev1.Pod) {
	data := readGhostPeerFile(agents[0].Name, []string{"base64", "/dev/sbr/sbr-device-nodemap"})
	Expect(len(data)).To(BeNumerically(">=", nodeMapJSONCRCSize), "Node map must contain its CRC32")
	nodeMap, raw, err := parseNodeMapPayload(data)
	Expect(err).ToNot(HaveOccurred(), "Filesystem node map CRC and format must be valid: %s", raw)
	AddReportEntry("Shared filesystem node map", raw)
	Expect(nodeMap.Entries).To(HaveLen(len(agents)), "Every agent node must have one assignment")
	Expect(nodeMap.NodeUsage).To(HaveLen(len(agents)), "Reverse node map must contain only real agents")
	for idx := range agents {
		nodeName := agents[idx].Spec.NodeName
		entry, found := nodeMap.Entries[nodeName]
		Expect(found).To(BeTrue(), "Agent node %q must be in the node map", nodeName)
		Expect(entry.NodeName).To(Equal(nodeName), "Node map entry must identify agent node %q", nodeName)
		Expect(entry.NodeID).To(BeNumerically(">=", 1), "Node ID must be in 1-255")
		Expect(entry.NodeID).To(BeNumerically("<=", 255), "Node ID must be in 1-255")
		Expect(nodeMap.NodeUsage[strconv.Itoa(int(entry.NodeID))]).To(Equal(nodeName),
			"Node ID %d must uniquely identify node %q", entry.NodeID, nodeName)
	}
	if owner, assigned := nodeMap.NodeUsage["1"]; assigned {
		Skip(fmt.Sprintf("Node %s legitimately owns ID 1, so the unused preflight slot regression cannot run", owner))
	}
}

func checkGhostPeerStability(configName string, baseline []corev1.Pod) error {
	current, err := ghostPeerAgents(configName)
	if err != nil {
		return err
	}
	Expect(current).To(HaveLen(len(baseline)), "Agent count must stay unchanged")
	for idx := range baseline {
		original := &baseline[idx]
		var samePod *corev1.Pod
		for other := range current {
			if current[other].UID == original.UID {
				samePod = &current[other]
			}
		}
		Expect(samePod).ToNot(BeNil(), "Agent %q must not be replaced during observation", original.Name)
		Expect(samePod.Status.ContainerStatuses).To(Equal(original.Status.ContainerStatuses),
			"Agent %q container state and restart counts must remain unchanged", original.Name)
		// dd must return exactly one full filesystem slot. A failed/short read must not look empty.
		slot := readGhostPeerFile(original.Name, []string{
			"sh", "-ec",
			"dd if=/dev/sbr/sbr-device bs=512 skip=1 count=1 status=none | base64",
		})
		Expect(slot).To(HaveLen(preflightSlotSize), "Agent %q must read the full slot 1", original.Name)
		Expect(bytes.Equal(slot, make([]byte, preflightSlotSize))).To(BeTrue(),
			"Agent %q must read a cleared preflight heartbeat in unused slot 1", original.Name)
	}

	return nil
}

func cleanupGhostPeerConfig(config *unstructured.Unstructured) {
	ctx, cancel := context.WithTimeout(context.Background(), sbrparams.GhostPeerCleanupTimeout)
	defer cancel()
	name, uid := config.GetName(), config.GetUID()
	background := metav1.DeletePropagationBackground
	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}, PropagationPolicy: &background}
	err := ghostPeerConfigs().Delete(ctx, name, options)
	Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue(), "Delete test SBRC %q: %v", name, err)
	Eventually(func() bool {
		_, err := ghostPeerConfigs().Get(ctx, name, metav1.GetOptions{})

		return k8serrors.IsNotFound(err)
	}, sbrparams.GhostPeerCleanupTimeout, sbrparams.DefaultPollInterval).Should(BeTrue(),
		"SBRC %q must finish normal operator cleanup", name)
	selector := metav1.ListOptions{LabelSelector: "sbrconfig=" + name}
	jobs, err := APIClient.K8sClient.BatchV1().Jobs(medik8sparams.OperatorNs).List(ctx, selector)
	Expect(err).ToNot(HaveOccurred(), "Test jobs for %q must be readable", name)
	for idx := range jobs.Items {
		job := &jobs.Items[idx]
		jobUID := job.UID
		jobOptions := metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &jobUID}, PropagationPolicy: &background,
		}
		err := APIClient.K8sClient.BatchV1().Jobs(medik8sparams.OperatorNs).Delete(ctx, job.Name, jobOptions)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue(), "Delete test job %q: %v", job.Name, err)
	}
	Eventually(func(check Gomega) {
		_, err := APIClient.DaemonSets(medik8sparams.OperatorNs).Get(ctx,
			sbrparams.SBRAgentDaemonSetPrefix+name, metav1.GetOptions{})
		check.Expect(k8serrors.IsNotFound(err)).To(BeTrue(), "Test agent DaemonSet must be gone")
		pods, err := APIClient.Pods(medik8sparams.OperatorNs).List(ctx, selector)
		check.Expect(err).ToNot(HaveOccurred())
		check.Expect(pods.Items).To(BeEmpty(), "Test pods must be gone")
		pvcs, err := APIClient.PersistentVolumeClaims(medik8sparams.OperatorNs).List(ctx, selector)
		check.Expect(err).ToNot(HaveOccurred())
		check.Expect(pvcs.Items).To(BeEmpty(), "Test PVCs must be gone")
		jobs, err := APIClient.K8sClient.BatchV1().Jobs(medik8sparams.OperatorNs).List(ctx, selector)
		check.Expect(err).ToNot(HaveOccurred())
		check.Expect(jobs.Items).To(BeEmpty(), "Test jobs must be gone")
	}, sbrparams.GhostPeerCleanupTimeout, sbrparams.DefaultPollInterval).Should(Succeed(),
		"All operands of test SBRC %q must be removed", name)
}
