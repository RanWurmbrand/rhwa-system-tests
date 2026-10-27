package tests

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/deployment"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/pod"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/reportxml"

	"github.com/medik8s/system-tests/tests/internal/helpers"
	"github.com/medik8s/system-tests/tests/internal/labels"
	. "github.com/medik8s/system-tests/tests/internal/medik8sinittools"
	"github.com/medik8s/system-tests/tests/internal/medik8sparams"
	"github.com/medik8s/system-tests/tests/snr-operator/internal/snrparams"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("SNR Agent MinPeersForRemediation",
	Serial, Ordered, ContinueOnFailure,
	Label(labels.OperatorSNR, snrparams.Label,
		labels.DisruptionDestructive, labels.FrequencyNightly),
	func() {
		var (
			ctx              context.Context
			targetWorkerName string
			readyWorkerCount int
		)

		BeforeAll(func() {
			ctx = context.Background()

			By("Verifying the SNR controller-manager deployment is ready")

			snrDeployment, err := deployment.Pull(
				APIClient, snrparams.OperatorDeploymentName, medik8sparams.OperatorNs)
			Expect(err).ToNot(HaveOccurred(), "Failed to get SNR deployment")
			Expect(snrDeployment.IsReady(medik8sparams.DefaultTimeout)).To(BeTrue(),
				"SNR deployment is not Ready")

			By("Verifying the cluster has at least 2 Ready worker nodes")

			// The agent only counts peers other than itself, so the node under
			// test must keep at least one real peer. Without one, a build
			// missing the RHWA-1872 fix takes the isolated-node path and
			// reboots the node instead of failing the log assertion.
			var countErr error
			readyWorkerCount, countErr = helpers.CountReadyWorkerNodes(ctx, APIClient)
			Expect(countErr).ToNot(HaveOccurred(), "Failed to count Ready worker nodes")

			if readyWorkerCount < snrparams.MinWorkersForPeerCheck {
				Skip(fmt.Sprintf(
					"minPeersForRemediation spec needs at least %d Ready worker nodes, found %d",
					snrparams.MinWorkersForPeerCheck, readyWorkerCount))
			}

			By("Selecting the worker node under test")

			targetNode, selectErr := helpers.SelectWorkerNode(ctx, APIClient)
			Expect(selectErr).ToNot(HaveOccurred(), "Failed to select a worker node")
			targetWorkerName = targetNode.Name
			GinkgoWriter.Printf("Node under test: %s (%d Ready workers)\n",
				targetWorkerName, readyWorkerCount)

			By("Verifying the SNR agent DaemonSet pods are Running")

			Eventually(
				verifyDSPodsRunning, snrparams.DSPodRestartTimeout, snrparams.DefaultPollInterval).Should(Succeed(),
				"SNR DaemonSet pods must be Running before the minPeersForRemediation spec starts")
		})

		It("Verify the SNR agent uses the configured minPeersForRemediation value",
			reportxml.ID("1972"),
			Label(labels.TierResiliency, labels.PlatformAny,
				labels.ComponentRemediation), func() {
				By("Reading the current minPeersForRemediation from the SelfNodeRemediationConfig")

				originalMinPeers, originalFound := readSNRCMinPeers()
				// The Ready worker count is exactly one more than the most
				// worker peers a single node can see, because the agent's peer
				// list excludes the node itself. The configured minimum is
				// therefore always out of reach, and with at least 2 workers it
				// always differs from the CRD default of 1. Bump it once more
				// when the cluster already uses that value, so the patch is
				// never a no-op and the DaemonSet really rolls.
				minPeersUnderTest := int64(readyWorkerCount)
				if originalFound && originalMinPeers == minPeersUnderTest {
					minPeersUnderTest++
				}

				GinkgoWriter.Printf(
					"minPeersForRemediation: original %d (set=%t), under test %d\n",
					originalMinPeers, originalFound, minPeersUnderTest)

				DeferCleanup(func() {
					By("DeferCleanup: restoring the original minPeersForRemediation")

					restoreSNRCMinPeers(originalMinPeers, originalFound)

					By("DeferCleanup: waiting for the agent DaemonSet pods to be Running again")

					Eventually(
						verifyDSPodsRunning, snrparams.DSPodRestartTimeout,
						snrparams.DefaultPollInterval).Should(Succeed(),
						"SNR DaemonSet pods must be Running after minPeersForRemediation is restored")
				})

				By("Patching the SelfNodeRemediationConfig with minPeersForRemediation above the available peer count")

				prePatchUIDs := collectDSPodUIDs()
				patch := []byte(fmt.Sprintf(
					`{"spec":{"minPeersForRemediation":%d}}`, minPeersUnderTest))
				Eventually(func() error {
					return APIClient.Patch(context.TODO(),
						snrcForPatch(),
						client.RawPatch(types.MergePatchType, patch))
				}, medik8sparams.DefaultTimeout, snrparams.DefaultPollInterval).Should(Succeed(),
					"SNRC %q must accept minPeersForRemediation=%d",
					snrparams.SNRConfigName, minPeersUnderTest)

				By("Waiting for the SNR agent DaemonSet pods to be replaced and Ready")

				Eventually(func() error {
					return verifyDSPodsReplaced(prePatchUIDs)
				}, snrparams.DSPodRestartTimeout, snrparams.DefaultPollInterval).Should(Succeed(),
					"SNR DaemonSet pods must be replaced after minPeersForRemediation changes")

				By("Letting every agent re-discover its peers after the rollout")

				// An agent that rolled before its peers caches an incomplete
				// peer list for up to peerUpdateInterval. Injecting the fault
				// into an agent that believes it has no peer at all would
				// exercise the out-of-scope isolated-node path instead.
				Consistently(
					verifyDSPodsRunning, snrparams.PeerListSettleTimeout,
					snrparams.DefaultPollInterval).Should(Succeed(),
					"SNR DaemonSet pods must stay Running while every agent refreshes its peer list")

				By("Asserting the agent container on the node under test carries MIN_PEERS_FOR_REMEDIATION")

				agentPodName := agentPodNameOnNode(targetWorkerName)
				Expect(agentMinPeersEnv(agentPodName)).
					To(Equal(strconv.FormatInt(minPeersUnderTest, 10)),
						"Agent pod %q must carry %s=%d rendered from the SNRC",
						agentPodName, snrparams.MinPeersEnvVarName, minPeersUnderTest)

				By("Resolving the kubernetes API server cluster IPs")

				apiIPs := apiServerClusterIPs()
				Expect(apiIPs).ToNot(BeEmpty(),
					"Service default/kubernetes must report at least one cluster IP to blackhole")

				By("Recording the boot ID of the node under test")

				oldBootID, bootErr := helpers.GetNodeBootIDFromAPI(ctx, APIClient, targetWorkerName)
				Expect(bootErr).ToNot(HaveOccurred(),
					"Must read the boot ID of node %q before the fault injection", targetWorkerName)

				By("Blackholing the API server addresses inside the agent pod, restoring them automatically")

				DeferCleanup(func() {
					By("DeferCleanup: removing any leftover blackhole route from the agent pod")

					removeBlackholeRoutes(agentPodName, apiIPs)
				})

				blackholeAPIForAgent(agentPodName, apiIPs)

				By("Asserting the agent reported an insufficient number of peers with the configured minimum")

				Eventually(func() error {
					return agentLoggedInsufficientPeers(agentPodName, minPeersUnderTest)
				}, medik8sparams.DefaultTimeout, snrparams.DefaultPollInterval).Should(Succeed(),
					"Agent pod %q must log %q with minPeersRequired=%d; a pre-RHWA-1872 agent "+
						"ignores MIN_PEERS_FOR_REMEDIATION and uses 0 instead",
					agentPodName, snrparams.MinPeersInsufficientMessage, minPeersUnderTest)

				By("Asserting the node under test did not reboot and is still Ready")

				newBootID, newBootErr := helpers.GetNodeBootIDFromAPI(ctx, APIClient, targetWorkerName)
				Expect(newBootErr).ToNot(HaveOccurred(),
					"Must read the boot ID of node %q after the fault injection", targetWorkerName)
				Expect(newBootID).To(Equal(oldBootID),
					"Node %q must not reboot while minPeersForRemediation keeps the agent from remediating",
					targetWorkerName)
				node := &corev1.Node{}
				Expect(APIClient.Get(ctx, client.ObjectKey{Name: targetWorkerName}, node)).To(Succeed(),
					"Must read node %q after the fault injection", targetWorkerName)
				Expect(helpers.IsNodeReady(node)).To(BeTrue(),
					"Node %q must still be Ready after the agent lost api-server access", targetWorkerName)

				By("Asserting API connectivity is restored inside the agent pod")

				Eventually(func() error {
					return verifyNoBlackholeRoutes(agentPodName, apiIPs)
				}, medik8sparams.DefaultTimeout, snrparams.DefaultPollInterval).Should(Succeed(),
					"Agent pod %q must have no blackhole route left to the api-server", agentPodName)
			})
	})

// readSNRCMinPeers returns spec.minPeersForRemediation of the default SNRC and
// whether the field was set.
func readSNRCMinPeers() (int64, bool) {
	snrc := &unstructured.Unstructured{}
	snrc.SetGroupVersionKind(snrcGVK)
	err := APIClient.Get(context.TODO(),
		client.ObjectKey{
			Name:      snrparams.SNRConfigName,
			Namespace: medik8sparams.OperatorNs,
		},
		snrc)
	Expect(err).ToNot(HaveOccurred(),
		"Failed to get default SNRC %q", snrparams.SNRConfigName)
	value, found, fieldErr := unstructured.NestedInt64(
		snrc.Object, "spec", "minPeersForRemediation")
	Expect(fieldErr).ToNot(HaveOccurred(),
		"Failed to read minPeersForRemediation from SNRC %q", snrparams.SNRConfigName)

	return value, found
}

// restoreSNRCMinPeers puts spec.minPeersForRemediation back to the value read
// before the test. An absent field is removed again so the CRD default applies.
func restoreSNRCMinPeers(value int64, found bool) {
	patch := []byte(`{"spec":{"minPeersForRemediation":null}}`)
	if found {
		patch = []byte(fmt.Sprintf(`{"spec":{"minPeersForRemediation":%d}}`, value))
	}

	Eventually(func() error {
		return APIClient.Patch(context.TODO(),
			snrcForPatch(),
			client.RawPatch(types.MergePatchType, patch))
	}, medik8sparams.DefaultTimeout, snrparams.DefaultPollInterval).Should(Succeed(),
		"Failed to restore minPeersForRemediation on SNRC %q", snrparams.SNRConfigName)
}

// agentPodNameOnNode returns the name of the Running SNR agent pod scheduled on
// the given node.
func agentPodNameOnNode(nodeName string) string {
	var podName string

	Eventually(func() error {
		dsPods, listErr := pod.List(APIClient, medik8sparams.OperatorNs, metav1.ListOptions{
			LabelSelector: snrparams.DaemonSetPodLabelSelector,
		})
		if listErr != nil {
			return fmt.Errorf("failed to list SNR DaemonSet pods: %w", listErr)
		}

		for _, dsPod := range dsPods {
			if dsPod.Object.Spec.NodeName != nodeName {
				continue
			}

			if dsPod.Object.Status.Phase != corev1.PodRunning {
				return fmt.Errorf("agent pod %q on node %q is %s, expected Running",
					dsPod.Object.Name, nodeName, dsPod.Object.Status.Phase)
			}

			podName = dsPod.Object.Name

			return nil
		}

		return fmt.Errorf("no SNR agent pod found on node %q", nodeName)
	}, snrparams.DSPodRestartTimeout, snrparams.DefaultPollInterval).Should(Succeed(),
		"A Running SNR agent pod must exist on node %q", nodeName)

	return podName
}

// agentMinPeersEnv returns the value of MIN_PEERS_FOR_REMEDIATION on the
// manager container of the named agent pod.
func agentMinPeersEnv(podName string) string {
	agentPod, pullErr := pod.Pull(APIClient, podName, medik8sparams.OperatorNs)
	Expect(pullErr).ToNot(HaveOccurred(), "Failed to pull agent pod %q", podName)

	for _, container := range agentPod.Object.Spec.Containers {
		if container.Name != snrparams.ManagerContainerName {
			continue
		}

		for _, env := range container.Env {
			if env.Name == snrparams.MinPeersEnvVarName {
				return env.Value
			}
		}
	}

	Fail(fmt.Sprintf("agent pod %q container %q has no %s environment variable",
		podName, snrparams.ManagerContainerName, snrparams.MinPeersEnvVarName))

	return ""
}

// apiServerClusterIPs returns the cluster IPs of the default/kubernetes
// Service, which is the address the agent uses to reach the api-server.
func apiServerClusterIPs() []string {
	svc := &corev1.Service{}
	err := APIClient.Get(context.TODO(),
		client.ObjectKey{Namespace: "default", Name: "kubernetes"}, svc)
	Expect(err).ToNot(HaveOccurred(), "Failed to get Service default/kubernetes")

	if len(svc.Spec.ClusterIPs) > 0 {
		return svc.Spec.ClusterIPs
	}

	if svc.Spec.ClusterIP != "" {
		return []string{svc.Spec.ClusterIP}
	}

	return nil
}

// blackholeAPIForAgent drops the agent pod's route to the api-server for
// snrparams.APIDisconnectDuration and then restores it, from inside the pod's
// own network namespace so the node and its kubelet are untouched. The exec
// blocks for the whole window; errors are reported but not fatal, because the
// assertions on the agent log are the real gate.
func blackholeAPIForAgent(podName string, apiIPs []string) {
	agentPod, pullErr := pod.Pull(APIClient, podName, medik8sparams.OperatorNs)
	Expect(pullErr).ToNot(HaveOccurred(), "Failed to pull agent pod %q", podName)

	var script strings.Builder

	for _, apiIP := range apiIPs {
		fmt.Fprintf(&script, "ip route add blackhole %s; ", apiIP)
	}

	// Separated by ';' rather than '&&' so a failure to add one address still
	// runs every delete below.
	fmt.Fprintf(&script, "sleep %d; ", int(snrparams.APIDisconnectDuration.Seconds()))

	for _, apiIP := range apiIPs {
		fmt.Fprintf(&script, "ip route delete blackhole %s; ", apiIP)
	}

	GinkgoWriter.Printf("Blackholing %v inside agent pod %s for %s\n",
		apiIPs, podName, snrparams.APIDisconnectDuration)
	output, execErr := agentPod.ExecCommandWithTimeout(
		[]string{"sh", "-c", script.String()},
		snrparams.APIDisconnectExecTimeout,
		snrparams.ManagerContainerName)
	if execErr != nil {
		GinkgoWriter.Printf("WARNING: blackhole exec on agent pod %s returned %v (output: %q)\n",
			podName, execErr, output.String())
	}
}

// removeBlackholeRoutes best-effort deletes any blackhole route left in the
// agent pod, and deletes the pod when one survives so the DaemonSet recreates
// it with a clean network namespace.
func removeBlackholeRoutes(podName string, apiIPs []string) {
	agentPod, pullErr := pod.Pull(APIClient, podName, medik8sparams.OperatorNs)
	if pullErr != nil {
		GinkgoWriter.Printf("removeBlackholeRoutes: agent pod %s is gone, nothing to clean: %v\n",
			podName, pullErr)

		return
	}

	var script strings.Builder

	for _, apiIP := range apiIPs {
		fmt.Fprintf(&script, "ip route delete blackhole %s 2>/dev/null; ", apiIP)
	}

	fmt.Fprint(&script, "true")

	if _, execErr := agentPod.ExecCommandWithTimeout(
		[]string{"sh", "-c", script.String()},
		medik8sparams.DefaultTimeout,
		snrparams.ManagerContainerName); execErr != nil {
		GinkgoWriter.Printf("WARNING: failed to clean blackhole routes in agent pod %s: %v\n",
			podName, execErr)
	}

	if checkErr := verifyNoBlackholeRoutes(podName, apiIPs); checkErr != nil {
		GinkgoWriter.Printf(
			"WARNING: %v -- deleting agent pod %s so the DaemonSet recreates it\n",
			checkErr, podName)

		if _, delErr := agentPod.Delete(); delErr != nil {
			GinkgoWriter.Printf("WARNING: failed to delete agent pod %s: %v\n", podName, delErr)
		}
	}
}

// verifyNoBlackholeRoutes reports an error while the agent pod still has a
// blackhole route to any api-server address.
func verifyNoBlackholeRoutes(podName string, apiIPs []string) error {
	agentPod, pullErr := pod.Pull(APIClient, podName, medik8sparams.OperatorNs)
	if pullErr != nil {
		// A replaced pod has a fresh network namespace and no blackhole route.
		GinkgoWriter.Printf("verifyNoBlackholeRoutes: agent pod %s is gone: %v\n", podName, pullErr)

		return nil
	}

	output, execErr := agentPod.ExecCommandWithTimeout(
		[]string{"sh", "-c", "ip route show " + strings.Join(apiIPs, " ")},
		medik8sparams.DefaultTimeout,
		snrparams.ManagerContainerName)
	if execErr != nil {
		return fmt.Errorf("failed to read routes in agent pod %s: %w", podName, execErr)
	}

	if strings.Contains(output.String(), "blackhole") {
		return fmt.Errorf("agent pod %s still blackholes the api-server: %s",
			podName, strings.TrimSpace(output.String()))
	}

	return nil
}

// agentLoggedInsufficientPeers reports an error until the agent log shows that
// it refused to contact peers because fewer than minPeers were found, naming
// the configured minimum. An agent that never read MIN_PEERS_FOR_REMEDIATION
// uses 0, never takes this path, and so never logs the message.
func agentLoggedInsufficientPeers(podName string, minPeers int64) error {
	agentPod, pullErr := pod.Pull(APIClient, podName, medik8sparams.OperatorNs)
	if pullErr != nil {
		return fmt.Errorf("failed to pull agent pod %s: %w", podName, pullErr)
	}

	logStr, logErr := agentPod.GetLog(snrparams.AgentLogWindow, snrparams.ManagerContainerName)
	if logErr != nil {
		return fmt.Errorf("failed to read logs of agent pod %s: %w", podName, logErr)
	}

	if !strings.Contains(logStr, snrparams.MinPeersInsufficientMessage) {
		return fmt.Errorf("agent pod %s has not logged %q",
			podName, snrparams.MinPeersInsufficientMessage)
	}

	// zap renders the log fields as JSON, spaced in console mode and unspaced
	// in JSON mode: match "minPeersRequired": 3 and "minPeersRequired":3 alike.
	pattern := regexp.MustCompile(fmt.Sprintf(`"minPeersRequired"\s*:\s*%d\b`, minPeers))
	if !pattern.MatchString(logStr) {
		return fmt.Errorf(
			"agent pod %s logged %q but not with minPeersRequired=%d",
			podName, snrparams.MinPeersInsufficientMessage, minPeers)
	}

	return nil
}
