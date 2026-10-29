package tests

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

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
			rebootNodeName   string
			healthyNodeName  string
			routeTool        string
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

			By("Verifying the cluster has enough Ready worker nodes")

			// Each spec isolates a worker of its own, and each of those must
			// keep at least one peer: the agent's peer list excludes the node
			// itself, and a node that sees no peer at all reboots on isolation
			// whatever minPeersForRemediation says.
			var countErr error
			readyWorkerCount, countErr = helpers.CountReadyWorkerNodes(ctx, APIClient)
			Expect(countErr).ToNot(HaveOccurred(), "Failed to count Ready worker nodes")

			if readyWorkerCount < snrparams.MinWorkersForPeerCheck {
				Skip(fmt.Sprintf(
					"minPeersForRemediation specs need at least %d Ready worker nodes, found %d",
					snrparams.MinWorkersForPeerCheck, readyWorkerCount))
			}

			By("Selecting the two worker nodes under test")

			rebootNode, selectErr := helpers.SelectWorkerNode(ctx, APIClient)
			Expect(selectErr).ToNot(HaveOccurred(), "Failed to select the worker node to reboot")
			rebootNodeName = rebootNode.Name

			healthyNode, healthyErr := helpers.SelectWorkerNode(ctx, APIClient, rebootNodeName)
			Expect(healthyErr).ToNot(HaveOccurred(),
				"Failed to select a second worker node distinct from %q", rebootNodeName)
			healthyNodeName = healthyNode.Name
			Expect(healthyNodeName).ToNot(Equal(rebootNodeName),
				"The two specs must isolate different nodes")

			GinkgoWriter.Printf(
				"Nodes under test: %s (must reboot), %s (must not reboot); %d Ready workers\n",
				rebootNodeName, healthyNodeName, readyWorkerCount)

			By("Verifying the SNR agent DaemonSet pods are Running")

			Eventually(
				verifyDSPodsRunning, snrparams.DSPodRestartTimeout, snrparams.DefaultPollInterval).Should(Succeed(),
				"SNR DaemonSet pods must be Running before the minPeersForRemediation specs start")

			By("Resolving how to manipulate routes inside the agent container")

			// Probed before anything is patched: an image without iproute2
			// cannot host the fault at all, and finding that out after the
			// first rollout would disturb every agent in the cluster for a
			// run that then skips.
			routeTool = resolveRouteTool(agentPodNameOnNode(rebootNodeName))

			By("Reading the current minPeersForRemediation from the SelfNodeRemediationConfig")

			// Read and restored once for the whole container: both specs patch
			// the same cluster-wide SNRC, and restoring between them would roll
			// every agent pod for nothing.
			originalMinPeers, originalFound := readSNRCMinPeers()
			GinkgoWriter.Printf("minPeersForRemediation: original %d (set=%t)\n",
				originalMinPeers, originalFound)

			DeferCleanup(func() {
				By("DeferCleanup: restoring the original minPeersForRemediation")

				restoreSNRCMinPeers(originalMinPeers, originalFound)
			})
		})

		It("Verify an isolated agent reboots its node when minPeersForRemediation is zero",
			reportxml.ID("1972"),
			Label(labels.TierResiliency, labels.PlatformAny,
				labels.ComponentRemediation), func() {
				// The positive control. With no minimum to meet, the agent asks
				// its peers, hears nothing back because they are blackholed too,
				// and remediates itself. The second spec injects exactly the
				// same fault, so this one is what tells that fault apart from a
				// fault that does nothing at all.
				agent := prepareIsolatedAgent(ctx, rebootNodeName, routeTool, snrparams.MinPeersRemediateAlone)

				DeferCleanup(func() {
					By("DeferCleanup: waiting for the rebooted node and its agent to come back")

					removeBlackholeRoutes(agent)

					Expect(helpers.WaitForNodeReady(ctx, APIClient, rebootNodeName,
						snrparams.DefaultPollInterval, snrparams.NodeReadyTimeout,
						GinkgoWriter.Printf)).To(Succeed(),
						"Node %q must return Ready after the isolation reboot", rebootNodeName)

					Eventually(
						verifyDSPodsRunning, snrparams.DSPodRestartTimeout,
						snrparams.DefaultPollInterval).Should(Succeed(),
						"SNR DaemonSet pods must be Running again after the isolation reboot")
				})

				By("Isolating the agent from the api-server and from every peer agent")

				isolateAgent(agent, snrparams.IsolationRebootHoldDuration)

				By("Asserting the node under test rebooted")

				Expect(helpers.WaitForNodeReboot(ctx, APIClient, rebootNodeName, agent.bootID,
					snrparams.DefaultPollInterval, snrparams.NodeReadyTimeout,
					GinkgoWriter.Printf)).To(Succeed(),
					"Node %q must reboot: an isolated agent that needs no peer to agree with it "+
						"has to remediate itself", rebootNodeName)
			})

		It("Verify an isolated agent leaves its node alone when minPeersForRemediation is out of reach",
			reportxml.ID("1972"),
			Label(labels.TierResiliency, labels.PlatformAny,
				labels.ComponentRemediation), func() {
				// A worker's peer list holds every other worker, so the Ready
				// worker count is always one more than the node under test can
				// find, and with at least 2 workers it always differs from the
				// CRD default of 1.
				minPeersUnderTest := int64(readyWorkerCount)

				agent := prepareIsolatedAgent(ctx, healthyNodeName, routeTool, minPeersUnderTest)

				DeferCleanup(func() {
					By("DeferCleanup: removing any leftover blackhole route from the agent pod")

					removeBlackholeRoutes(agent)
				})

				By("Isolating the agent from the api-server and from every peer agent")

				isolateAgent(agent, snrparams.IsolationHoldDuration)

				By("Asserting the node under test did not reboot and stays Ready")

				// Watched rather than read once: the agent reaches its verdict
				// partway into the isolation window and a watchdog reboot lands
				// some way after that, so a single reading taken the moment the
				// window closes could still miss it.
				Consistently(func() error {
					return verifyNodeNotRebooted(ctx, healthyNodeName, agent.bootID)
				}, snrparams.RebootSettleWindow, snrparams.DefaultPollInterval).Should(Succeed(),
					"Node %q must not reboot while minPeersForRemediation keeps the agent from "+
						"remediating; a pre-RHWA-1872 agent ignores %s, uses 0 instead, and "+
						"reboots just like the node in the previous spec",
					healthyNodeName, snrparams.MinPeersEnvVarName)

				By("Asserting API connectivity is restored inside the agent pod")

				Eventually(func() error {
					return verifyNoBlackholeRoutes(agent)
				}, medik8sparams.DefaultTimeout, snrparams.DefaultPollInterval).Should(Succeed(),
					"Agent pod %q must have no blackhole route left to the api-server or its peers",
					agent.podName)
			})
	})

// isolatedAgent is everything the fault injection and its assertions need
// about one agent pod: the pod to exec into, how to reach iproute2 inside it,
// the addresses that cut it off from the api-server and from every peer, and
// the boot ID its node reported before the fault.
type isolatedAgent struct {
	podName      string
	routeTool    string
	blackholeIPs []string
	bootID       string
}

// routeToolCandidates are the ways to reach iproute2 from inside the agent
// container, most direct first. The agent image installs iproute for exactly
// this purpose (self-node-remediation df364797, in medik8s-5.8), but builds
// from before it, and downstream builds that did not pick it up, ship without
// the binary. The last candidate borrows the host's copy through the host
// mount namespace, which the pod can reach because the agent DaemonSet runs
// privileged, as uid 0 and with hostPID. Only the *mount* namespace is
// entered: the network namespace stays the pod's, so the routes still change
// inside the pod and the node keeps its own untouched. Never add -n here.
var routeToolCandidates = []string{
	"ip",
	"/usr/sbin/ip",
	"/sbin/ip",
	"nsenter -m/proc/1/ns/mnt -- ip",
}

// resolveRouteTool returns the first candidate that can read routes inside the
// agent pod. It skips the spec when the container offers no way to do it,
// rather than letting the injection fail silently and the assertions time out
// on a fault that was never applied.
func resolveRouteTool(podName string) string {
	agentPod, pullErr := pod.Pull(APIClient, podName, medik8sparams.OperatorNs)
	Expect(pullErr).ToNot(HaveOccurred(), "Failed to pull agent pod %q", podName)

	for _, candidate := range routeToolCandidates {
		output, execErr := agentPod.ExecCommandWithTimeout(
			[]string{"sh", "-c", candidate + " -4 route show"},
			medik8sparams.DefaultTimeout,
			snrparams.ManagerContainerName)
		if execErr == nil {
			GinkgoWriter.Printf("Using %q to manipulate routes inside agent pod %s\n",
				candidate, podName)

			return candidate
		}

		GinkgoWriter.Printf("Route tool %q is unusable in agent pod %s: %v (output: %q)\n",
			candidate, podName, execErr, strings.TrimSpace(output.String()))
	}

	Skip(fmt.Sprintf(
		"agent pod %q offers no working iproute2 command (tried %v), so the api-server "+
			"and peer disconnection cannot be simulated. The agent image gained iproute in "+
			"self-node-remediation df364797 for this very purpose; a build from before it, "+
			"or a downstream build without it, cannot run these specs",
		podName, routeToolCandidates))

	return ""
}

// prepareIsolatedAgent sets minPeersForRemediation cluster-wide, waits for the
// agent DaemonSet to finish rolling, and collects the handle on the agent pod
// running on nodeName.
func prepareIsolatedAgent(
	ctx context.Context, nodeName, routeTool string, minPeers int64,
) isolatedAgent {
	By(fmt.Sprintf(
		"Patching the SelfNodeRemediationConfig with minPeersForRemediation=%d", minPeers))

	setSNRCMinPeers(minPeers)

	By("Letting every agent re-discover its peers after the rollout")

	// An agent that rolled before its peers caches an incomplete peer list for
	// up to peerUpdateInterval. An agent that believes it has no peer at all
	// takes the isolated-node branch no matter what minimum is configured, so
	// both specs would reboot their node and neither would prove anything.
	Consistently(
		verifyDSPodsRunning, snrparams.PeerListSettleTimeout,
		snrparams.DefaultPollInterval).Should(Succeed(),
		"SNR DaemonSet pods must stay Running while every agent refreshes its peer list")

	By("Asserting the agent container on the node under test carries MIN_PEERS_FOR_REMEDIATION")

	podName := agentPodNameOnNode(nodeName)
	Expect(agentMinPeersEnv(podName)).
		To(Equal(strconv.FormatInt(minPeers, 10)),
			"Agent pod %q must carry %s=%d rendered from the SNRC",
			podName, snrparams.MinPeersEnvVarName, minPeers)

	By("Resolving the api-server and peer agent addresses to blackhole")

	apiIPs := apiServerClusterIPs()
	Expect(apiIPs).ToNot(BeEmpty(),
		"Service default/kubernetes must report at least one cluster IP to blackhole")

	peerIPs := peerAgentPodIPs(nodeName)
	Expect(peerIPs).ToNot(BeEmpty(),
		"Node %q must have at least one peer agent to be cut off from", nodeName)

	blackholeIPs := make([]string, 0, len(apiIPs)+len(peerIPs))
	blackholeIPs = append(blackholeIPs, apiIPs...)
	blackholeIPs = append(blackholeIPs, peerIPs...)

	By("Recording the boot ID of the node under test")

	bootID, bootErr := helpers.GetNodeBootIDFromAPI(ctx, APIClient, nodeName)
	Expect(bootErr).ToNot(HaveOccurred(),
		"Must read the boot ID of node %q before the fault injection", nodeName)

	return isolatedAgent{
		podName:      podName,
		routeTool:    routeTool,
		blackholeIPs: blackholeIPs,
		bootID:       bootID,
	}
}

// readSNRCMinPeers returns spec.minPeersForRemediation of the default SNRC and
// whether the field was set.
func readSNRCMinPeers() (int64, bool) {
	value, found, err := currentSNRCMinPeers()
	Expect(err).ToNot(HaveOccurred(),
		"Failed to read minPeersForRemediation from SNRC %q", snrparams.SNRConfigName)

	return value, found
}

// currentSNRCMinPeers reads spec.minPeersForRemediation back from the live
// SNRC, reporting whether the field is set. It returns an error instead of
// failing the spec so cleanup can use it too.
func currentSNRCMinPeers() (int64, bool, error) {
	snrc := &unstructured.Unstructured{}
	snrc.SetGroupVersionKind(snrcGVK)

	if err := APIClient.Get(context.TODO(),
		client.ObjectKey{
			Name:      snrparams.SNRConfigName,
			Namespace: medik8sparams.OperatorNs,
		},
		snrc); err != nil {
		return 0, false, fmt.Errorf("failed to get default SNRC %q: %w",
			snrparams.SNRConfigName, err)
	}

	value, found, fieldErr := unstructured.NestedInt64(
		snrc.Object, "spec", "minPeersForRemediation")
	if fieldErr != nil {
		return 0, false, fmt.Errorf("failed to read spec.minPeersForRemediation: %w", fieldErr)
	}

	return value, found, nil
}

// minPeersPatch renders the merge patch for spec.minPeersForRemediation. An
// unset field is patched to null so the CRD default applies again.
func minPeersPatch(value int64, found bool) []byte {
	if !found {
		return []byte(`{"spec":{"minPeersForRemediation":null}}`)
	}

	return fmt.Appendf(nil, `{"spec":{"minPeersForRemediation":%d}}`, value)
}

// applySNRCMinPeers patches the SNRC and waits for the agent DaemonSet to
// settle. rolloutExpected picks between the two waits: a patch that changes
// the value rolls every agent pod, and merely waiting for pods to be Running
// would return at once while the pre-patch pods are still up, leaving the next
// step to run against a cluster mid-rollout; a patch that changes nothing
// rolls nothing, and waiting for replacements would block for the whole
// DSPodRestartTimeout.
func applySNRCMinPeers(patch []byte, rolloutExpected bool) {
	// Captured before the patch so the wait below can tell the pre-patch pods
	// from their replacements.
	prePatchUIDs := collectDSPodUIDs()

	Eventually(func() error {
		return APIClient.Patch(context.TODO(),
			snrcForPatch(),
			client.RawPatch(types.MergePatchType, patch))
	}, medik8sparams.DefaultTimeout, snrparams.DefaultPollInterval).Should(Succeed(),
		"SNRC %q must accept the merge patch %s", snrparams.SNRConfigName, patch)

	if !rolloutExpected {
		Eventually(
			verifyDSPodsRunning, snrparams.DSPodRestartTimeout,
			snrparams.DefaultPollInterval).Should(Succeed(),
			"SNR DaemonSet pods must be Running after the minPeersForRemediation patch")

		return
	}

	Eventually(func() error {
		return verifyDSPodsReplaced(prePatchUIDs)
	}, snrparams.DSPodRestartTimeout, snrparams.DefaultPollInterval).Should(Succeed(),
		"SNR DaemonSet pods must be replaced after minPeersForRemediation changes")
}

// setSNRCMinPeers sets spec.minPeersForRemediation to value and waits for the
// agent DaemonSet to settle.
func setSNRCMinPeers(value int64) {
	current, found, readErr := currentSNRCMinPeers()

	applySNRCMinPeers(minPeersPatch(value, true), readErr != nil || !found || current != value)
}

// restoreSNRCMinPeers puts spec.minPeersForRemediation back to the value read
// before the specs ran -- an absent field is removed again so the CRD default
// applies -- and waits for the agent DaemonSet to settle afterwards.
func restoreSNRCMinPeers(value int64, found bool) {
	current, currentFound, readErr := currentSNRCMinPeers()

	applySNRCMinPeers(minPeersPatch(value, found),
		readErr != nil || currentFound != found || current != value)
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

// peerAgentPodIPs returns the pod IPs of every SNR agent except the one on
// nodeName. An agent reaches its peers at their agent pod IP -- the peer list
// is built from each peer pod's primary pod IP, not from any node address --
// so blackholing these is what cuts the node under test off from every peer,
// and it still leaves every node, kubelet and host route alone.
func peerAgentPodIPs(nodeName string) []string {
	dsPods, listErr := pod.List(APIClient, medik8sparams.OperatorNs, metav1.ListOptions{
		LabelSelector: snrparams.DaemonSetPodLabelSelector,
	})
	Expect(listErr).ToNot(HaveOccurred(), "Failed to list SNR DaemonSet pods")

	var peerIPs []string

	for _, dsPod := range dsPods {
		if dsPod.Object.Spec.NodeName == nodeName {
			continue
		}

		for _, podIP := range dsPod.Object.Status.PodIPs {
			if podIP.IP != "" {
				peerIPs = append(peerIPs, podIP.IP)
			}
		}
	}

	return peerIPs
}

// isolateAgent blackholes every address the agent needs -- the api-server and
// each peer agent -- for hold and then restores them, from inside the pod's
// own network namespace so the node and its kubelet are untouched. The exec
// blocks for the whole window, and ends early when a reboot takes the pod with
// it. Errors are reported but not fatal, because the reboot assertions are the
// real gate.
func isolateAgent(agent isolatedAgent, hold time.Duration) {
	agentPod, pullErr := pod.Pull(APIClient, agent.podName, medik8sparams.OperatorNs)
	Expect(pullErr).ToNot(HaveOccurred(), "Failed to pull agent pod %q", agent.podName)

	var script strings.Builder

	for _, blockIP := range agent.blackholeIPs {
		fmt.Fprintf(&script, "%s route add blackhole %s; ", agent.routeTool, blockIP)
	}

	// Separated by ';' rather than '&&' so a failure to add one address still
	// runs every delete below.
	fmt.Fprintf(&script, "sleep %d; ", int(hold.Seconds()))

	for _, blockIP := range agent.blackholeIPs {
		fmt.Fprintf(&script, "%s route delete blackhole %s; ", agent.routeTool, blockIP)
	}

	GinkgoWriter.Printf("Blackholing %v inside agent pod %s for %s\n",
		agent.blackholeIPs, agent.podName, hold)
	output, execErr := agentPod.ExecCommandWithTimeout(
		[]string{"sh", "-c", script.String()},
		snrparams.IsolationExecTimeout,
		snrparams.ManagerContainerName)
	if execErr != nil {
		GinkgoWriter.Printf("WARNING: isolation exec on agent pod %s returned %v (output: %q)\n",
			agent.podName, execErr, output.String())
	}
}

// agentPodIsLive reports whether the named agent pod still exists and is
// Running. A pod that is Terminating, Failed, or merely a stale API object
// left behind by a rebooted node holds no network namespace: there is no
// blackhole route to delete and none to find. Exec against it fails with
// "container not found", which must not be mistaken for a surviving route.
func agentPodIsLive(podName string) (*pod.Builder, bool) {
	agentPod, pullErr := pod.Pull(APIClient, podName, medik8sparams.OperatorNs)
	if pullErr != nil {
		GinkgoWriter.Printf("agent pod %s is gone: %v\n", podName, pullErr)

		return nil, false
	}

	if agentPod.Object.Status.Phase != corev1.PodRunning || agentPod.Object.DeletionTimestamp != nil {
		GinkgoWriter.Printf("agent pod %s is %s (deleting=%t), so it holds no routes\n",
			podName, agentPod.Object.Status.Phase, agentPod.Object.DeletionTimestamp != nil)

		return nil, false
	}

	return agentPod, true
}

// removeBlackholeRoutes best-effort deletes any blackhole route left in the
// agent pod, and deletes the pod when one survives so the DaemonSet recreates
// it with a clean network namespace.
func removeBlackholeRoutes(agent isolatedAgent) {
	agentPod, live := agentPodIsLive(agent.podName)
	if !live {
		GinkgoWriter.Printf("removeBlackholeRoutes: nothing to clean for agent pod %s\n",
			agent.podName)

		return
	}

	var script strings.Builder

	for _, apiIP := range agent.blackholeIPs {
		fmt.Fprintf(&script, "%s route delete blackhole %s 2>/dev/null; ", agent.routeTool, apiIP)
	}

	fmt.Fprint(&script, "true")

	if _, execErr := agentPod.ExecCommandWithTimeout(
		[]string{"sh", "-c", script.String()},
		medik8sparams.DefaultTimeout,
		snrparams.ManagerContainerName); execErr != nil {
		GinkgoWriter.Printf("WARNING: failed to clean blackhole routes in agent pod %s: %v\n",
			agent.podName, execErr)
	}

	if checkErr := verifyNoBlackholeRoutes(agent); checkErr != nil {
		GinkgoWriter.Printf(
			"WARNING: %v -- deleting agent pod %s so the DaemonSet recreates it\n",
			checkErr, agent.podName)

		if _, delErr := agentPod.Delete(); delErr != nil {
			GinkgoWriter.Printf("WARNING: failed to delete agent pod %s: %v\n",
				agent.podName, delErr)
		}
	}
}

// ipFamilyFlag returns the iproute2 flag that pins a lookup to the family of
// the given address. "ip route show" dumps the IPv4 table whenever neither -4
// nor -6 is given, whatever family the selector belongs to, so an IPv6 address
// queried without -6 silently matches nothing.
func ipFamilyFlag(addr string) string {
	if strings.Contains(addr, ":") {
		return "-6"
	}

	return "-4"
}

// verifyNoBlackholeRoutes reports an error while the agent pod still has a
// blackhole route to any of the given addresses.
func verifyNoBlackholeRoutes(agent isolatedAgent) error {
	// A pod that is gone or no longer Running took its network namespace with
	// it; its replacement gets a fresh one. Either way there is no route left.
	agentPod, live := agentPodIsLive(agent.podName)
	if !live {
		return nil
	}

	// One lookup per address. "ip route show" takes a single prefix selector,
	// so handing it every address at once -- as a dual-stack Service reports,
	// let alone a whole peer list -- makes iproute2 reject the command, and the
	// check then says nothing about any of them.
	for _, apiIP := range agent.blackholeIPs {
		output, execErr := agentPod.ExecCommandWithTimeout(
			[]string{"sh", "-c", fmt.Sprintf("%s %s route show %s",
				agent.routeTool, ipFamilyFlag(apiIP), apiIP)},
			medik8sparams.DefaultTimeout,
			snrparams.ManagerContainerName)
		if execErr != nil {
			return fmt.Errorf("failed to read the route to %s in agent pod %s: %w",
				apiIP, agent.podName, execErr)
		}

		if strings.Contains(output.String(), "blackhole") {
			return fmt.Errorf("agent pod %s still blackholes %s: %s",
				agent.podName, apiIP, strings.TrimSpace(output.String()))
		}
	}

	return nil
}

// verifyNodeNotRebooted reports an error as soon as the node reports a new boot
// ID or stops being Ready.
func verifyNodeNotRebooted(ctx context.Context, nodeName, bootID string) error {
	node := &corev1.Node{}
	if err := APIClient.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return fmt.Errorf("failed to read node %s: %w", nodeName, err)
	}

	if node.Status.NodeInfo.BootID != bootID {
		return fmt.Errorf("node %s rebooted: boot ID went from %s to %s",
			nodeName, bootID, node.Status.NodeInfo.BootID)
	}

	if !helpers.IsNodeReady(node) {
		return fmt.Errorf("node %s is no longer Ready", nodeName)
	}

	return nil
}
