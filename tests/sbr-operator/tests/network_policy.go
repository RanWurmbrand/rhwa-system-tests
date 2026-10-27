package tests

import (
	"context"
	"fmt"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/reportxml"

	"github.com/medik8s/system-tests/tests/internal/helpers"
	"github.com/medik8s/system-tests/tests/internal/labels"
	. "github.com/medik8s/system-tests/tests/internal/medik8sinittools"
	"github.com/medik8s/system-tests/tests/internal/medik8sparams"
	"github.com/medik8s/system-tests/tests/sbr-operator/internal/sbrparams"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// networkPolicyName returns the NetworkPolicy name the SBRC controller renders for the given
// StorageBasedRemediationConfig name: "sbr-agent-<sbrcName>", identical to the agent DaemonSet's
// naming scheme.
func networkPolicyName(sbrcName string) string {
	return sbrparams.NetworkPolicyNamePrefix + sbrcName
}

// waitForNetworkPolicyGC polls until the named NetworkPolicy is gone (GC'd after its owning
// StorageBasedRemediationConfig is deleted).
func waitForNetworkPolicyGC(npName string) {
	Eventually(func() error {
		getErr := APIClient.Get(context.TODO(),
			types.NamespacedName{Name: npName, Namespace: medik8sparams.OperatorNs}, &networkingv1.NetworkPolicy{})
		if k8serrors.IsNotFound(getErr) {
			return nil
		}

		if getErr != nil {
			return getErr
		}

		return fmt.Errorf("NetworkPolicy %s still present; waiting for GC", npName)
	}, sbrparams.SBRCDaemonSetGCTimeout, sbrparams.DefaultPollInterval).Should(Succeed(),
		"NetworkPolicy %s must be GC-d after its StorageBasedRemediationConfig is deleted", npName)
}

// verifyNetworkPolicyRecreated reports a non-nil error unless the named NetworkPolicy exists
// again with a UID different from originalUID, confirming the operator recreated it rather than
// the pre-deletion object lingering. Intended as an Eventually body.
func verifyNetworkPolicyRecreated(npName string, originalUID types.UID) error {
	recreated := &networkingv1.NetworkPolicy{}
	if err := APIClient.Get(context.TODO(),
		types.NamespacedName{Name: npName, Namespace: medik8sparams.OperatorNs}, recreated); err != nil {
		return err
	}

	if recreated.UID == originalUID {
		return fmt.Errorf("NetworkPolicy %q still has the pre-deletion UID %s", npName, originalUID)
	}

	return nil
}

// verifySBRNetworkPolicyIngressFrom reports a non-nil error unless the
// NetworkPolicy's single ingress rule restricts its peer to exactly the namespaces carrying
// sbrparams.MetricsNamespaceSelectorLabelKey/Value, with no podSelector or ipBlock peer.
func verifySBRNetworkPolicyIngressFrom(networkPolicy *networkingv1.NetworkPolicy) error {
	if len(networkPolicy.Spec.Ingress) != 1 {
		return fmt.Errorf("NetworkPolicy %q must have exactly one ingress rule, has %d",
			networkPolicy.Name, len(networkPolicy.Spec.Ingress))
	}

	from := networkPolicy.Spec.Ingress[0].From
	if len(from) != 1 {
		return fmt.Errorf("NetworkPolicy %q ingress rule must have exactly one peer, has %d",
			networkPolicy.Name, len(from))
	}

	if from[0].PodSelector != nil {
		return fmt.Errorf("NetworkPolicy %q ingress peer must not be a podSelector", networkPolicy.Name)
	}

	if from[0].IPBlock != nil {
		return fmt.Errorf("NetworkPolicy %q ingress peer must not be an ipBlock", networkPolicy.Name)
	}

	if from[0].NamespaceSelector == nil {
		return fmt.Errorf("NetworkPolicy %q ingress peer must be a namespaceSelector", networkPolicy.Name)
	}

	if from[0].NamespaceSelector.MatchLabels[sbrparams.MetricsNamespaceSelectorLabelKey] !=
		sbrparams.MetricsNamespaceSelectorLabelValue {
		return fmt.Errorf("NetworkPolicy %q ingress namespaceSelector must match %s=%s, got %v",
			networkPolicy.Name, sbrparams.MetricsNamespaceSelectorLabelKey, sbrparams.MetricsNamespaceSelectorLabelValue,
			from[0].NamespaceSelector.MatchLabels)
	}

	return nil
}

// verifySBRNetworkPolicyIngress asserts the full ingress shape
func verifySBRNetworkPolicyIngress(networkPolicy *networkingv1.NetworkPolicy) {
	Expect(networkPolicy.Spec.Ingress).To(HaveLen(1),
		"NetworkPolicy %q must have exactly one ingress rule", networkPolicy.Name)
	ingress := networkPolicy.Spec.Ingress[0]
	agentMetricsPort, convErr := strconv.Atoi(sbrparams.AgentMetricsPort)
	Expect(convErr).ToNot(HaveOccurred(), "Failed to parse sbrparams.AgentMetricsPort %q", sbrparams.AgentMetricsPort)
	Expect(ingress.Ports).To(HaveLen(2),
		"NetworkPolicy %q ingress rule must allow exactly two ports", networkPolicy.Name)
	Expect(ingress.Ports[0].Port.IntValue()).To(Equal(int(sbrparams.RuntimeMetricsPort)),
		"NetworkPolicy %q ingress rule's first port must be the runtime metrics port (%d)",
		networkPolicy.Name, sbrparams.RuntimeMetricsPort)
	Expect(ingress.Ports[0].Protocol).ToNot(BeNil())
	Expect(*ingress.Ports[0].Protocol).To(Equal(corev1.ProtocolTCP),
		"NetworkPolicy %q ingress rule's first port must be TCP", networkPolicy.Name)
	Expect(ingress.Ports[1].Port.IntValue()).To(Equal(agentMetricsPort),
		"NetworkPolicy %q ingress rule's second port must be the agent metrics port (%d)",
		networkPolicy.Name, agentMetricsPort)
	Expect(ingress.Ports[1].Protocol).ToNot(BeNil())
	Expect(*ingress.Ports[1].Protocol).To(Equal(corev1.ProtocolTCP),
		"NetworkPolicy %q ingress rule's second port must be TCP", networkPolicy.Name)
	Expect(verifySBRNetworkPolicyIngressFrom(networkPolicy)).To(Succeed())
}

// verifyMetricsNamespaceLabelWarning checks whether sbrparams.ExpectedOpenShiftMonitoringNamespace
// carries the metrics label the agent NetworkPolicy's ingress rule requires. Every real OpenShift
// cluster has this namespace
func verifyMetricsNamespaceLabelWarning(sbrcName, sbrcUID string) {
	monitoringNamespace := &corev1.Namespace{}
	getErr := APIClient.Get(context.TODO(),
		types.NamespacedName{Name: sbrparams.ExpectedOpenShiftMonitoringNamespace}, monitoringNamespace)
	if k8serrors.IsNotFound(getErr) {
		AddReportEntry("sbr-networkpolicy-metrics-warning-not-applicable", fmt.Sprintf(
			"namespace %s does not exist (not OpenShift, or monitoring is not installed); "+
				"skipping the metrics-namespace-label-missing warning assertion",
			sbrparams.ExpectedOpenShiftMonitoringNamespace))

		return
	}
	Expect(getErr).ToNot(HaveOccurred(),
		"Failed to get namespace %s", sbrparams.ExpectedOpenShiftMonitoringNamespace)

	if monitoringNamespace.Labels[sbrparams.MetricsNamespaceSelectorLabelKey] ==
		sbrparams.MetricsNamespaceSelectorLabelValue {
		AddReportEntry("sbr-networkpolicy-metrics-warning-not-applicable", fmt.Sprintf(
			"namespace %s already carries %s=%s; cannot exercise the missing-label warning path",
			sbrparams.ExpectedOpenShiftMonitoringNamespace,
			sbrparams.MetricsNamespaceSelectorLabelKey, sbrparams.MetricsNamespaceSelectorLabelValue))

		return
	}

	Expect(helpers.WaitForEvents(context.TODO(), EventsClient,
		helpers.InvolvedObjectRef{
			Kind:      "StorageBasedRemediationConfig",
			Name:      sbrcName,
			Namespace: medik8sparams.OperatorNs,
			UID:       sbrcUID,
		},
		[]helpers.EventExpectation{
			{
				Reason:        sbrparams.MetricsNamespaceLabelMissingEventReason,
				Type:          corev1.EventTypeWarning,
				MessageSubstr: sbrparams.ExpectedOpenShiftMonitoringNamespace,
			},
		},
		sbrparams.SBRCReadyTimeout, sbrparams.DefaultPollInterval,
	)).To(Succeed(), "StorageBasedRemediationConfig %q must get a %s warning event referencing namespace %q",
		sbrcName, sbrparams.MetricsNamespaceLabelMissingEventReason, sbrparams.ExpectedOpenShiftMonitoringNamespace)
}

var _ = Describe(
	"SBR Functional - Agent NetworkPolicy",
	Ordered,
	Label(
		labels.OperatorSBR,
		labels.TierSmoke,
		labels.FrequencyPresubmit,
		labels.DisruptionNonDestructive,
		labels.PlatformAny,
		labels.ComponentNetworkPolicy,
	), func() {
		var rwxStorageClass string

		BeforeAll(func() {
			By("Discovering RWX-capable StorageClass (required for the StorageBasedRemediationConfig to pass validation)")

			rwxStorageClass = discoverRWXStorageClass()

			By("Cleaning up any leftover network-policy test SBRC from prior runs")

			staleRef := buildSBRC(sbrparams.SBRCNetworkPolicyTestName, map[string]interface{}{})
			deleteErr := APIClient.Delete(context.TODO(), staleRef)
			if deleteErr != nil && !k8serrors.IsNotFound(deleteErr) {
				GinkgoT().Logf("Warning: pre-test cleanup of stale SBRC %s failed: %v",
					sbrparams.SBRCNetworkPolicyTestName, deleteErr)
			}

			By("Waiting for the stale SBRC to be fully deleted (finalizer drain)")

			Eventually(func() error {
				getErr := APIClient.Get(context.TODO(),
					types.NamespacedName{Name: sbrparams.SBRCNetworkPolicyTestName, Namespace: medik8sparams.OperatorNs},
					buildSBRC(sbrparams.SBRCNetworkPolicyTestName, map[string]interface{}{}))
				if k8serrors.IsNotFound(getErr) {
					return nil
				}

				if getErr != nil {
					return getErr
				}

				return fmt.Errorf("SBRC %s still terminating", sbrparams.SBRCNetworkPolicyTestName)
			}, medik8sparams.DefaultTimeout, sbrparams.DefaultPollInterval).Should(Succeed(),
				"Stale SBRC %s must be fully deleted before the test recreates it", sbrparams.SBRCNetworkPolicyTestName)

			By("Waiting for any stale network-policy test resources to be garbage-collected")

			waitForDaemonSetGC(sbrparams.SBRAgentDaemonSetPrefix + sbrparams.SBRCNetworkPolicyTestName)
			waitForNetworkPolicyGC(networkPolicyName(sbrparams.SBRCNetworkPolicyTestName))
		})

		It("Verify SBR manages the agent DaemonSet NetworkPolicy",
			reportxml.ID("1970"),
			func() {
				npName := networkPolicyName(sbrparams.SBRCNetworkPolicyTestName)

				By("Creating a StorageBasedRemediationConfig with a non-matching nodeSelector so the " +
					"agent DaemonSet renders without scheduling pods")

				sbrc := buildSBRC(sbrparams.SBRCNetworkPolicyTestName, map[string]interface{}{
					"sharedStorageClass": rwxStorageClass,
					// No node matches this selector, so the NetworkPolicy and DaemonSet render in
					// full while zero agent pods are ever scheduled; the test never disturbs a node.
					"nodeSelector": map[string]interface{}{
						sbrparams.SBRCNoMatchSelectorKey: sbrparams.SBRCNoMatchSelectorValue,
					},
				})

				DeferCleanup(func() {
					By("DeferCleanup: deleting the network-policy test StorageBasedRemediationConfig")

					Eventually(func() error {
						delErr := APIClient.Delete(context.TODO(), sbrc.DeepCopy())
						if delErr == nil || k8serrors.IsNotFound(delErr) {
							return nil
						}

						return delErr
					}, medik8sparams.DefaultTimeout, sbrparams.DefaultPollInterval).Should(Succeed(),
						"DeferCleanup: failed to delete SBRC %s", sbrparams.SBRCNetworkPolicyTestName)

					waitForDaemonSetGC(sbrparams.SBRAgentDaemonSetPrefix + sbrparams.SBRCNetworkPolicyTestName)
					waitForNetworkPolicyGC(npName)
				})

				Expect(APIClient.Create(context.TODO(), sbrc)).To(Succeed(),
					"StorageBasedRemediationConfig %q must be created successfully", sbrparams.SBRCNetworkPolicyTestName)

				By(fmt.Sprintf("Verifying the NetworkPolicy %s exists for the StorageBasedRemediationConfig", npName))

				networkPolicy := &networkingv1.NetworkPolicy{}
				Eventually(func() error {
					return APIClient.Get(context.TODO(),
						types.NamespacedName{Name: npName, Namespace: medik8sparams.OperatorNs}, networkPolicy)
				}, sbrparams.SBRCReadyTimeout, sbrparams.DefaultPollInterval).Should(Succeed(),
					"NetworkPolicy %q must exist", npName)

				By("Verifying the NetworkPolicy is owned by the StorageBasedRemediationConfig CR")

				Expect(networkPolicy.OwnerReferences).To(HaveLen(1),
					"NetworkPolicy %q should have exactly one owner reference", npName)
				owner := networkPolicy.OwnerReferences[0]
				Expect(owner.Kind).To(Equal("StorageBasedRemediationConfig"),
					"NetworkPolicy %q owner reference kind must be StorageBasedRemediationConfig", npName)
				Expect(owner.Name).To(Equal(sbrparams.SBRCNetworkPolicyTestName),
					"NetworkPolicy %q owner reference name must be %q", npName, sbrparams.SBRCNetworkPolicyTestName)
				Expect(owner.Controller).ToNot(BeNil(),
					"NetworkPolicy %q owner reference must set controller", npName)
				Expect(*owner.Controller).To(BeTrue(),
					"NetworkPolicy %q owner reference must be the controlling owner", npName)

				By("Verifying the NetworkPolicy selects the agent pods for this StorageBasedRemediationConfig")

				Expect(networkPolicy.Spec.PodSelector.MatchLabels).To(Equal(map[string]string{
					"app":       sbrparams.AgentAppLabelValue,
					"sbrconfig": sbrparams.SBRCNetworkPolicyTestName,
				}), "NetworkPolicy %q must select only the agent pods for this StorageBasedRemediationConfig", npName)

				By("Verifying the NetworkPolicy restricts both ingress and egress")

				Expect(networkPolicy.Spec.PolicyTypes).To(ConsistOf(
					networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress),
					"NetworkPolicy %q must restrict both ingress and egress", npName)

				By("Verifying ingress is limited to the two metrics ports, scoped to namespaces labeled metrics: enabled")

				verifySBRNetworkPolicyIngress(networkPolicy)

				By("Verifying egress is intentionally left unrestricted (allow-all, documented design decision)")

				Expect(networkPolicy.Spec.Egress).To(HaveLen(1),
					"NetworkPolicy %q must have exactly one egress rule", npName)
				Expect(networkPolicy.Spec.Egress[0].To).To(BeEmpty(),
					"NetworkPolicy %q egress rule must have no peer restriction", npName)
				Expect(networkPolicy.Spec.Egress[0].Ports).To(BeEmpty(),
					"NetworkPolicy %q egress rule must have no port restriction", npName)

				By("Deleting the NetworkPolicy and verifying the operator recreates it")

				originalUID := networkPolicy.UID
				Expect(APIClient.Delete(context.TODO(), networkPolicy.DeepCopy())).To(Succeed(),
					"Failed to delete NetworkPolicy %q", npName)

				Eventually(func() error {
					return verifyNetworkPolicyRecreated(npName, originalUID)
				}, sbrparams.NetworkPolicyRecreateTimeout, sbrparams.DefaultPollInterval).Should(Succeed(),
					"NetworkPolicy %q must be recreated by the operator after deletion", npName)

				By("Manually widening the ingress rule and verifying reconciliation restores the namespace-scoped restriction")

				driftTarget := &networkingv1.NetworkPolicy{}
				Expect(APIClient.Get(context.TODO(),
					types.NamespacedName{Name: npName, Namespace: medik8sparams.OperatorNs}, driftTarget)).To(Succeed(),
					"Failed to re-fetch NetworkPolicy %q before simulating drift", npName)
				Expect(driftTarget.Spec.Ingress).ToNot(BeEmpty(),
					"NetworkPolicy %q must still have its ingress rule before simulating drift", npName)
				driftTarget.Spec.Ingress[0].From = nil
				Expect(APIClient.Update(context.TODO(), driftTarget)).To(Succeed(),
					"Failed to widen NetworkPolicy %q ingress rule to simulate drift", npName)

				Eventually(func() error {
					restored := &networkingv1.NetworkPolicy{}
					if getErr := APIClient.Get(context.TODO(),
						types.NamespacedName{Name: npName, Namespace: medik8sparams.OperatorNs}, restored); getErr != nil {
						return getErr
					}

					return verifySBRNetworkPolicyIngressFrom(restored)
				}, sbrparams.NetworkPolicyRecreateTimeout, sbrparams.DefaultPollInterval).Should(Succeed(),
					"NetworkPolicy %q ingress rule must be restored to the namespace-scoped restriction after drift", npName)

				By("Verifying the operator warns when the OpenShift monitoring namespace lacks the metrics label")

				verifyMetricsNamespaceLabelWarning(sbrparams.SBRCNetworkPolicyTestName, string(sbrc.GetUID()))
			})
	})
