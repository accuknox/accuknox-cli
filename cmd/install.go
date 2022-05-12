package cmd

import (
	"context"
	"os"
	"time"

	di "github.com/accuknox/accuknox-cli/install"
	"github.com/cilium/cilium-cli/defaults"
	"github.com/cilium/cilium-cli/hubble"
	ci "github.com/cilium/cilium-cli/install"
	ki "github.com/kubearmor/kubearmor-client/install"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var (
	namespace      string
	installOptions ki.Options
	params         = ci.Parameters{Writer: os.Stdout}
	hparams        = hubble.Parameters{Writer: os.Stdout}
	diOptions      di.Options
)

const (
	encryptionDisabled  = "disabled"
	encryptionIPsec     = "ipsec"
	encryptionWireguard = "wireguard"
)

// installCmd represents the get command
var installCmd = &cobra.Command{
	Use:   "install",
	Short: "Install KubeArmor, Cilium and Discovery-engine in a Kubernetes Cluster",
	Long:  `Install KubeArmor, Cilium and Discovery-engine in a Kubernetes Clusters`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Install Cilium
		params.Namespace = namespace
		installer, err := ci.NewK8sInstaller(k8sClient, params)
		if err != nil {
			return err
		}
		cmd.SilenceUsage = true
		if err := installer.Install(context.Background()); err != nil {
			installer.RollbackInstallation(context.Background())

			log.Error().Msgf("Unable to install Cilium: %s", err.Error())
		}

		// Enable cilium hubble
		hparams.Namespace = namespace
		hparams.Relay = true
		hparams.HelmValuesSecretName = defaults.HelmValuesSecretName
		hparams.RedactHelmCertKeys = true
		hparams.CreateCA = true
		h := hubble.NewK8sHubble(k8sClient, hparams)
		if err := h.Enable(context.Background()); err != nil {
			log.Error().Msgf("Unable to enable Hubble: %s", err.Error())
		}

		// Install KubeArmor
		installOptions.Namespace = namespace
		if err := ki.K8sInstaller(client, installOptions); err != nil {
			return err
		}

		// Install MySQL DB
		installOptions.Namespace = namespace
		if err := di.MySQLInstaller(client); err != nil {
			return err
		}

		// Install dscovery-engine
		diOptions.Namespace = namespace
		// diOptions.Namespace = "explorer"
		if err := di.DiscoveryEngineInstaller(client, diOptions); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(installCmd)
	//kubearmor
	installCmd.Flags().StringVarP(&installOptions.KubearmorImage, "image", "i", "kubearmor/kubearmor:stable", "Kubearmor daemonset image to use")
	installCmd.Flags().StringVarP(&namespace, "namespace", "n", "kube-system", "Namespace for resources")

	//cilium
	installCmd.Flags().StringVar(&params.Version, "version", defaults.Version, "Cilium version to install")
	installCmd.Flags().StringVar(&params.BaseVersion, "base-version", defaults.Version,
		"Specify the base Cilium version for configuration purpose in case the --version flag doesn't indicate the actual Cilium version")
	installCmd.Flags().MarkHidden("base-version")
	installCmd.Flags().IntVar(&params.ClusterID, "cluster-id", 0, "Unique cluster identifier for multi-cluster")
	installCmd.Flags().BoolVar(&params.Wait, "wait", true, "Wait for status to report success (no errors)")
	installCmd.Flags().DurationVar(&params.WaitDuration, "wait-duration", defaults.StatusWaitDuration, "Maximum time to wait for status")
	installCmd.Flags().BoolVar(&params.RestartUnmanagedPods, "restart-unmanaged-pods", true, "Restart pods which are not being managed by Cilium")
	installCmd.Flags().StringVar(&params.Encryption, "encryption", "disabled", "Enable encryption of all workloads traffic { disabled | ipsec | wireguard }")
	// It can be deprecated since we have a helm option for it
	installCmd.Flags().BoolVar(&params.NodeEncryption, "node-encryption", false, "Enable encryption of all node to node traffic")
	// It can be deprecated since we have a helm option for it
	installCmd.Flags().DurationVar(&params.CiliumReadyTimeout, "cilium-ready-timeout", 5*time.Minute,
		"Timeout for Cilium to become ready before restarting unmanaged pods")
	installCmd.Flags().BoolVar(&params.Rollback, "rollback", true, "Roll back installed resources on failure")

	// It can be deprecated since we have a helm option for it
	installCmd.Flags().StringVar(&params.HelmValuesSecretName, "helm-values-secret-name", defaults.HelmValuesSecretName, "Secret name to store the auto-generated helm values file. The namespace is the same as where Cilium will be installed")
}