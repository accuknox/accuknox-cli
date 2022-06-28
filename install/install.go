package install

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/kubearmor/kubearmor-client/k8s"
	"gopkg.in/yaml.v2"

	"github.com/gofrs/flock"
	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/cli/values"
	"helm.sh/helm/v3/pkg/downloader"
	"helm.sh/helm/v3/pkg/getter"
	"helm.sh/helm/v3/pkg/repo"
	"helm.sh/helm/v3/pkg/strvals"

	appsv1 "k8s.io/api/apps/v1"
	rbacv1 "k8s.io/api/rbac/v1"
)

// Options -- options
type Options struct {
	Namespace string
}

var selectorLabels = map[string]string{
	"container": "knoxautopolicy",
}

var serviceLabels = map[string]string{
	"service": "knoxautopolicy",
}

var deploymentLabels = map[string]string{
	"deployment": "knoxautopolicy",
}

// GetService -- Get service details
func GetService(namespace string) *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   "knoxautopolicy",
			Labels: serviceLabels,
		},
		Spec: corev1.ServiceSpec{
			Selector: selectorLabels,
			Ports: []corev1.ServicePort{
				{
					Port:       9089,
					TargetPort: intstr.FromInt(9089),
					Protocol:   "TCP",
				},
			},
		},
	}
}

// GetDeployment -- Get deployment details
func GetDeployment(namespace string) *appsv1.Deployment {
	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Deployment",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "knoxautopolicy",
			Labels:    deploymentLabels,
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: selectorLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: selectorLabels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: "knoxautopolicy",
					Containers: []corev1.Container{
						{
							Name:            "knoxautopolicy",
							Image:           "accuknox/knoxautopolicy:dev",
							ImagePullPolicy: "Always",
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 9089,
									Protocol:      "TCP",
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "config-volume", //BPF (read-only)
									MountPath: "/conf",
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "config-volume",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "knoxautopolicy-config",
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// GetServiceAccount -- get service account
func GetServiceAccount(namespace string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ServiceAccount",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "knoxautopolicy",
			Namespace: namespace,
		},
	}
}

// GetClusterRoleBinding -- Get cluster role bindings
func GetClusterRoleBinding(namespace string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ClusterRoleBinding",
			APIVersion: "rbac.authorization.k8s.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "knoxautopolicy",
			Namespace: namespace,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      "knoxautopolicy",
				Namespace: namespace,
			},
		},
	}
}

var cm = corev1.ConfigMap{
	TypeMeta: metav1.TypeMeta{
		Kind:       "ConfigMap",
		APIVersion: "v1",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name:      "knoxautopolicy-config",
		Namespace: namespace,
	},
	Data: map[string]string{
		"conf.yaml": `application:
  name: knoxautopolicy
  network:
    operation-mode: 1                         # 1: cronjob | 2: one-time-job
    cron-job-time-interval: "0h0m10s"         # format: XhYmZs
    operation-trigger: 1000
    network-log-from: "hubble"                    # db|hubble
    network-log-file: "./flow.json"           # file path
    network-policy-to: "db"              # db, file
    network-policy-dir: "./"
    network-policy-types: 3
    network-policy-rule-types: 511
  system:
    operation-mode: 1                         # 1: cronjob | 2: one-time-job
    cron-job-time-interval: "0h0m10s"         # format: XhYmZs
    system-log-from: "kubearmor"                     # db|kubearmor
    system-log-file: "./log.json"             # file path
    system-policy-to: "db"               # db, file
    system-policy-dir: "./"
    deprecate-old-mode: true
  cluster:
    cluster-info-from: "k8sclient"            # k8sclient|accuknox

observability: true

database:
  driver: sqlite3
  host: mysql.explorer.svc.cluster.local
  port: 3306
  user: root
  password: password
  dbname: knoxautopolicy
  table-configuration: auto_policy_config
  table-network-log: network_log
  table-network-policy: network_policy
  table-system-log: system_log
  table-system-policy: system_policy

feed-consumer:
  kafka:
    broker-address-family: v4
    session-timeout-ms: 6000
    auto-offset-reset: "earliest"
    bootstrap-servers: "dev-kafka-kafka-bootstrap.accuknox-dev-kafka.svc.cluster.local:9092"
    group-id: policy.cilium
    topics:
    - cilium-telemetry-new
    - kubearmor-syslogs
    ssl:
      enabled: false
    events:
      buffer: 50

logging:
  level: "INFO"

# kubectl -n kube-system port-forward service/hubble-relay --address 0.0.0.0 --address :: 4245:80
cilium-hubble:
  url: hubble-relay.kube-system.svc.cluster.local
  port: 80

kubearmor:
  url: kubearmor.kube-system.svc.cluster.local
  port: 32767`,
	},
}

// DiscoveryEngineInstaller -- Installer for discovery engine
func DiscoveryEngineInstaller(c *k8s.Client, o Options) error {

	o.Namespace = "explorer"

	nsName := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: o.Namespace,
		},
	}

	// create explorer namespace
	if _, err := c.K8sClientset.CoreV1().Namespaces().Create(context.Background(), nsName, metav1.CreateOptions{}); err != nil {
		log.Print(err.Error())
	}

	// discovery-engine Service
	fmt.Print("Discovery-engine Service...\n")
	if _, err := c.K8sClientset.CoreV1().Services(o.Namespace).Create(context.Background(), GetService(o.Namespace), metav1.CreateOptions{}); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}
		fmt.Print("Discovery-engine Service already exists...\n")
	}

	// //discovery-engine dev-config
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}

	config, err := clientcmd.BuildConfigFromFlags("", path.Join(home, ".kube/config"))
	if err != nil {
		panic(err.Error())
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	namespace := "explorer"

	// Create Configmap
	created, err := client.
		CoreV1().
		ConfigMaps(namespace).
		Create(
			context.Background(),
			&cm,
			metav1.CreateOptions{},
		)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		if !reflect.DeepEqual(created.Data, cm.Data) {
			fmt.Print("WARN: existing ConfigMap has different data from the default one, not overwriting\n")
		}
	} else {
		fmt.Printf("Created ConfigMap %s/%s\n", namespace, created.GetName())
	}

	// discovery-engine Deployment
	fmt.Print("KubeArmor Relay Deployment...\n")
	if _, err := c.K8sClientset.AppsV1().Deployments(o.Namespace).Create(context.Background(), GetDeployment(o.Namespace), metav1.CreateOptions{}); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}
		fmt.Print("KubeArmor Relay Deployment already exists...\n")
	}

	// discovery-engine Service account
	fmt.Print("Service Account...\n")
	if _, err := c.K8sClientset.CoreV1().ServiceAccounts(o.Namespace).Create(context.Background(), GetServiceAccount(o.Namespace), metav1.CreateOptions{}); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}
		fmt.Print("Service Account already exists...\n")
	}

	// discovery-engine cluster role bindings
	fmt.Print("Cluster Role Bindings...\n")
	if _, err := c.K8sClientset.RbacV1().ClusterRoleBindings().Create(context.Background(), GetClusterRoleBinding(o.Namespace), metav1.CreateOptions{}); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}
		fmt.Print("Cluster Role Bindings already exists...\n")
	}

	return nil
}

var (
	url         = "https://charts.bitnami.com/bitnami"
	repoName    = "bitnami"
	chartName   = "mysql"
	releaseName = "mysql"
	namespace   = "explorer"
	args        = map[string]string{
		// comma seperated values to set
		"set": "auth.user=test-user,auth.password=password,auth.rootPassword=password,auth.database=knoxautopolicy,",
	}
)

var settings *cli.EnvSettings

// MySQLInstaller -- Install MySQL
func MySQLInstaller(c *k8s.Client) error {

	nsName := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "explorer",
		},
	}

	// create explorer namespace
	if _, err := c.K8sClientset.CoreV1().Namespaces().Create(context.Background(), nsName, metav1.CreateOptions{}); err != nil {
		log.Print(err.Error())
	}

	if err := os.Setenv("HELM_NAMESPACE", namespace); err != nil {
		return err
	}
	settings = cli.New()
	// Add helm repo
	RepoAdd(repoName, url)
	// Update charts from the helm repo
	RepoUpdate()
	// Install charts
	InstallChart(releaseName, repoName, chartName, args)
	return nil
}

// DiscoveryEngineUninstaller -- Un-installer for discovery engine
func DiscoveryEngineUninstaller(c *k8s.Client, o Options) error {

	//o.Namespace = "explorer"

	// discovery-engine Service
	fmt.Print("🔥 Deleting Discovery-engine Service...\n")
	if err := c.K8sClientset.CoreV1().Services(o.Namespace).Delete(context.Background(), "knoxautopolicy", metav1.DeleteOptions{}); err != nil {
		if !strings.Contains(err.Error(), "not found") {
			return err
		}
		fmt.Print("Discovery-engine Service not found...\n")
	}

	// discovery-engine dev-config
	fmt.Print("🔥 Deleting Configmap...\n")
	if err := c.K8sClientset.CoreV1().ConfigMaps(o.Namespace).Delete(context.Background(), "knoxautopolicy-config", metav1.DeleteOptions{}); err != nil {
		if !strings.Contains(err.Error(), "not found") {
			return err
		}
		fmt.Print("Configmap not found...\n")
	}

	// discovery-engine Deployment
	fmt.Print("🔥 Deleting KubeArmor Relay Deployment...\n")
	if err := c.K8sClientset.AppsV1().Deployments(o.Namespace).Delete(context.Background(), "knoxautopolicy", metav1.DeleteOptions{}); err != nil {
		if !strings.Contains(err.Error(), "not found") {
			return err
		}
		fmt.Print("KubeArmor Relay Deployment not found...\n")
	}

	// discovery-engine Service account
	fmt.Print("🔥 Deleting Service Account...\n")
	if err := c.K8sClientset.CoreV1().ServiceAccounts(o.Namespace).Delete(context.Background(), "knoxautopolicy", metav1.DeleteOptions{}); err != nil {
		if !strings.Contains(err.Error(), "not found") {
			return err
		}
		fmt.Print("Service Account not found...\n")
	}

	// discovery-engine cluster role bindings
	fmt.Print("🔥 Deleting Cluster Role Bindings...\n")
	if err := c.K8sClientset.RbacV1().ClusterRoleBindings().Delete(context.Background(), "knoxautopolicy", metav1.DeleteOptions{}); err != nil {
		if !strings.Contains(err.Error(), "not found") {
			return err
		}
		fmt.Print("Cluster Role Bindings not found...\n")
	}

	// Uninstall MySQL DB
	fmt.Print("🔥 Uninstalling MySQL...\n")
	if err := UninstallChart(releaseName, namespace); err != nil {
		return nil
	}
	return nil
}

// RepoAdd adds repo with given name and url
func RepoAdd(name, url string) {
	repoFile := settings.RepositoryConfig

	//Ensure the file directory exists as it is required for file locking
	err := os.MkdirAll(filepath.Dir(repoFile), os.ModePerm)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	// Acquire a file lock for process synchronization
	fileLock := flock.New(strings.Replace(repoFile, filepath.Ext(repoFile), ".lock", 1))
	lockCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	locked, err := fileLock.TryLockContext(lockCtx, time.Second)
	if err == nil && locked {
		defer fileLock.Unlock()
	}
	if err != nil {
		log.Fatal(err)
	}

	b, err := ioutil.ReadFile(filepath.Clean(repoFile))
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}

	var f repo.File
	if err := yaml.Unmarshal(b, &f); err != nil {
		log.Fatal(err)
	}

	if f.Has(name) {
		fmt.Printf("repository name (%s) already exists\n", name)
		return
	}

	c := repo.Entry{
		Name: name,
		URL:  url,
	}

	r, err := repo.NewChartRepository(&c, getter.All(settings))
	if err != nil {
		log.Fatal(err)
	}

	if _, err := r.DownloadIndexFile(); err != nil {
		err := errors.Wrapf(err, "looks like %q is not a valid chart repository or cannot be reached", url)
		log.Fatal(err)
	}

	f.Update(&c)

	if err := f.WriteFile(repoFile, 0644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%q has been added to your repositories\n", name)
}

// RepoUpdate updates charts for all helm repos
func RepoUpdate() {
	repoFile := settings.RepositoryConfig

	f, err := repo.LoadFile(repoFile)
	if os.IsNotExist(errors.Cause(err)) || len(f.Repositories) == 0 {
		log.Fatal(errors.New("no repositories found. You must add one before updating"))
	}
	var repos []*repo.ChartRepository
	for _, cfg := range f.Repositories {
		r, err := repo.NewChartRepository(cfg, getter.All(settings))
		if err != nil {
			log.Fatal(err)
		}
		repos = append(repos, r)
	}

	fmt.Printf("Hang tight while we grab the latest from your chart repositories...\n")
	var wg sync.WaitGroup
	for _, re := range repos {
		wg.Add(1)
		go func(re *repo.ChartRepository) {
			defer wg.Done()
			if _, err := re.DownloadIndexFile(); err != nil {
				fmt.Printf("...Unable to get an update from the %q chart repository (%s):\n\t%s\n", re.Config.Name, re.Config.URL, err)
			} else {
				fmt.Printf("...Successfully got an update from the %q chart repository\n", re.Config.Name)
			}
		}(re)
	}
	wg.Wait()
	fmt.Printf("Update Complete. ⎈ Happy Helming!⎈\n")
}

// InstallChart -- Install Helm chart
func InstallChart(name, repo, chart string, args map[string]string) {
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(settings.RESTClientGetter(), settings.Namespace(), os.Getenv("HELM_DRIVER"), debug); err != nil {
		log.Fatal(err)
	}
	client := action.NewInstall(actionConfig)

	if client.Version == "" && client.Devel {
		client.Version = ">0.0.0-0"
	}
	//name, chart, err := client.NameAndChart(args)
	client.ReleaseName = name
	cp, err := client.ChartPathOptions.LocateChart(fmt.Sprintf("%s/%s", repo, chart), settings)
	if err != nil {
		log.Fatal(err)
	}

	debug("CHART PATH: %s\n", cp)

	p := getter.All(settings)
	valueOpts := &values.Options{}
	vals, err := valueOpts.MergeValues(p)
	if err != nil {
		log.Fatal(err)
	}

	// Add args
	if err := strvals.ParseInto(args["set"], vals); err != nil {
		log.Fatal(errors.Wrap(err, "failed parsing --set data"))
	}

	// Check chart dependencies to make sure all are present in /charts
	chartRequested, err := loader.Load(cp)
	if err != nil {
		log.Fatal(err)
	}

	validInstallableChart, err := isChartInstallable(chartRequested)
	if !validInstallableChart {
		log.Fatal(err)
	}

	if req := chartRequested.Metadata.Dependencies; req != nil {
		// If CheckDependencies returns an error, we have unfulfilled dependencies.
		// As of Helm 2.4.0, this is treated as a stopping condition:
		// https://github.com/helm/helm/issues/2209
		if err := action.CheckDependencies(chartRequested, req); err != nil {
			if client.DependencyUpdate {
				man := &downloader.Manager{
					Out:              os.Stdout,
					ChartPath:        cp,
					Keyring:          client.ChartPathOptions.Keyring,
					SkipUpdate:       false,
					Getters:          p,
					RepositoryConfig: settings.RepositoryConfig,
					RepositoryCache:  settings.RepositoryCache,
				}
				if err := man.Update(); err != nil {
					log.Fatal(err)
				}
			} else {
				log.Fatal(err)
			}
		}
	}

	client.Namespace = settings.Namespace()
	if _, err := client.Run(chartRequested, vals); err != nil {
		log.Fatal(err)
	}
}

// UninstallChart -- uninstall chart
func UninstallChart(name, namespace string) error {
	if err := os.Setenv("HELM_NAMESPACE", namespace); err != nil {
		return err
	}
	settings = cli.New()
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(settings.RESTClientGetter(), settings.Namespace(), os.Getenv("HELM_DRIVER"), debug); err != nil {
		return err
	}

	client := action.NewUninstall(actionConfig)

	if _, err := client.Run(name); err != nil {
		return err
	}

	fmt.Printf("Uninstalled release, name %s \n", name)
	return nil
}

func isChartInstallable(ch *chart.Chart) (bool, error) {
	switch ch.Metadata.Type {
	case "", "application":
		return true, nil
	}
	return false, errors.Errorf("%s charts are not installable", ch.Metadata.Type)
}

func debug(format string, v ...interface{}) {
	format = fmt.Sprintf("[debug] %s\n", format)
	if err := log.Output(2, fmt.Sprintf(format, v...)); err != nil {
		log.Print(err.Error())
	}
}
