package client

import (
	"fmt"
	"strings"

	sdk "github.com/openshift-online/ocm-sdk-go"
	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"github.com/openshift/osdctl/cmd/common"
	"github.com/openshift/osdctl/cmd/hcp/restore/internal/restorer"
	"github.com/openshift/osdctl/pkg/utils"
	logrus "github.com/sirupsen/logrus"
	workv1 "open-cluster-management.io/api/work/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewDefaultClusterContextGetter(conn *sdk.Connection, opts ...DefaultClusterContextGetterOption) *DefaultClusterContextGetter {
	var cfg DefaultClusterContextGetterConfig
	cfg.Options(opts...)
	cfg.Default()

	return &DefaultClusterContextGetter{
		cfg:  cfg,
		conn: conn,
	}
}

type DefaultClusterContextGetter struct {
	cfg  DefaultClusterContextGetterConfig
	conn *sdk.Connection
}

type DefaultClusterContextGetterConfig struct {
	Logger *logrus.Logger
}

func (c *DefaultClusterContextGetterConfig) Options(opts ...DefaultClusterContextGetterOption) {
	for _, opt := range opts {
		opt.ConfgiureDefaultClusterContextGetter(c)
	}
}

func (c *DefaultClusterContextGetterConfig) Default() {
	if c.Logger == nil {
		c.Logger = logrus.New()
	}
}

type DefaultClusterContextGetterOption interface {
	ConfgiureDefaultClusterContextGetter(*DefaultClusterContextGetterConfig)
}

// GetClusterContext resolves the cluster topology via OCM, creates elevated
// K8s clients for the management and service clusters, and returns a
// ClusterContext with scoped sub-clients and the resolved namespaces.
func (c *DefaultClusterContextGetter) GetClusterContext(clusterID string, opts ...restorer.GetClusterContextOption) (*restorer.ClusterContext, error) {
	var cfg restorer.GetClusterContextConfig
	cfg.Options(opts...)

	cluster, err := utils.GetClusterAnyStatus(c.conn, clusterID)
	if err != nil {
		return nil, fmt.Errorf("finding cluster %q: %w", clusterID, err)
	}

	if !cluster.Hypershift().Enabled() {
		return nil, fmt.Errorf("cluster %q is not an HCP cluster", clusterID)
	}

	internalID := cluster.ID()

	// Resolve management cluster
	hypershiftResp, err := c.conn.ClustersMgmt().V1().Clusters().
		Cluster(internalID).
		Hypershift().
		Get().
		Send()
	if err != nil {
		return nil, fmt.Errorf("getting hypershift info for cluster %q: %w", internalID, err)
	}

	mgmtClusterName := hypershiftResp.Body().ManagementCluster()
	if mgmtClusterName == "" {
		return nil, fmt.Errorf("no management cluster found for %s", internalID)
	}

	hcpNamespace, _ := hypershiftResp.Body().GetHCPNamespace()
	if hcpNamespace == "" {
		return nil, fmt.Errorf("no HCP namespace found for %s", internalID)
	}

	// Derive HCNamespace from HCPNamespace: HCPNamespace is
	// "<prefix>-<clusterID>-<domainPrefix>", HCNamespace is "<prefix>-<clusterID>".
	hcNamespace := strings.SplitAfter(hcpNamespace, internalID)[0]

	mcCluster, err := utils.GetClusterAnyStatus(c.conn, mgmtClusterName)
	if err != nil {
		return nil, fmt.Errorf("resolving management cluster %q: %w", mgmtClusterName, err)
	}

	// Resolve service cluster via OSD Fleet Management
	ofmResp, err := c.conn.OSDFleetMgmt().V1().ManagementClusters().
		List().
		Parameter("search", fmt.Sprintf("name='%s'", mgmtClusterName)).
		Send()
	if err != nil {
		return nil, fmt.Errorf("getting fleet manager info for management cluster %s: %w", mgmtClusterName, err)
	}

	svcClusterName := ""
	if ofmResp.Items().Len() > 0 {
		if kind := ofmResp.Items().Get(0).Parent().Kind(); kind == "ServiceCluster" {
			svcClusterName = ofmResp.Items().Get(0).Parent().Name()
		}
	}
	if svcClusterName == "" {
		return nil, fmt.Errorf("resolving service cluster for management cluster %s", mgmtClusterName)
	}

	scCluster, err := utils.GetClusterAnyStatus(c.conn, svcClusterName)
	if err != nil {
		return nil, fmt.Errorf("resolving service cluster %q: %w", svcClusterName, err)
	}

	var elevationReasons []string
	if cfg.Reason != "" {
		elevationReasons = append(elevationReasons, cfg.Reason)
	}

	// Create elevated K8s clients (for mutating operations)
	_, mcConfig, _, err := common.GetKubeConfigAndClientWithConn(mcCluster.ID(), c.conn, elevationReasons...)
	if err != nil {
		return nil, fmt.Errorf("creating MC elevated K8s client: %w", err)
	}
	mcK8s, err := client.NewWithWatch(mcConfig, client.Options{})
	if err != nil {
		return nil, fmt.Errorf("creating MC elevated watch client: %w", err)
	}
	if err := addSchemes(mcK8s); err != nil {
		return nil, err
	}

	// Create non-elevated K8s clients (for read-only operations)
	_, mcConfigNoElevation, _, err := common.GetKubeConfigAndClientWithConn(mcCluster.ID(), c.conn)
	if err != nil {
		return nil, fmt.Errorf("creating MC K8s client: %w", err)
	}
	mcK8sNoElevation, err := client.NewWithWatch(mcConfigNoElevation, client.Options{})
	if err != nil {
		return nil, fmt.Errorf("creating MC watch client: %w", err)
	}
	if err := addSchemes(mcK8sNoElevation); err != nil {
		return nil, err
	}

	scK8s, _, _, err := common.GetKubeConfigAndClientWithConn(scCluster.ID(), c.conn, elevationReasons...)
	if err != nil {
		return nil, fmt.Errorf("creating SC elevated K8s client: %w", err)
	}
	if err := addSchemes(scK8s); err != nil {
		return nil, err
	}

	scK8sNoElevation, _, _, err := common.GetKubeConfigAndClientWithConn(scCluster.ID(), c.conn)
	if err != nil {
		return nil, fmt.Errorf("creating SC K8s client: %w", err)
	}
	if err := addSchemes(scK8sNoElevation); err != nil {
		return nil, err
	}

	c.cfg.Logger.WithFields(logrus.Fields{
		"cluster":       cluster.Name(),
		"cluster_id":    internalID,
		"mc":            mcCluster.Name(),
		"mc_id":         mcCluster.ID(),
		"sc":            scCluster.Name(),
		"sc_id":         scCluster.ID(),
		"hc_namespace":  hcNamespace,
		"hcp_namespace": hcpNamespace,
	}).Info("Resolved cluster topology")

	return &restorer.ClusterContext{
		ClusterID:    internalID,
		HCNamespace:  hcNamespace,
		HCPNamespace: hcpNamespace,
		MC:           NewDefaultMCClient(mcK8s, mcK8sNoElevation, clusterID),
		SC:           NewDefaultSCCLient(mcCluster.Name(), scK8s, scK8sNoElevation),
	}, nil
}

func addSchemes(c client.Client) error {
	if err := workv1.AddToScheme(c.Scheme()); err != nil {
		return fmt.Errorf("adding ManifestWork scheme: %w", err)
	}
	if err := hypershiftv1beta1.AddToScheme(c.Scheme()); err != nil {
		return fmt.Errorf("adding HyperShift scheme: %w", err)
	}
	return nil
}
