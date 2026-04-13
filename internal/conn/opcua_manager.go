package conn

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	gopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

type OpcuaConfig struct {
	Endpoint       string
	SecurityPolicy string
	SecurityMode   string
	Username       string
	Password       string
}

type OpcuaManager struct {
	mu      sync.Mutex
	clients map[string]*opcuaClientHolder
}

type opcuaClientHolder struct {
	cfg    OpcuaConfig
	client *gopcua.Client
}

var opcuaDefaultHolder struct {
	mu  sync.Mutex
	mgr *OpcuaManager
}

func DefaultOpcuaManager() *OpcuaManager {
	opcuaDefaultHolder.mu.Lock()
	defer opcuaDefaultHolder.mu.Unlock()
	if opcuaDefaultHolder.mgr == nil {
		opcuaDefaultHolder.mgr = NewOpcuaManager()
	}
	return opcuaDefaultHolder.mgr
}

func NewOpcuaManager() *OpcuaManager {
	return &OpcuaManager{clients: make(map[string]*opcuaClientHolder)}
}

func (m *OpcuaManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, holder := range m.clients {
		if holder.client != nil {
			_ = holder.client.CloseWithContext(context.Background())
		}
		delete(m.clients, key)
	}
	return nil
}

func (m *OpcuaManager) ReadNodes(ctx context.Context, cfg OpcuaConfig, nodeIDs []string) ([]*ua.DataValue, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	client, err := m.ensureClient(ctx, cfg)
	if err != nil {
		return nil, err
	}

	nodes := make([]*ua.ReadValueID, 0, len(nodeIDs))
	for _, raw := range nodeIDs {
		id, err := ua.ParseNodeID(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("opcua: invalid nodeId %q: %w", raw, err)
		}
		nodes = append(nodes, &ua.ReadValueID{NodeID: id, AttributeID: ua.AttributeIDValue})
	}

	resp, err := client.ReadWithContext(ctx, &ua.ReadRequest{NodesToRead: nodes})
	if err != nil {
		return nil, err
	}
	if resp == nil || len(resp.Results) < len(nodeIDs) {
		return nil, fmt.Errorf("opcua: read response incomplete")
	}
	return resp.Results, nil
}

func (m *OpcuaManager) WriteNode(ctx context.Context, cfg OpcuaConfig, nodeID string, value interface{}) error {
	client, err := m.ensureClient(ctx, cfg)
	if err != nil {
		return err
	}
	id, err := ua.ParseNodeID(strings.TrimSpace(nodeID))
	if err != nil {
		return fmt.Errorf("opcua: invalid nodeId %q: %w", nodeID, err)
	}
	v, err := ua.NewVariant(value)
	if err != nil {
		return fmt.Errorf("opcua: invalid value: %w", err)
	}

	req := &ua.WriteRequest{
		NodesToWrite: []*ua.WriteValue{
			{
				NodeID:      id,
				AttributeID: ua.AttributeIDValue,
				Value: &ua.DataValue{
					EncodingMask: ua.DataValueValue,
					Value:        v,
				},
			},
		},
	}

	resp, err := client.WriteWithContext(ctx, req)
	if err != nil {
		return err
	}
	if len(resp.Results) == 0 || resp.Results[0] != ua.StatusOK {
		if len(resp.Results) == 0 {
			return fmt.Errorf("opcua: write failed: empty response")
		}
		return fmt.Errorf("opcua: write failed: %s", resp.Results[0])
	}
	return nil
}

func (m *OpcuaManager) ensureClient(ctx context.Context, cfg OpcuaConfig) (*gopcua.Client, error) {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, fmt.Errorf("opcua: endpoint missing")
	}
	key := opcuaClientKey(cfg)

	m.mu.Lock()
	defer m.mu.Unlock()

	if holder, ok := m.clients[key]; ok {
		if holder.client != nil && holder.client.State() == gopcua.Connected {
			return holder.client, nil
		}
		if holder.client != nil {
			_ = holder.client.CloseWithContext(context.Background())
		}
		delete(m.clients, key)
	}

	opts, err := buildOpcuaClientOptions(ctx, cfg)
	if err != nil {
		return nil, err
	}
	client := gopcua.NewClient(cfg.Endpoint, opts...)
	if err := client.Connect(ctx); err != nil {
		_ = client.CloseWithContext(context.Background())
		return nil, err
	}
	m.clients[key] = &opcuaClientHolder{cfg: cfg, client: client}
	return client, nil
}

func buildOpcuaClientOptions(ctx context.Context, cfg OpcuaConfig) ([]gopcua.Option, error) {
	policy := strings.TrimSpace(cfg.SecurityPolicy)
	if policy == "" {
		policy = "None"
	}
	mode := strings.TrimSpace(cfg.SecurityMode)
	if mode == "" {
		mode = "None"
	}

	authType := ua.UserTokenTypeAnonymous
	authOpt := gopcua.AuthAnonymous()
	if strings.TrimSpace(cfg.Username) != "" {
		authType = ua.UserTokenTypeUserName
		authOpt = gopcua.AuthUsername(cfg.Username, cfg.Password)
	}

	endpoints, err := gopcua.GetEndpoints(ctx, cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	selected := gopcua.SelectEndpoint(endpoints, policy, ua.MessageSecurityModeFromString(mode))
	if selected == nil {
		return nil, fmt.Errorf("opcua: no matching endpoint for policy=%s mode=%s", policy, mode)
	}

	return []gopcua.Option{
		gopcua.SecurityFromEndpoint(selected, authType),
		authOpt,
		gopcua.DialTimeout(5 * time.Second),
	}, nil
}

func opcuaClientKey(cfg OpcuaConfig) string {
	parts := []string{
		strings.TrimSpace(cfg.Endpoint),
		strings.ToLower(strings.TrimSpace(cfg.SecurityPolicy)),
		strings.ToLower(strings.TrimSpace(cfg.SecurityMode)),
		strings.TrimSpace(cfg.Username),
	}
	return strings.Join(parts, "|")
}
