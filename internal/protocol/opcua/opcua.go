package opcua

import (
	"encoding/json"
	"fmt"
	"strings"

	"multiple-protocol-controller/internal/protocol"
	"multiple-protocol-controller/internal/protocol/datatypes"
)

type Opcua struct{}

type protocolBase struct {
	Type string `json:"type"`
}

type OpcuaProtocol struct {
	Type  string `json:"type"`
	Nodes struct {
		Read  []OpcuaNode `json:"read"`
		Write []OpcuaNode `json:"write"`
	} `json:"nodes"`
}

type OpcuaNode struct {
	NodeID   string `json:"nodeId"`
	DataType string `json:"dataType"` // 线缆数据类型，可选；缺省沿用属性 type
}

func NewOpcua() *Opcua { return &Opcua{} }

func init() {
	protocol.RegisterProtocol("OPCUA", NewOpcua())
}

func (o *Opcua) ParsePropProtocol(raw json.RawMessage) (any, error) {
	var base protocolBase
	if err := json.Unmarshal(raw, &base); err != nil {
		return nil, err
	}
	switch strings.ToUpper(strings.TrimSpace(base.Type)) {
	case "OPCUA":
		var cfg OpcuaProtocol
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		if err := validatePointDataTypes(&cfg); err != nil {
			return nil, err
		}
		return &cfg, nil
	default:
		return nil, fmt.Errorf("protocol is not OPCUA type %q", base.Type)
	}
}

func validatePointDataTypes(cfg *OpcuaProtocol) error {
	for index := range cfg.Nodes.Read {
		if err := normalizeNodeDataType(&cfg.Nodes.Read[index]); err != nil {
			return fmt.Errorf("opcua: read node %d: %w", index, err)
		}
	}
	for index := range cfg.Nodes.Write {
		if err := normalizeNodeDataType(&cfg.Nodes.Write[index]); err != nil {
			return fmt.Errorf("opcua: write node %d: %w", index, err)
		}
	}
	return nil
}

func normalizeNodeDataType(node *OpcuaNode) error {
	if strings.TrimSpace(node.DataType) == "" {
		node.DataType = ""
		return nil
	}
	canonical, err := datatypes.Canonical(node.DataType)
	if err != nil {
		return err
	}
	if !datatypes.SupportsOpcua(canonical) {
		return fmt.Errorf("unsupported data type %q", canonical)
	}
	node.DataType = canonical
	return nil
}

func (o *Opcua) EncodeCommand(cmd any) ([]byte, error) {
	return nil, fmt.Errorf("opcua: EncodeCommand not supported")
}
