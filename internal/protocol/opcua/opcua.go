package opcua

import (
	"encoding/json"
	"fmt"
	"strings"

	"multiple-protocol-controller/internal/protocol"
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
	NodeID string `json:"nodeId"`
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
		return &cfg, json.Unmarshal(raw, &cfg)
	default:
		return nil, fmt.Errorf("protocol is not OPCUA type %q", base.Type)
	}
}

func (o *Opcua) EncodeCommand(cmd any) ([]byte, error) {
	return nil, fmt.Errorf("opcua: EncodeCommand not supported")
}
