package bacnet

import (
	"encoding/json"
	"fmt"
	"strings"

	"multiple-protocol-controller/internal/protocol"
)

type Bacnet struct{}

type protocolBase struct {
	Type string `json:"type"`
}

type BacnetProtocol struct {
	Type    string        `json:"type"`
	Objects BacnetObjects `json:"objects"`
}

type BacnetObjects struct {
	Read  []BacnetObject `json:"read"`
	Write []BacnetObject `json:"write"`
}

type BacnetObject struct {
	ObjectType string `json:"objectType"`
	Instance   uint32 `json:"instance"`
	PropertyID string `json:"propertyId"`
}

func NewBacnet() *Bacnet { return &Bacnet{} }

func init() {
	protocol.RegisterProtocol("bacnet", NewBacnet())
}

func (b *Bacnet) ParsePropProtocol(raw json.RawMessage) (any, error) {
	var base protocolBase
	if err := json.Unmarshal(raw, &base); err != nil {
		return nil, err
	}
	switch strings.ToUpper(strings.TrimSpace(base.Type)) {
	case "BACNET":
		var cfg BacnetProtocol
		return &cfg, json.Unmarshal(raw, &cfg)
	default:
		return nil, fmt.Errorf("protocol is not BACNET type %q", base.Type)
	}
}

func (b *Bacnet) EncodeCommand(cmd any) ([]byte, error) {
	return nil, fmt.Errorf("bacnet: EncodeCommand not supported in read-only phase")
}
