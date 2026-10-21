package bacnet

import (
	"encoding/json"
	"fmt"
	"strings"

	"multiple-protocol-controller/internal/protocol"
	"multiple-protocol-controller/internal/protocol/datatypes"
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
	DataType   string `json:"dataType"` // 线缆数据类型，可选；缺省沿用属性 type；enum 当前按 Int32 编码
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
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		if err := validatePointDataTypes(&cfg); err != nil {
			return nil, err
		}
		return &cfg, nil
	default:
		return nil, fmt.Errorf("protocol is not BACNET type %q", base.Type)
	}
}

func validatePointDataTypes(cfg *BacnetProtocol) error {
	for index := range cfg.Objects.Read {
		if err := normalizeObjectDataType(&cfg.Objects.Read[index]); err != nil {
			return fmt.Errorf("bacnet: read object %d: %w", index, err)
		}
	}
	for index := range cfg.Objects.Write {
		if err := normalizeObjectDataType(&cfg.Objects.Write[index]); err != nil {
			return fmt.Errorf("bacnet: write object %d: %w", index, err)
		}
	}
	return nil
}

func normalizeObjectDataType(object *BacnetObject) error {
	if strings.TrimSpace(object.DataType) == "" {
		object.DataType = ""
		return nil
	}
	canonical, err := datatypes.Canonical(object.DataType)
	if err != nil {
		return err
	}
	if !datatypes.SupportsBacnet(canonical) {
		return fmt.Errorf("unsupported data type %q", canonical)
	}
	object.DataType = canonical
	return nil
}

func (b *Bacnet) EncodeCommand(cmd any) ([]byte, error) {
	return nil, fmt.Errorf("bacnet: EncodeCommand not supported in read-only phase")
}
