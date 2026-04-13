package bacnet

import (
	"fmt"
	"strings"

	"multiple-protocol-controller/pkg/utils"
)

func NormalizeValue(val interface{}, dataType string) (interface{}, error) {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "float":
		v, err := utils.CoerceFloat32(val)
		if err != nil {
			return nil, fmt.Errorf("bacnet: %w", err)
		}
		return v, nil
	case "int", "enum":
		v, err := utils.CoerceInt32(val)
		if err != nil {
			return nil, fmt.Errorf("bacnet: %w", err)
		}
		return v, nil
	case "bool":
		v, err := utils.CoerceBool(val)
		if err != nil {
			return nil, fmt.Errorf("bacnet: %w", err)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("bacnet: unsupported data type %q in current phase", dataType)
	}
}
