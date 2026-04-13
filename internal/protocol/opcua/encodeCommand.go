package opcua

import (
	"fmt"
	"strings"

	"multiple-protocol-controller/pkg/utils"
)

// NormalizeValue converts a raw value into the expected OPC UA data type.
func NormalizeValue(val interface{}, dataType string) (interface{}, error) {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dt {
	case "bool":
		return utils.CoerceBool(val)
	case "int", "enum":
		n, err := utils.CoerceInt32(val)
		if err != nil {
			return 0, fmt.Errorf("opcua: %w", err)
		}
		return n, nil
	case "float":
		f, err := utils.CoerceFloat32(val)
		if err != nil {
			return 0, fmt.Errorf("opcua: %w", err)
		}
		return f, nil
	case "string":
		return utils.CoerceString(val), nil
	default:
		return val, nil
	}
}
