package collector

import (
	"sort"

	"multiple-protocol-controller/internal/config"
)

const (
	maxRegisterBatchQuantity = 24
	maxRegisterGap           = 2
	maxDiscreteBatchQuantity = 32
	maxDiscreteGap           = 0
)

type batchQuery struct {
	functionCode int
	startAddr    uint16
	quantity     uint16
	params       []*config.ModbusParam
}

func buildBatchQueries(device config.DeviceRuntime, params []config.ModbusParam) ([]batchQuery, map[string]struct{}) {
	grouped := make(map[int][]*config.ModbusParam)
	for i := range params {
		param := &params[i]
		if param.ReadDisabled {
			continue
		}
		if !isReadFunctionCode(param.FunctionCode) {
			continue
		}
		grouped[param.FunctionCode] = append(grouped[param.FunctionCode], param)
	}

	covered := make(map[string]struct{})
	var queries []batchQuery
	for fc, list := range grouped {
		sort.Slice(list, func(i, j int) bool {
			return list[i].Address < list[j].Address
		})
		builder := newBatchBuilder(fc)
		for _, param := range list {
			if !builder.canAdd(param) {
				if builder.hasParams() {
					queries = append(queries, builder.build())
				}
				builder = newBatchBuilder(fc)
			}
			builder.add(param)
			covered[paramKey(param)] = struct{}{}
		}
		if builder.hasParams() {
			queries = append(queries, builder.build())
		}
	}

	return queries, covered
}

type batchBuilder struct {
	functionCode int
	params       []*config.ModbusParam
	start        uint64
	end          uint64
	maxQuantity  uint16
	gapLimit     uint64
}

func newBatchBuilder(fc int) *batchBuilder {
	builder := &batchBuilder{
		functionCode: fc,
	}
	if fc == 1 || fc == 2 {
		builder.maxQuantity = maxDiscreteBatchQuantity
		builder.gapLimit = maxDiscreteGap
	} else {
		builder.maxQuantity = maxRegisterBatchQuantity
		builder.gapLimit = maxRegisterGap
	}
	return builder
}

func (b *batchBuilder) canAdd(param *config.ModbusParam) bool {
	if b == nil {
		return false
	}
	if len(b.params) == 0 {
		return true
	}
	if param.FunctionCode != b.functionCode {
		return false
	}
	start, end := paramSpan(param)
	if start < b.start {
		return false
	}
	gap := uint64(0)
	if start > b.end+1 {
		gap = start - b.end - 1
	}
	if b.gapLimit > 0 && gap > b.gapLimit {
		return false
	}
	newQuantity := uint16((max(b.end, end) - b.start) + 1)
	if newQuantity > b.maxQuantity {
		return false
	}
	return true
}

func (b *batchBuilder) add(param *config.ModbusParam) {
	start, end := paramSpan(param)
	if len(b.params) == 0 {
		b.start = start
		b.end = end
	} else if end > b.end {
		b.end = end
	}
	b.params = append(b.params, param)
}

func (b *batchBuilder) hasParams() bool {
	return len(b.params) > 0
}

func (b *batchBuilder) build() batchQuery {
	if len(b.params) == 0 {
		return batchQuery{}
	}
	qty := uint16((b.end - b.start) + 1)
	return batchQuery{
		functionCode: b.functionCode,
		startAddr:    uint16(b.start),
		quantity:     qty,
		params:       append([]*config.ModbusParam(nil), b.params...),
	}
}

func paramSpan(param *config.ModbusParam) (uint64, uint64) {
	if param == nil {
		return 0, 0
	}
	if param.FunctionCode == 1 || param.FunctionCode == 2 {
		qty := param.Quantity
		if qty <= 0 {
			qty = 1
		}
		if qty == 1 {
			return param.Address, param.Address
		}
		end := param.Address + uint64(qty) - 1
		return param.Address, end
	}
	count := uint64(quantityForParam(*param))
	if count == 0 {
		count = 1
	}
	if count == 1 {
		return param.Address, param.Address
	}
	end := param.Address + count - 1
	return param.Address, end
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
