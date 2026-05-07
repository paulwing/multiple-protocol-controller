package config

import "encoding/json"

type DeviceConfig struct {
	ID          string `json:"id"`
	DeviceType  int    `json:"deviceType"` // 0-普通设备;1-网关；2-自带网关设备
	GatewayInfo struct {
		ID         string `json:"id"`
		IP         string `json:"ip"`
		Port       int    `json:"port"`
		DeviceName string `json:"deviceName"`
	} `json:"gatewayInfo"`
	SerialNumber string        `json:"serialNumber"`
	Disabled     bool          `json:"disabled"`
	Description  string        `json:"description"`
	Protocol     string        `json:"protocol"`
	AcqFreq      int           `json:"acqFreq"`
	DeviceName   string        `json:"deviceName"`
	DeviceKind   bool          `json:"deviceKind"`
	ProductId    string        `json:"productId"`
	Location     []float64     `json:"location"` // 经、纬度
	Position     string        `json:"position"` // 位置：xx大厦2楼东口
	Username     string        `json:"username"`
	Password     string        `json:"password"`
	Properties   []Property    `json:"properties"` // 采集数据
	Tags         string        `json:"tags"`       // 设备标签，用于对设备进行人为划分、检索
	Params       []DeviceParam `json:"params"`     // 协议规定的设备其它信息
}

type DeviceParam struct {
	Label string          `json:"label"`
	Key   string          `json:"key"`
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

type Property struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"` // enum, bool, int, float, string
	Key      string          `json:"key"`
	Name     string          `json:"name"`
	Access   string          `json:"access"` // 11-读写；10-读；01-写
	Unit     string          `json:"unit"`
	Protocol json.RawMessage `json:"protocol"`
}

// Protocol 解析信息说明：
// 该字段结构由具体协议约定，如modbus协议，则该字段结构为：
// "protocol": {
// 	"type": "MODBUS_RTU",
// 	"points": {
// 		"read": [
// 			{
// 				"endian": "ABCD",
// 				"functionCode": "03",
// 				"address": "02"
// 			}
// 		],
// 		"write": [
// 			{
// 				"endian": "ABCD",
// 				"functionCode": "06",
// 				"address": "01"
// 			}
// 		]
// 	}
// },
// opcua协议解析信息说明：
// "protocol": {
// 	"type": "OPCUA",
// 	"nodes": {
// 	"read": [
// 		{ "nodeId": "ns=2;s=DeviceA/Temperature" }
// 	]
// 	"write": [
// 		{ "nodeId": "ns=2;s=DeviceA/Temperature" }
// 	]
// 	}
// }

// Params 解析信息说明：
// 该字段结构由设备特殊参数列表表示，运行时按 key 读取 value，如modbus协议需要从机ID，则该字段结构为：
// "params": [
// 	{"label": "从机ID", "key": "slaveID", "type": "string", "value": "1"},
// 	{"label": "超时时间", "key": "timeout", "type": "int", "value": 3000}
// ]
