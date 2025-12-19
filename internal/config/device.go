package config

type DeviceConfig struct {
	ID          string `json:"id"`
	DeviceType  int    `json:"deviceType"`
	GatewayInfo struct {
		ID         string `json:"id"`
		IP         string `json:"ip"`
		Port       int    `json:"port"`
		DeviceName string `json:"deviceName"`
	} `json:"gatewayInfo"`
	SerialNumber string      `json:"serialNumber"`
	Disabled     bool        `json:"disabled"`
	Description  string      `json:"description"`
	Protocol     string      `json:"protocol"`
	AcqFreq      int         `json:"acqFreq"`
	DeviceName   string      `json:"deviceName"`
	IsShown      bool        `json:"isShown"`
	ProductId    string      `json:"productId"`
	Location     []float64   `json:"location"`   // 经、纬度
	Position     string      `json:"position"`   // 位置：xx大厦2楼东口
	Properties   []Property  `json:"properties"` // 采集数据
	PointInfo    []PointInfo `json:"pointInfo"`  // 点位信息
}

type Property struct {
	Type    string   `json:"type"` // enum, bool, int, float, string
	CtlInfo struct { // 控制点位信息
		PointID string `json:"pointId"`
	} `json:"ctlInfo"`
	ReadInfo struct { // 读取点位信息
		PointID string `json:"pointId"`
	} `json:"readInfo"`
}

type PointInfo struct {
	ID           string `json:"id"`
	SlaveId      string `json:"slaveId"`
	PointType    string `json:"pointType"`
	Address      int    `json:"address"`
	FunctionCode string `json:"functionCode"`
}

// ParseInfo 解析信息说明：
// 该字段结构由具体协议约定，如modbus协议，则该字段结构为：
// "parseInfo":{
// 	"type": "modbus",
// 	ctl_info:{
// 		"pointId": "1",
// 	},
// 	read_info:{
// 		"pointId": "3",
// 	}
// }
