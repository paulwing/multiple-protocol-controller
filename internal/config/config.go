package config

import (
	"encoding/json"
	"sync/atomic"

	"github.com/spf13/viper"
)

type Config struct {
	RunMode string    `mapstructure:"runmode"`
	Redis   RedisCfg  `mapstructure:"redis"`
	Influx  InfluxCfg `mapstructure:"influx"`
}

type Command4MPC struct {
	Uid  string                   `json:"uid"`
	Data []map[string]interface{} `json:"data"`
}

type RedisCfg struct {
	Address string `mapstructure:"address"`
	Timeout string `mapstructure:"timeout"`
	Pwd     string `mapstructure:"pwd"`
}

type InfluxCfg struct {
	Enabled         bool   `mapstructure:"enabled"`
	URL             string `mapstructure:"url"`
	Token           string `mapstructure:"token"`
	Org             string `mapstructure:"org"`
	Bucket          string `mapstructure:"bucket"`
	TimeoutSeconds  int    `mapstructure:"timeout_seconds"`
	BatchSize       int    `mapstructure:"batch_size"`
	FlushIntervalMS int    `mapstructure:"flush_interval_ms"`
	QueueSize       int    `mapstructure:"queue_size"`
	RetryCount      int    `mapstructure:"retry_count"`
	RetryIntervalMS int    `mapstructure:"retry_interval_ms"`
}

type RedisWrapper struct {
	Version string          `json:"version"`
	Key     string          `json:"key"`
	Data    json.RawMessage `json:"data"`
}

var IotCfgStore atomic.Value   // 存储 Config
var CfgChangeCh = "CFG_CHANGE" // 配置数据变更频道
var DeviceCfg = "IOT:DEVICE"   // redis存储设备信息的key
var ProductCfg = "IOT:PRODUCT" // redis存储产品模型的key
// var SetDeviceProperty = "SET_DEVICE_PROPERTY" // 控制指令下发频道
// var SendCmdResCh = "SEND_CMD_RES"                  // 控制指令回执频道
var SetDeviceProperty = "set_device_current_value" // 控制指令下发频道
var SendCmdResCh = "device_current_value_response" // 控制指令回执频道
var ServerStatus = "server:status:mpc"             // redis存储服务心跳的key

func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	// v.SetConfigType("toml")

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
