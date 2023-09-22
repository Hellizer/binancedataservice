package config

import (
	"encoding/json"
	"io/ioutil"
	"sync"
)

type Conf struct {
	ServiceLogLevel int    `json:"service_log_level"` //
	LogstashAddr    string `json:"logstash_addr"`     //
	GRPCPort        string `json:"grpc_port"`         //
}

const (
	confFileName = "config/data-serv-conf.json"
)

var instance *Conf = nil
var lock sync.Mutex

func GetConfig() *Conf {
	if instance != nil {
		return instance
	}
	lock.Lock()
	if instance != nil {
		lock.Unlock()
		return instance
	}
	conf := new(Conf)
	conf.ServiceLogLevel = 1
	conf.GRPCPort = ":50051"
	file, err := ioutil.ReadFile(confFileName)
	if err != nil {
		//если нет конфига - необходимо создать пустой и вернуть nil
		file, _ := json.MarshalIndent(conf, "", "")
		_ = ioutil.WriteFile(confFileName, file, 0644)
		return nil
	}

	if err = json.Unmarshal(file, &conf); err != nil {
		return nil
	}

	instance = conf
	lock.Unlock()
	return instance
}
