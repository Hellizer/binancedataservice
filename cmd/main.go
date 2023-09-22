package main

import (
	"os"
	"os/signal"
	"syscall"

	ds "github.com/Hellizer/binancedataservice"
	"github.com/Hellizer/binancedataservice/config"
	log "github.com/Hellizer/lightlogger"
)

func main() {
	log.SetServiceName("Binance DS")
	defer log.SoftDispose()
	conf := config.GetConfig()
	if conf == nil {
		log.Print(1, log.LogError, "main", "Проблема с конфигом")
		return
	}
	if conf.GRPCPort == "" {
		log.Print(1, log.LogError, "main", "не указан адрес GRPC")
		return
	}
	log.SetLogLevel(uint8(conf.ServiceLogLevel))
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	log.Print(1, log.LogInfo, "main", "Стартуем...")

	s := ds.NewDataService(conf.GRPCPort)
	if s == nil {
		log.Print(1, log.LogError, "main", "ошибка создания сервиса")
		return
	}
	sig := <-ch
	log.Print(1, log.LogInfo, "main", "получен сигнал остановки: "+sig.String())
	s.Stop()
}
