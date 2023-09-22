package binancedataservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	dc "github.com/Hellizer/binancedataconnector"
	"github.com/Hellizer/binancedataserviceproto/pb"
	log "github.com/Hellizer/lightlogger"
	"google.golang.org/grpc"
)

type DataService struct {
	pb.UnimplementedDataServServer
	wsClient          *dc.MarketWsClient       //websocket клиент
	socketSubscribers map[string][]chan string //
	apiServer         *grpc.Server             //
	apiListener       net.Listener             //
	ExchangeInfo      *dc.ExchangeInfo         //информация о бирже
	timeDelta         int64                    //дельта между локальным и серверным временем
	timeDeltaRenew    *time.Ticker             //таймер для обновлений данных
	stopUpdateCh      chan bool                //стоппер обновления данных
	stopSocketClients []chan bool              //стоппер для сокет клиентов
}

func NewDataService(addr string) *DataService {
	ds := new(DataService)
	ds.apiServer = grpc.NewServer()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Print(1, log.LogError, "grpc server", fmt.Sprintf("ошибка открытия порта: %s", err.Error()))
	}
	ds.apiListener = lis
	pb.RegisterDataServServer(ds.apiServer, ds)

	go func() {
		log.Print(1, log.LogInfo, "grpc server", "Старт GRPC сервера")
		err := ds.apiServer.Serve(ds.apiListener)
		if err != nil {
			log.Print(1, log.LogError, "grpc server", fmt.Sprintf("ошибка запуска GRPC сервера : %s", err.Error()))
			log.SoftDispose()
			lis.Close()
			os.Exit(1)
		}
	}()
	sTime := dc.GetServerTime()
	ds.ExchangeInfo = dc.GetExchangeInfo()
	ds.timeDelta = sTime - time.Now().UTC().UnixMilli()
	ds.timeDeltaRenew = time.NewTicker(30 * time.Minute)
	ds.stopUpdateCh = make(chan bool)
	go func() {
		for {
			select {
			case <-ds.timeDeltaRenew.C:
				sTime := dc.GetServerTime()
				ds.timeDelta = sTime - time.Now().UTC().UnixMilli()
				if time.UnixMilli(sTime).UTC().Hour() == 0 {
					ds.ExchangeInfo = dc.GetExchangeInfo()
				}
			case <-ds.stopUpdateCh:
				close(ds.stopUpdateCh)
				log.Print(1, log.LogInfo, "grpc server", "автоматическое обновление данных остановлено")
				return
			}
		}

	}()
	ds.stopSocketClients = make([]chan bool, 0)
	ds.wsClient = dc.GetMarketWsClient()
	ds.wsClient.SetHandler(ds.wsMarketHandler)
	return ds
}

func (ds *DataService) GetTime(ctx context.Context, void *pb.Void) (*pb.ServerTime, error) {
	t := dc.GetServerTime()
	if t == 0 {
		return nil, errors.New("проблема получения времени от сервера")
	}
	return &pb.ServerTime{Time: t}, nil
}

func (ds *DataService) GetKlines(ctx context.Context, req *pb.KlinesRequest) (*pb.KlinesResponse, error) {
	resp := dc.GetKlines(req.Symbol, req.Interval, req.Limit+1)
	result := new(pb.KlinesResponse)
	klines := strings.Split(resp, "],[")
	if len(klines) != int(req.Limit+1) {
		log.Print(1, log.LogError, "grpc server", "Количество не совпадает с запрошенным")
	}
	last := klines[len(klines)-1]
	kdata := strings.Split(last, ",")
	t, _ := strconv.ParseInt(kdata[6], 10, 64)
	if t > time.Now().UTC().UnixMilli()+ds.timeDelta {
		result.Klines = klines[:len(klines)-1]
		log.Print(1, log.LogInfo, "grpc server", "последняя свеча не полная, отбрасываем")
	} else {
		result.Klines = klines[1:]
		log.Print(1, log.LogInfo, "grpc server", "последняя свеча полная, отбрасываем первую")
	}
	return result, nil
}

// func fillKline(data []interface{}) *pb.Kline {
// 	var err error
// 	kline := new(pb.Kline)
// 	kline.OpenTime, err = data[0].(json.Number).Int64()
// 	kline.CloseTime, err = data[6].(json.Number).Int64()
// 	kline.TradeCounts, err = data[8].(json.Number).Int64()
// 	kline.Open, err = strconv.ParseFloat(data[1].(string), 64)
// 	kline.High, err = strconv.ParseFloat(data[2].(string), 64)
// 	kline.Low, err = strconv.ParseFloat(data[3].(string), 64)
// 	kline.Close, err = strconv.ParseFloat(data[4].(string), 64)
// 	kline.Volume, err = strconv.ParseFloat(data[5].(string), 64)
// 	if err != nil {
// 		return nil
// 	}
// 	return kline
// }

func (ds *DataService) GetFuturesPairs(ctx context.Context, req *pb.Void) (*pb.FuturesPairsResponse, error) {
	result := new(pb.FuturesPairsResponse)
	result.Pairs = make([]*pb.FuturesPair, 0)
	for _, v := range ds.ExchangeInfo.Symbols {
		if v.ContractType == "PERPETUAL" && v.Status == "TRADING" {
			pair := new(pb.FuturesPair)
			pair.Symbol = v.Symbol
			pair.BaseAsset = v.BaseAsset
			for _, vv := range v.Filters {
				if vv.FilterType == "PRICE_FILTER" {
					pair.PriceStep, _ = strconv.ParseFloat(vv.TickSize, 64)
				}
				if vv.FilterType == "MARKET_LOT_SIZE" {
					pair.LotStep, _ = strconv.ParseFloat(vv.MinQty, 64)
				}
			}

			result.Pairs = append(result.Pairs, pair)
		}
	}
	return result, nil
}

func (ds *DataService) wsMarketHandler(msg []byte) {
	event := dc.StreamEvent{}
	err := json.Unmarshal(msg, &event)
	if err != nil {
		log.Print(1, log.LogError, "grpc server", "json pasrse error")
		fmt.Printf("err: %v\n", err)
	}
	if chs, ok := ds.socketSubscribers[event.Stream]; ok {
		if strings.Contains(event.Stream, "kline") { // проверяем что за тип данных нам пришел
			kEvent := dc.StreamKlineEvent{}
			err = json.Unmarshal(msg, &kEvent)
			if err != nil {
				log.Print(1, log.LogError, "grpc server", "json pasrse error")
				fmt.Printf("err: %v\n", err)
			}
			if kEvent.Data.Kline.IsClosed { // отправка только в случае если свеча завершена
				for k := range chs {
					chs[k] <- string(msg)
				}
			}
		}

	} else {
		log.Print(1, log.LogWarn, "grpc server", fmt.Sprintf("неизвестные данные: %s", string(msg)))
	}
	//log.Print(1, log.LogInfo, "msg handler", string(msg))
}

func (ds *DataService) GetSocketData(stream pb.DataServ_GetSocketDataServer) error {
	dataChan := make(chan string)
	stopper := make(chan bool)
	ds.stopSocketClients = append(ds.stopSocketClients, stopper)
	isWorking := true
	go func() { //получение запросов от клиентов (подписаться, отписаться)
		for isWorking {
			req, err := stream.Recv()
			if err == io.EOF {
				log.Print(1, log.LogWarn, "grpc server", "socket EOF")
				break
			}
			if err != nil {
				log.Print(1, log.LogError, "grpc server", "socket rec error")
				break
			}
			ds.parseSocketRequest(req, dataChan)
		}
		ds.clientClosed(dataChan)
		log.Print(1, log.LogWarn, "grpc server", "end receiving socket")

	}()
	var data string
	//go func() {
	for {
		select {
		case data = <-dataChan:
			err := stream.Send(&pb.SocketResponse{Response: data})
			if err != nil {
				log.Print(1, log.LogError, "grpc server", "socket send error")
				//return err
			}
		case <-stopper:
			isWorking = false
			time.Sleep(time.Second)
			close(dataChan)
			close(stopper)
			return nil
		}
	}
	//}()
	//return nil
}

func (ds *DataService) parseSocketRequest(req *pb.SocketRequest, ch chan string) {
	log.Print(1, log.LogInfo, "parse request", fmt.Sprint(req))
	channelName := strings.ToLower(fmt.Sprintf("%s@kline_%s", req.Symbol, req.Interval))
	if req.IsSubscribe {
		if ds.socketSubscribers != nil {
			if chs, ok := ds.socketSubscribers[channelName]; ok {
				for k := range chs {
					if chs[k] == ch {
						return //уже подписаны
					}
				}
				chs = append(chs, ch) // не подписаны но данные уже получаем, подписываемся
			} else {
				ds.socketSubscribers[channelName] = make([]chan string, 1) //данные не получаем, организуем получение данных и подписку
				ds.socketSubscribers[channelName][0] = ch
				ds.wsClient.Subscribe([]string{channelName})
			}
		} else {
			ds.socketSubscribers = make(map[string][]chan string) //подписок еще нет, сокет не активный
			ds.socketSubscribers[channelName] = make([]chan string, 1)
			ds.socketSubscribers[channelName][0] = ch
			ds.wsClient.Open(channelName)
			ds.wsClient.SetHandler(ds.wsMarketHandler)
		}
	} else {
		//TODO: проверить что все ок
		ds.wsClient.UnSubscribe([]string{channelName}) //отписываемся
		if ds.socketSubscribers != nil {               //проверяем на всякий случай
			if len(ds.socketSubscribers[channelName]) > 0 {
				chs := ds.socketSubscribers[channelName]
				for kk := range chs {
					if chs[kk] == ch {
						ds.socketSubscribers[channelName] = append(chs[:kk], chs[kk+1:]...) //удаляем элемент из массива
					}
				}
				if len(ds.socketSubscribers[channelName]) == 0 {
					delete(ds.socketSubscribers, channelName)
				}
			}
			delete(ds.socketSubscribers, channelName)
			if len(ds.socketSubscribers) == 0 { //проверяем что нет активных подписок и закрываем сокет
				ds.wsClient.Close()
				ds.socketSubscribers = nil
				log.Print(1, log.LogInfo, "grpc server", "активных подписок нет, закрываем сокет")
			}
		}
	}
}

// надо убрать все подписки от закрывшегося клиента
func (ds *DataService) clientClosed(ch chan string) {
	if ds.socketSubscribers != nil { //проверяем на всякий случай
		for k := range ds.socketSubscribers {
			chs := ds.socketSubscribers[k]
			if len(chs) > 1 {
				for kk := range chs {
					if chs[kk] == ch {
						ds.socketSubscribers[k] = append(chs[:kk], chs[kk+1:]...) //удаляем элемент из массива
					}
				}
			} else {
				if chs[0] == ch {
					delete(ds.socketSubscribers, k)
				}
			}
		}
		if len(ds.socketSubscribers) == 0 { //проверяем что нет активных подписок и закрываем сокет
			ds.wsClient.Close()
			ds.socketSubscribers = nil
			log.Print(1, log.LogInfo, "grpc server", "обнулили подписки")
		}
	}
}

func (ds *DataService) Stop() {
	if ds.socketSubscribers != nil {
		log.Print(1, log.LogInfo, "grpc server", "закрываем клиенты")
		for k := range ds.stopSocketClients {
			ds.stopSocketClients[k] <- true
		}
	}

	log.Print(1, log.LogInfo, "grpc server", "останавливаем GRPC server")
	ds.apiServer.Stop()

	log.Print(1, log.LogInfo, "grpc server", "GRPC сервер остановлен")
	ds.apiListener.Close()
	ds.timeDeltaRenew.Stop()
	ds.stopUpdateCh <- true
	time.Sleep(time.Second)

	log.Print(1, log.LogInfo, "grpc server", "выход ")
}
