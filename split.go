package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type AppConfig struct {
	Source RedisConfig   `json:"source"`
	Sinks  []RedisConfig `json:"sinks"`
}

type RedisConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	Key      string `json:"key"`
}

func load() (AppConfig, error) {

	var appConfig AppConfig

	// Load App Config
	jsonFile, err := os.Open("config.json")

	if err != nil {
		return AppConfig{}, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal([]byte(byteValue), &appConfig)

	return appConfig, nil

}

func main() {

	var wg sync.WaitGroup

	appConfig, err := load()
	if err != nil {
		fmt.Println(err)
		return
	}

	// Create Sinks
	// ------------
	sinkConfigs := appConfig.Sinks
	var sinkChans = make([]chan string, len(sinkConfigs))
	var stopChans = make([]chan int, len(sinkConfigs)+1)

	for i := 0; i < len(sinkConfigs); i++ {

		wg.Add(1)

		sinkChans[i] = make(chan string)
		stopChans[i] = make(chan int)

		sinkChan := sinkChans[i]
		stopChan := stopChans[i]
		sinkConf := sinkConfigs[i]

		go func(data chan string, control chan int, wg *sync.WaitGroup) {
			for {
				select {
				case item := <-data:
					fmt.Println(sinkConf)
					fmt.Println(item)
				case <-control:
					wg.Done()
					return
				}
			}

		}(sinkChan, stopChan, &wg)
	}

	// Create Source
	// -------------

	wg.Add(1)
	stopChans[len(sinkConfigs)] = make(chan int)
	sourceStop := stopChans[len(sinkConfigs)]
	go func(appConfig AppConfig, sinkChans []chan string, sourceStop chan int) {

		rdb := redis.NewClient(&redis.Options{
			Addr:     appConfig.Source.Addr,
			Password: appConfig.Source.Password,
		})

		_, err = rdb.Ping(context.TODO()).Result()
		if err != nil {
			fmt.Println(err)
			return
		}

		timeout, _ := time.ParseDuration("1s")
		sourceKey := appConfig.Source.Key

		for {
			select {
			case <-sourceStop:
				wg.Done()
				return
			default:
				item, _ := rdb.BRPop(context.TODO(), timeout, sourceKey).Result()
				if len(item) < 2 {
					continue
				}

				for i := range sinkChans {
					sinkChan := sinkChans[i]
					sinkChan <- item[1]
				}
			}

		}

	}(appConfig, sinkChans, sourceStop)

	// Cleanup Routine
	// ---------------

	control := make(chan os.Signal)
	signal.Notify(control, os.Interrupt, syscall.SIGTERM)
	go func(stopChans []chan int) {
		<-control
		for i := range stopChans {
			stopChans[i] <- 0
		}
	}(stopChans)

	wg.Wait()

}
