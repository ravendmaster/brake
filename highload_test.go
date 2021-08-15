package main

import (
	"crypto/tls"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHighLoad(t *testing.T) {

	logFile, _ := os.OpenFile("test_log.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(logFile)

	log.Println("Start")

	os.RemoveAll("testdb_dir")
	os.MkdirAll("testdb_dir", 0777)

	brake := Brake("config_test.yaml")

	go brake.HttpService()
	time.Sleep(time.Second)
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	//client := &http.Client{}

	_, lastId := brake.info("alpha", 0)

	var testMessages [1]string
	testMessages[0] = strings.Repeat("1", 20480)

	var wg sync.WaitGroup

	for k := 0; k < 50; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {

				rpc_params := make(map[string]interface{})
				rpc_data := jsonRPCRequest{MethodName: "brake_push_messages", Params: rpc_params}
				authorizationKey := "Bearer 123123"
				rpc_params["topic"] = "alpha"
				rpc_params["messages"] = testMessages
				responce := rpc_exec(rpc_data, authorizationKey)
				if responce.Error.Code != 0 {
					t.Error("Error!!!", responce.Error.Message)
					return
				}

				/*
					r := strings.NewReader(`{"topic": "alpha", "partition": "0", "messages":["` + testMessage + `"] }`)
					req, _ := http.NewRequest("POST", "https://localhost:9200/push/v1", r)
					req.Header.Add("Authorization", "Bearer 123123")
					resp, err := client.Do(req)
					if err != nil {
						t.Error("Error!!!", err)
						return
					}
					if resp.StatusCode != 200 {
						t.Error("push resp.StatusCode != 200, returnd", resp.StatusCode)
						return
					}
				*/

			}
		}()
	}

	wg.Wait()
	//time.Sleep(time.Second * 10)

	brake.stopHttpService()

	_, lastId = brake.info("alpha", 0)

	//id, msg := brake.pull("test", 0, 1)
	//println(id, msg)

	if lastId != 5000 {
		t.Error("Losting messages detected, 5000!=", lastId)
	}

	brake.Close()
	os.RemoveAll("testdb_dir")

	log.Println("Finish")
}
