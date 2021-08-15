package main

import (
	"crypto/tls"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBrakeFull(t *testing.T) {

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

	//bad Authorizarion push
	rpc_params := make(map[string]interface{})

	rpc_data := jsonRPCRequest{MethodName: "brake_push_messages", Params: rpc_params}
	authorizationKey := "Bearer 66666"

	rpc_params["topic"] = "test"
	responce := rpc_exec(rpc_data, authorizationKey)

	if responce.Error.Code == 0 {
		t.Error("Auth not work")
		return
	}

	//r := strings.NewReader(`{"topic": "non_exist_topic", "partition": "0", "messages":[] }`)
	//req, err := http.NewRequest("POST", "https://localhost:9200/push/v1", r)
	//req.Header.Add("Authorization", "Bearer 66666")
	//resp, err := client.Do(req)

	authorizationKey = "Bearer 123123"

	rpc_params["topic"] = "test2"
	rpc_params["partition"] = 0
	responce = rpc_exec(rpc_data, authorizationKey)
	if responce.Error.Code == 0 {
		t.Error("Topic check not work")
		return
	}

	/*
		//success push test
		_, lastId := brake.info("test", 0)
		r = strings.NewReader(`{"topic": "test", "partition": "0", "messages":["` + strconv.FormatUint(lastId+1, 10) + `","` + strconv.FormatUint(lastId+2, 10) + `"] }`)

		req, err = http.NewRequest("POST", "https://localhost:9200/push/v1", r)
		req.Header.Add("Authorization", "Bearer 123123")
		resp, err = client.Do(req)
		if err != nil {
			t.Error("Error!!!", err)
			return
		}
		if resp.StatusCode != 200 {
			t.Error("push resp.StatusCode != 200, returnd", resp.StatusCode)
			return
		}
		body, _ = io.ReadAll(resp.Body)
		if string(body) != `{"error":""}` {
			t.Error("Expected ", string(body))
			return
		}

		//bad authorization pull test
		req, err = http.NewRequest("GET", "https://localhost:9200/pull/v1?topic=test&partition=0&off=0&limit=1000", nil)
		req.Header.Add("Authorization", "Bearer 66666")
		resp, err = client.Do(req)

		//non-existent topic pull test
		req, err = http.NewRequest("GET", "https://localhost:9200/pull/v1?topic=non_existent&partition=0&off=0&limit=1000", nil)
		req.Header.Add("Authorization", "Bearer 123123")
		resp, err = client.Do(req)

		//pull test
		req, err = http.NewRequest("GET", "https://localhost:9200/pull/v1?topic=test&partition=0&off=0&limit=1000&max_size=10000000", nil)
		req.Header.Add("Authorization", "Bearer 123123")
		resp, err = client.Do(req)

		if err != nil {
			t.Error(err)
			return
		}
		if resp.StatusCode != 200 {
			t.Error("pull resp.StatusCode != 200")
			return
		}
		body, _ = io.ReadAll(resp.Body)

		incommingMessage := []responseHttpMessage{}
		json.Unmarshal([]byte(body), &incommingMessage)

		if len(incommingMessage) != 1 {
			t.Error("Expected 1 element, recived", len(incommingMessage))
			return
		}

		messagePayloadNo, _ := strconv.ParseUint(incommingMessage[0].Msg, 10, 64)

		if incommingMessage[0].Id != messagePayloadNo {
			t.Error("Expected payload error, recived", messagePayloadNo)
			return
		}
	*/

	brake.stopHttpService()
	brake.Close()

	// mass push test
	brake = Brake("config_test.yaml")

	go brake.HttpService()
	time.Sleep(time.Second)

	brake.doMassPushPullTest("test", 0)
	brake.doMassPushPullTest("test", 1)
	if false {
		t.Error("Error!!!")
	}

	brake.pull("test", 0, 1)

	//pull limit test
	const limit = 20
	firstId, _ := brake.info("test", 0)

	rpc_data.MethodName = "brake_pull_messages"
	rpc_params["topic"] = "test"
	rpc_params["limit"] = 20
	rpc_params["off"] = firstId

	responce = rpc_exec(rpc_data, authorizationKey)
	if responce.Error.Code != 0 || len(responce.Result.([]interface{})) != limit {
		t.Error("Limit not work")
		return
	}

	/*
		req, err := http.NewRequest("GET", "https://localhost:9200/pull/v1?topic=test&partition=0&off="+strconv.FormatUint(firstId, 10)+"&limit="+strconv.FormatUint(limit, 10), nil)
		req.Header.Add("Authorization", "Bearer 123123")
		resp, err := client.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		body, _ := io.ReadAll(resp.Body)
		//res := string(body)
		incommingMessage := []responseHttpMessage{}
		json.Unmarshal([]byte(body), &incommingMessage)
		if len(incommingMessage) != limit {
			t.Error("Expecter", limit, "messages in response, finded", len(incommingMessage))
		}

		//topics info
		req, err = http.NewRequest("GET", "https://localhost:9200/topics_info/v1", nil)
		req.Header.Add("Authorization", "Bearer 123123")
		resp, err = client.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		//................//
	*/
	brake.stopHttpService()
	brake.Close()
	os.RemoveAll("testdb_dir")

	log.Println("Finish")
}

func rpc_exec(rpc jsonRPCRequest, authorizationKey string) jsonRPCResponce {

	rpc_data, _ := json.Marshal(rpc)

	r := strings.NewReader(string(rpc_data))

	req, _ := http.NewRequest("POST", "https://localhost:9200/rpc_exec/v1", r)
	req.Header.Add("Authorization", authorizationKey)
	client := &http.Client{}
	respone, _ := client.Do(req)

	var responce jsonRPCResponce

	body, _ := io.ReadAll(respone.Body)
	json.Unmarshal(body, &responce)
	return responce
}

func (brake *brakeContext) doMassPushPullTest(topic string, partition int) {

	message := strings.Repeat("1", 512)
	_, lastId := brake.info(topic, partition)
	id := lastId + 1
	if lastId == 0 {
		id = 1
	}

	idForRead := id

	var k int
	start := time.Now().UnixNano()

	for {

		for k = 1; k < 10000; k++ {
			testString := strconv.Itoa(int(id)) + "             " + message
			brake.push(topic, partition, testString)
			id++
		}

		firstId, _ := brake.info(topic, partition)
		if firstId != 1 {
			break
		}

	}

	println(topic, "put time", (time.Now().UnixNano()-start)/1000/1000)

	start = time.Now().UnixNano()

	for i := 1; i < k; i++ {
		id, _, message := brake.pull(topic, partition, idForRead)
		if id == 0 {
			println(topic, "no more", idForRead)
			break
		}

		idForRead = id + 1

		res, _ := strconv.Atoi(strings.Split(message, " ")[0])
		if uint64(res) != id {
			println(topic, "error!")
		}
	}
	println(topic, "get time", (time.Now().UnixNano()-start)/1000/1000)

	firstId, lastId := brake.info(topic, partition)
	println(topic, "messages from", firstId, "to", lastId)
}
