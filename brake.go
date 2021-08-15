package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

func main() {

	logFile, _ := os.OpenFile("log.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(logFile)

	log.Println("Start")

	brake := Brake("config.yaml")
	defer brake.Close()

	brake.HttpService()

	log.Println("Finish")
}

var rpcId uint64
var rpcId_mux sync.Mutex

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type jsonRPCResponce struct {
	Result interface{}  `json:"result"`
	Error  jsonRPCError `json:"error"`
	Id     uint64       `json:"id"`
}

type jsonRPCResponceOK struct {
	Result interface{} `json:"result"`
	Id     uint64      `json:"id"`
}

type jsonRPCResponceError struct {
	Error jsonRPCError `json:"error"`
	Id    uint64       `json:"id"`
}

type jsonRPCRequest struct {
	MethodName string      `json:"method"`
	Params     interface{} `json:"params"`
	Id         uint64      `json:"id"`
}

type jsonRPCParamStruct struct {
	Name string `json:"name"`
	Type string `json:"type"`
	//Description string `json:"description"`
}
type jsonRPCMethodMetadataStruct struct {
	ServiceId string               `json:"serviceId"`
	Name      string               `json:"name"`
	Params    []jsonRPCParamStruct `json:"params"`
}
type jsonRPCServiceDeclaration struct {
	Methods []jsonRPCMethodMetadataStruct `json:"methods"`
}

type requestChanByServersMux struct {
	mx sync.RWMutex
	m  map[string]chan jsonRPCRequest
}

func NewRequestChanByServersMux() *requestChanByServersMux {
	return &requestChanByServersMux{
		m: make(map[string]chan jsonRPCRequest),
	}
}
func (c *requestChanByServersMux) Get(key string) (chan jsonRPCRequest, bool) {
	c.mx.RLock()
	defer c.mx.RUnlock()
	val, ok := c.m[key]
	return val, ok
}
func (c *requestChanByServersMux) Store(key string, val chan jsonRPCRequest) {
	c.mx.Lock()
	c.m[key] = val
	c.mx.Unlock()
}

type jsonRPCResponcesMux struct {
	mx sync.RWMutex
	m  map[uint64](chan jsonRPCResponce)
}

func NewJsonRPCResponcesMux() *jsonRPCResponcesMux {
	return &jsonRPCResponcesMux{
		m: make(map[uint64](chan jsonRPCResponce)),
	}
}
func (c *jsonRPCResponcesMux) Load(key uint64) (result chan jsonRPCResponce) {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.m[key]
}

func (c *jsonRPCResponcesMux) ReserveIdAndChanel(incommingMessage *jsonRPCRequest) {

	c.mx.Lock()
	incommingMessage.Id = rpcId
	c.m[rpcId] = make(chan jsonRPCResponce)
	rpcId++
	c.mx.Unlock()
}

func (c *jsonRPCResponcesMux) Delete(key uint64) {
	c.mx.Lock()
	delete(c.m, key)
	c.mx.Unlock()
}

type methodMetadataByMethodNameMux struct {
	mx sync.RWMutex `json:"-"`
	m  map[string]jsonRPCMethodMetadataStruct
}

func NewMethodMetadataByMethodNameMux() *methodMetadataByMethodNameMux {
	return &methodMetadataByMethodNameMux{
		m: make(map[string]jsonRPCMethodMetadataStruct),
	}
}
func (c *methodMetadataByMethodNameMux) Get(key string) (jsonRPCMethodMetadataStruct, bool) {
	c.mx.RLock()
	defer c.mx.RUnlock()
	val, ok := c.m[key]
	return val, ok
}

///////////////

func (brake *brakeContext) HttpService() {

	serverMux := http.NewServeMux()

	/*
		serverMux.HandleFunc("/push/v1", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case "POST":
				brake.processPush(w, r)

			}
		})
		serverMux.HandleFunc("/pull/v1", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case "GET":
				brake.processPull(w, r)
			}
		})

		serverMux.HandleFunc("/topics_info/v1", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case "GET":
				brake.processTopicsInfo(w, r)
			}
		})
	*/

	requestChanelByServer := NewRequestChanByServersMux()

	responceChanels := NewJsonRPCResponcesMux()

	methodMetadataByMethodName := NewMethodMetadataByMethodNameMux()

	serverMux.HandleFunc("/rpc_declare_service/v1", func(w http.ResponseWriter, r *http.Request) {

		setGeneralResponseHeaders(w)

		switch r.Method {
		case "POST":

			serverId := r.Header.Get("Service-Id")
			println("Declare service", serverId)
			body, _ := io.ReadAll(r.Body)
			var jsonRPCServiceDeclarationData jsonRPCServiceDeclaration
			json.Unmarshal(body, &jsonRPCServiceDeclarationData)

			methodMetadataByMethodName.mx.Lock()
			for key, value := range methodMetadataByMethodName.m {
				if value.ServiceId == serverId {
					delete(methodMetadataByMethodName.m, key)
				}
			}
			for _, method := range jsonRPCServiceDeclarationData.Methods {
				method.ServiceId = serverId
				methodMetadataByMethodName.m[method.Name] = method
			}
			methodMetadataByMethodName.mx.Unlock()

			fmt.Fprint(w, string("{}"))
		}
	})

	serverMux.HandleFunc("/rpc_wait_request/v1", func(w http.ResponseWriter, r *http.Request) {

		setGeneralResponseHeaders(w)

		switch r.Method {
		case "POST":
			clientId := r.Header.Get("Service-Id")
			prepareToStop := r.Header.Get("Prepare-To-Stop")

			body, _ := io.ReadAll(r.Body)
			if len(body) > 0 {
				var responce jsonRPCResponce
				json.Unmarshal(body, &responce)

				responceChan := responceChanels.Load(responce.Id)
				if responceChan != nil {
					select {
					case responceChan <- responce:
					case <-time.After(time.Second * 60):
					}
				}
			}

			if len(prepareToStop) == 0 {
				requestChanel, ok := requestChanelByServer.Get(clientId)
				if !ok {
					requestChanel = make(chan jsonRPCRequest)
					requestChanelByServer.Store(clientId, requestChanel)
				}
				select {
				case requestMessage := <-requestChanel:
					data, _ := json.Marshal(requestMessage)
					fmt.Fprint(w, string(data))
				case <-time.After(time.Second * 60):
				}
			}

		}
	})

	serverMux.HandleFunc("/rpc_exec/v1", func(w http.ResponseWriter, r *http.Request) {

		setGeneralResponseHeaders(w)

		switch r.Method {
		case "POST":
			//clientId := r.Header.Get("Client-Id")
			authorizationKey := r.Header.Get("Authorization")

			body, _ := io.ReadAll(r.Body)

			var incommingMessage jsonRPCRequest
			json.Unmarshal(body, &incommingMessage)

			switch incommingMessage.MethodName {
			case "brake_topics_info":
				fmt.Fprint(w, emb_rpc_impl_brake_topics_info(brake, &incommingMessage, authorizationKey))
			case "brake_push_messages":
				fmt.Fprint(w, emb_rpc_impl_brake_push_messages(brake, &incommingMessage, authorizationKey))
			case "brake_pull_messages":
				fmt.Fprint(w, emb_rpc_impl_brake_pull_messages(brake, &incommingMessage, authorizationKey))
			case "rpc_methods":
				//return list of registered by 1Cs methods
				methodMetadataByMethodName.mx.RLock()
				data, _ := json.Marshal(methodMetadataByMethodName.m)
				methodMetadataByMethodName.mx.RUnlock()
				tempRes, _ := json.Marshal(jsonRPCResponceOK{Id: 0, Result: string(data)})
				fmt.Fprint(w, string(tempRes))
			default:
				//execute 1C methods
				if methodMetadata, ok := methodMetadataByMethodName.Get(incommingMessage.MethodName); ok {
					if requestChanel, ok := requestChanelByServer.Get(methodMetadata.ServiceId); ok {
						responceChanels.ReserveIdAndChanel(&incommingMessage)
						requestChanel <- incommingMessage
						select {
						case incommingResponce := <-responceChanels.Load(incommingMessage.Id):
							if incommingResponce.Error.Code == 0 {
								data, _ := json.Marshal(jsonRPCResponceOK{Id: incommingResponce.Id, Result: incommingResponce.Result})
								fmt.Fprint(w, string(data))
							} else {
								data, _ := json.Marshal(jsonRPCResponceError{Id: incommingResponce.Id, Error: incommingResponce.Error})
								fmt.Fprint(w, string(data))
							}
						case <-time.After(time.Second * 60):
							errorRet := jsonRPCResponceError{Id: incommingMessage.Id, Error: jsonRPCError{Code: -32603, Message: "Timeout"}}
							data, _ := json.Marshal(errorRet)
							fmt.Fprint(w, string(data))
						}
						responceChanels.Delete(incommingMessage.Id)
					} else {
						errorRet := jsonRPCResponceError{Id: incommingMessage.Id, Error: jsonRPCError{Code: -32603, Message: "Internal JSON-RPC error."}}
						data, _ := json.Marshal(errorRet)
						fmt.Fprint(w, string(data))
					}

				} else {
					errorRet := jsonRPCResponceError{Id: incommingMessage.Id, Error: jsonRPCError{Code: -32601, Message: "Procedure not found"}}
					data, _ := json.Marshal(errorRet)
					fmt.Fprint(w, string(data))
				}
			}

		}
	})

	brake.httpServer = http.Server{Addr: ":" + strconv.Itoa(brake.config.TcpPort), Handler: serverMux}
	if err := brake.httpServer.ListenAndServeTLS("https-server.crt", "https-server.key"); err != http.ErrServerClosed {
		log.Fatalln(err)
	}

	log.Println("HTTP Server shutdown...")
}

func emb_rpc_impl_brake_topics_info(brake *brakeContext, incommingMessage *jsonRPCRequest, authorizationKey string) string {

	res := []topicsInfoHttpBody{}

	for _, el := range brake.config.Topics {

		for _, acl := range brake.config.ACL {
			if acl.AuthToken == authorizationKey {
				if find(acl.RW, el.Name) >= 0 || find(acl.RO, el.Name) >= 0 {
					res = append(res, topicsInfoHttpBody{Topic: el.Name, PartitionCount: el.PartitionCount})
				}
			}
		}
	}

	data, _ := json.Marshal(jsonRPCResponceOK{Id: incommingMessage.Id, Result: res})
	return string(data)
}

func emb_rpc_impl_brake_push_messages(brake *brakeContext, incommingMessage *jsonRPCRequest, authorizationKey string) string {

	params := incommingMessage.Params.(map[string]interface{})

	var topic string
	if val, ok := params["topic"]; ok {
		topic = val.(string)
	}

	var partition int
	if val, ok := params["partition"]; ok {
		partition = (int)(val.(float64))
	}

	if !brake.accessGranted(topic, partition) {
		data, _ := json.Marshal(jsonRPCResponceError{Id: incommingMessage.Id, Error: jsonRPCError{Code: -32603, Message: "Topic '" + topic + "' partition '" + strconv.Itoa(partition) + "' not registered"}})
		return string(data)
	}

	if !brake.checkAuthorization(topic, authorizationKey) {
		data, _ := json.Marshal(jsonRPCResponceError{Id: incommingMessage.Id, Error: jsonRPCError{Code: -32603, Message: "Auth error"}})
		return string(data)
	}

	if val, ok := params["messages"]; ok {

		for _, message := range val.([]interface{}) {
			messageData := message.(string)
			brake.push(topic, partition, messageData)
		}

	}

	data, _ := json.Marshal(jsonRPCResponceOK{Id: incommingMessage.Id})
	return string(data)
}

func emb_rpc_impl_brake_pull_messages(brake *brakeContext, incommingMessage *jsonRPCRequest, authorizationKey string) string {

	params := incommingMessage.Params.(map[string]interface{})

	var topic string
	if val, ok := params["topic"]; ok {
		topic = val.(string)
	}

	if !brake.checkAuthorization(topic, authorizationKey) {
		data, _ := json.Marshal(jsonRPCResponceError{Id: incommingMessage.Id, Error: jsonRPCError{Code: -32603, Message: "Auth error"}})
		return string(data)
	}

	var partition int
	if val, ok := params["partition"]; ok {
		partition = (int)(val.(float64))
	}
	//partition := (int)(params["partition"].(float64))

	if !brake.accessGranted(topic, partition) {
		data, _ := json.Marshal(jsonRPCResponceError{Id: incommingMessage.Id, Error: jsonRPCError{Code: -32603, Message: "Topic '" + topic + "' partition '" + strconv.Itoa(partition) + "' not registered"}})
		return string(data)
	}

	maxSize := 10_000_000

	//off := (uint64)(params["off"].(float64))
	var off uint64
	if val, ok := params["off"]; ok {
		off = (uint64)(val.(float64))
	}

	//limit := (int)(params["limit"].(float64))
	var limit int
	if val, ok := params["limit"]; ok {
		limit = (int)(val.(float64))
	}

	//wait := (int64)(params["wait"].(float64))
	var wait int64
	if val, ok := params["wait"]; ok {
		wait = (int64)(val.(float64))
	}

	res := []responseHttpMessage{}
	maxTime := time.Now().UnixNano() + wait*1_000_000_000
	for {
		if limit <= 0 {
			break
		}
		id, timestamp, message := brake.pull(topic, partition, off)
		if id == 0 {
			if (len(res) > 0) || (time.Now().UnixNano() >= maxTime) {
				break
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		tempVal := time.Unix(timestamp, 0)
		res = append(res, responseHttpMessage{Id: id, Time: tempVal.Format(time.RFC3339)[:19], Msg: message})
		off = id + 1
		limit--
		maxSize -= len(message)
		if maxSize < 0 {
			break
		}
	}

	data, _ := json.Marshal(jsonRPCResponceOK{Id: incommingMessage.Id, Result: res})
	return string(data)

}

func (brake *brakeContext) checkAuthorization(topic string, authorizationKey string) bool {
	for _, topicACL := range brake.config.ACL {
		if topicACL.AuthToken == authorizationKey {
			if find(topicACL.RW, topic) != -1 {
				return true
			}
			if find(topicACL.RO, topic) != -1 {
				return true
			}
		}
	}
	return false
}

const dataFileName = "data"
const indexFileName = "index"
const postStateFileName = "pst"

type brakeErrorResponceBody struct {
	Error string `json:"error"`
}

type incommingHttpBody struct {
	Topic     string   `json:"topic"`
	Partition int      `json:"partition"`
	Messages  []string `json:"messages"`
}

type topicConfigStruct struct {
	Name               string `yaml:"name"`
	DataRetentionHours int    `yaml:"data_retention_hours"`
	DataRetentionBytes int64  `yaml:"data_retention_bytes"`
	PartitionCount     int    `yaml:"partition_count"`
	GeneralAccess      bool   `yaml:"general_access"`
}

type ACLConfigStruct struct {
	ClientID  string   `yaml:"client_id"`
	AuthToken string   `yaml:"auth_token"`
	RW        []string `yaml:"rw,flow"`
	RO        []string `yaml:"ro,flow"`
}

type rootConfigStruct struct {
	DataPath string `yaml:"data_path"`
	TcpPort  int    `yaml:"tcp_port"`

	Topics []topicConfigStruct `yaml:",flow"`
	ACL    []ACLConfigStruct   `acl:",flow"`
}

func loadConfigFromFile(cfgFilename string) (rootConfigStruct, error) {

	cfgBytesBuff, err := ioutil.ReadFile(cfgFilename)
	if err != nil {
		log.Fatalln(err)
	}

	result := rootConfigStruct{DataPath: "", TcpPort: 9200}

	err = yaml.Unmarshal(cfgBytesBuff, &result)

	for idx, topicConfig := range result.Topics {
		if topicConfig.PartitionCount == 0 {
			result.Topics[idx].PartitionCount = 1
		}
		if topicConfig.DataRetentionHours == 0 {
			result.Topics[idx].DataRetentionHours = 720 // 30 (days) * 24 hours
		}
		if topicConfig.DataRetentionBytes == 0 {
			result.Topics[idx].DataRetentionBytes = 100_000_000_000
		}
	}

	return result, err
}

func Brake(cfgFileName string) *brakeContext {

	brake := new(brakeContext)
	//brake.cfgFileName = cfgFileName
	brake.files = make(map[string]*os.File)
	brake.topicsContexts = make(map[string]*topicContext)

	var err error
	var config rootConfigStruct
	if config, err = loadConfigFromFile(cfgFileName); err != nil {
		log.Fatalln(err)
	}
	brake.applyConfig(&config)

	log.Println("data_path", brake.config.DataPath)
	log.Println("tcp_port", brake.config.TcpPort)
	log.Println(brake.config.Topics)

	go func() {
		for {
			time.Sleep(time.Duration(1) * time.Second)

			if config, err := loadConfigFromFile(cfgFileName); err == nil {
				brake.applyConfig(&config)
			} else {
				log.Println(err)
			}
		}

	}()

	go func() {
		for {
			brake.syncFiles()
			time.Sleep(time.Duration(1) * time.Second)
		}

	}()

	return brake
}

func (brake *brakeContext) applyConfig(config *rootConfigStruct) {
	brake.config = *config
	for _, el := range config.Topics {

		for partition := 0; partition < el.PartitionCount; partition++ {
			topicContext := brake.getTopicContext(el.Name, partition)
			topicContext.ChunkDataRetentionSec = el.DataRetentionHours * 3600 / CHUNK_COUNT
			topicContext.ChunkDataRetentionBytes = el.DataRetentionBytes / CHUNK_COUNT / int64(el.PartitionCount)
		}
	}
}

func (brake *brakeContext) syncFiles() {
	temp := make(map[string]*os.File)
	brake.Lock()
	for k, v := range brake.files {
		temp[k] = v
	}
	brake.Unlock()

	for _, f := range temp {
		f.Sync()
	}
}

/*
func (brake *brakeContext) processPush(w http.ResponseWriter, r *http.Request) {

	setGeneralResponseHeaders(w)

	body, _ := io.ReadAll(r.Body)
	var incommingMessage incommingHttpBody
	json.Unmarshal(body, &incommingMessage)

	if !brake.accessGranted(incommingMessage.Topic, incommingMessage.Partition) {
		//notRegisteredTopicErrorHTTPResponce(w, incommingMessage.Topic, incommingMessage.Partition)
		jsonData, _ := json.Marshal(brakeErrorResponceBody{Error: "Topic '" + incommingMessage.Topic + "' partition '" + strconv.Itoa(incommingMessage.Partition) + "' not registered"})
		http.Error(w, string(jsonData), 400)

		return
	}

	if !brake.checkAuthorization(incommingMessage.Topic, r.Header.Get("Authorization")) {
		http.Error(w, "Forbidden", 403)
		return
	}

	if incommingMessage.Messages == nil {
		http.Error(w, `Field "messages" not found`, 400)
		return
	}

	for _, message := range incommingMessage.Messages {
		brake.push(incommingMessage.Topic, incommingMessage.Partition, message)
	}

	responce := brakeErrorResponceBody{Error: ""}

	json_data, _ := json.Marshal(responce)
	fmt.Fprint(w, string(json_data))
}
*/

type responseHttpMessage struct {
	Id   uint64 `json:"id"`
	Time string `json:"time"`
	Msg  string `json:"payload"`
}

func (brake *brakeContext) accessGranted(topic string, partition int) (granted bool) {

	if brake.topicsContexts[topic+"_"+strconv.Itoa(partition)] == nil {
		return false
	}
	return true
}

func setGeneralResponseHeaders(w http.ResponseWriter) {

	w.Header().Add("Content-Type", "application/json; charset=utf-8")
}

/*
func notRegisteredTopicErrorHTTPResponce(w http.ResponseWriter, topic string, partition int) {
	//w.WriteHeader(http.StatusBadRequest)

	//fmt.Fprint(w, string(jsonData))
}
*/

type topicsInfoHttpBody struct {
	Topic          string `json:"topic"`
	PartitionCount int    `json:"partition_count"`
}

func find(a []string, x string) int {
	for i, n := range a {
		if x == n {
			return i
		}
	}
	return -1
}

/*
func (brake *brakeContext) processTopicsInfo(w http.ResponseWriter, r *http.Request) {

	authorizationKey := r.Header.Get("Authorization")

	setGeneralResponseHeaders(w)

	res := []topicsInfoHttpBody{}

	for _, el := range brake.config.Topics {

		for _, acl := range brake.config.ACL {
			if acl.AuthToken == authorizationKey {
				if find(acl.RW, el.Name) >= 0 || find(acl.RO, el.Name) >= 0 {
					res = append(res, topicsInfoHttpBody{Topic: el.Name, PartitionCount: el.PartitionCount})
				}

			}
		}

	}

	jsonData, _ := json.Marshal(res)
	fmt.Fprint(w, string(jsonData))
}
*/

/*
func (brake *brakeContext) processPull(w http.ResponseWriter, r *http.Request) {

	setGeneralResponseHeaders(w)

	queryParams := r.URL.Query()

	topic := queryParams.Get("topic")

	if !brake.checkAuthorization(topic, r.Header.Get("Authorization")) {
		http.Error(w, "Forbidden", 403)
		return
	}

	partition, _ := strconv.Atoi(queryParams.Get("partition"))

	if !brake.accessGranted(topic, partition) {
		jsonData, _ := json.Marshal(brakeErrorResponceBody{Error: "Topic '" + topic + "' partition '" + strconv.Itoa(partition) + "' not registered"})
		http.Error(w, string(jsonData), 400)
		//notRegisteredTopicErrorHTTPResponce(w, topic, partition)
		return
	}

	off, _ := strconv.ParseUint(queryParams.Get("off"), 10, 64)
	limit, limitError := strconv.Atoi(queryParams.Get("limit"))
	if limitError != nil {
		limit = 1
	}

	if off == 0 {
		w.Header().Add("Cache-Control", "max-age=1")
	}

	maxSize, maxSizeError := strconv.Atoi(queryParams.Get("max_size"))
	if maxSizeError != nil {
		maxSize = 10_000_000
	}

	res := []responseHttpMessage{}

	for {
		if limit <= 0 {
			break
		}

		id, timestamp, message := brake.pull(topic, partition, off)
		if id == 0 {
			break
		}

		tempVal := time.Unix(timestamp, 0)

		res = append(res, responseHttpMessage{Id: id, Time: tempVal.Format(time.RFC3339)[:19], Msg: message})
		off = id + 1
		limit--
		maxSize -= len(message)
		if maxSize < 0 {
			break
		}
	}

	jsonData, _ := json.Marshal(res)
	fmt.Fprint(w, string(jsonData))
}
*/

func (brake *brakeContext) Close() {
	for _, f := range brake.files {
		f.Close()
	}
}

func (brake *brakeContext) stopHttpService() {
	brake.httpServer.Shutdown(context.Background())
}

type chunkContext struct {
	firstMessageId uint64
	lastMessageId  uint64
}

type topicContext struct {
	putStateInitialized       bool
	activeChunkNo             int
	ChunkDataRetentionSec     int
	ChunkDataRetentionBytes   int64
	firstMessageIdForNewChunk uint64
	startTime                 int64
	chunksContexts            []*chunkContext
	sync.Mutex
}

type brakeContext struct {
	httpServer     http.Server
	config         rootConfigStruct
	files          map[string]*os.File
	topicsContexts map[string]*topicContext
	sync.Mutex
}

func (brake *brakeContext) openIndexFile(topic string, partition int, chunk string) (f_idx *os.File, firstId uint64, count uint64) {

	f_idx = brake.openFile(topic, partition, indexFileName+chunk)

	pos, err := f_idx.Seek(0, io.SeekEnd)
	if err != nil {
		log.Fatalln(err)
	}
	if pos > 0 {
		firstId = readUint64At(f_idx, 0)
		count = messageCountByIndexFileSize(int(pos))
	}
	return f_idx, firstId, count
}

const CHUNK_COUNT = 4

func chunks(priorityChunk int) (result []int) {

	switch priorityChunk {
	case 0:
		return []int{0, 1, 2, 3}
	case 1:
		return []int{1, 0, 2, 3}
	case 2:
		return []int{2, 0, 1, 3}
	case 3:
		return []int{3, 0, 1, 2}
	}

	log.Fatalln("chunks(priorityChunk) out of range [0..3]")
	return result
}

func chunkDisplayName(chunk int) string {

	switch chunk {
	case 0:
		return "_a"
	case 1:
		return "_b"
	case 2:
		return "_c"
	case 3:
		return "_d"
	}
	log.Fatalln("chunkDisplayName(chunk) out of range [0..3]")

	return "_z"
}

func (brake *brakeContext) info(topic string, partition int) (FirstId uint64, LastId uint64) {

	for _, chunk := range chunks(0) {
		_, firstId, count := brake.openIndexFile(topic, partition, chunkDisplayName(chunk))
		if firstId == 0 {
			continue
		}
		if FirstId == 0 || firstId < FirstId {
			FirstId = firstId
		}
		lastId := firstId + count - 1
		if lastId > LastId {
			LastId = lastId
		}
	}
	return FirstId, LastId
}

func topicPartitionKey(topic string, partition int) string {
	return topic + "_" + strconv.Itoa(partition)
}

func (brake *brakeContext) getTopicContext(topic string, partition int) *topicContext {

	if brake.topicsContexts[topicPartitionKey(topic, partition)] == nil {
		brake.topicsContexts[topicPartitionKey(topic, partition)] = &topicContext{chunksContexts: make([]*chunkContext, len(chunks(0)))}
		for _, chunk := range chunks(0) {
			brake.topicsContexts[topicPartitionKey(topic, partition)].chunksContexts[chunk] = &chunkContext{}
		}
	}
	return brake.topicsContexts[topicPartitionKey(topic, partition)]
}

func (brake *brakeContext) predictChunk(topic string, partition int, id uint64) int {
	result := 0
	for _, chunk := range chunks(0) {
		chunkCtx := brake.getTopicContext(topic, partition).chunksContexts[chunk]
		if id >= chunkCtx.firstMessageId && id <= chunkCtx.lastMessageId {
			result = chunk
			break
		}
	}
	return result
}

func (brake *brakeContext) updatePredictData(topic string, partition int, chunk int, firstId uint64, count uint64) {

	chunkCtx := brake.getTopicContext(topic, partition).chunksContexts[chunk]

	if (chunkCtx.firstMessageId != firstId) || (chunkCtx.lastMessageId != firstId+count-1) {
		chunkCtx.firstMessageId = firstId
		chunkCtx.lastMessageId = firstId + count - 1
	}
}

func writeMessage(f_data *os.File, message string) {
	var b bytes.Buffer
	b.WriteByte(0) //version

	timestampBuff := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestampBuff, uint64(time.Now().Unix()))
	b.Write(timestampBuff) //timestamp

	messageSizeBuff := make([]byte, 4)
	binary.LittleEndian.PutUint32(messageSizeBuff, uint32(len(message)))
	b.Write(messageSizeBuff) //size

	b.WriteString(message) //message
	f_data.Write(b.Bytes())
}

func readMessage(f_data *os.File, dataFilePos int64) (timestamp int64, message string) {
	_ = readUint8At(f_data, dataFilePos) //version

	timestamp = int64(readUint64At(f_data, dataFilePos+1))

	messageSize := readUint32At(f_data, dataFilePos+1+8)
	messageBytesBuff := make([]byte, messageSize)
	f_data.ReadAt(messageBytesBuff, int64(dataFilePos+1+8+4))

	return timestamp, string(messageBytesBuff)
}

func readUint8At(f *os.File, at int64) uint8 {
	data := make([]byte, 1)
	f.ReadAt(data, at)
	return data[0]
}

func readUint32At(f *os.File, at int64) uint32 {
	data := make([]byte, 4)
	f.ReadAt(data, at)
	return uint32(binary.LittleEndian.Uint32(data))
}

func readUint64At(f *os.File, at int64) uint64 {
	data := make([]byte, 8)
	f.ReadAt(data, at)
	return uint64(binary.LittleEndian.Uint64(data))
}

func writeUint8(f *os.File, data uint8) {
	buff := make([]byte, 1)
	buff[0] = data
	f.Write(buff)
}

func writeUint32(f *os.File, data uint32) {
	buff := make([]byte, 4)
	binary.LittleEndian.PutUint32(buff, data)
	f.Write(buff)
}

func writeUint64(f *os.File, data uint64) {
	buff := make([]byte, 8)
	binary.LittleEndian.PutUint64(buff, data)
	f.Write(buff)
}

func (brake *brakeContext) activePushChunk(topic string, partition int) (int, uint64) {

	topicCtx := brake.getTopicContext(topic, partition)
	if !topicCtx.putStateInitialized {

		f_putState := brake.openFile(topic, partition, postStateFileName)
		pos, _ := f_putState.Seek(0, io.SeekEnd)
		if pos == 0 {
			topicCtx.startTime = time.Now().Unix()
			writeUint32(f_putState, uint32(topicCtx.activeChunkNo))
			writeUint64(f_putState, uint64(topicCtx.startTime))

			topicCtx.firstMessageIdForNewChunk = 1
		} else {
			topicCtx.activeChunkNo = int(readUint32At(f_putState, 0))
			topicCtx.startTime = int64(readUint64At(f_putState, 4))
			topicCtx.firstMessageIdForNewChunk = readUint64At(f_putState, 4+8)
		}
		topicCtx.putStateInitialized = true
	}
	return int(topicCtx.activeChunkNo), topicCtx.firstMessageIdForNewChunk
}

func switchChunk(brake *brakeContext, topic string, partition int, startingMessageIdForNewChunk uint64) (newChunk int) {

	topicCtx := brake.getTopicContext(topic, partition)
	f_putState := brake.openFile(topic, partition, postStateFileName)
	newChunk = int(readUint32At(f_putState, 0) + 1)
	if newChunk >= len(chunks(0)) {
		newChunk = 0
	}

	f_idx := brake.openFile(topic, partition, indexFileName+chunkDisplayName(newChunk))
	f_idx.Truncate(0)

	f_date := brake.openFile(topic, partition, dataFileName+chunkDisplayName(newChunk))
	f_date.Truncate(0)

	timeNow := time.Now().Unix()

	f_putState.Seek(0, io.SeekStart)
	writeUint32(f_putState, uint32(newChunk))
	writeUint64(f_putState, uint64(timeNow))
	writeUint64(f_putState, startingMessageIdForNewChunk)

	topicCtx.activeChunkNo = newChunk
	topicCtx.firstMessageIdForNewChunk = startingMessageIdForNewChunk
	topicCtx.startTime = timeNow

	log.Println("Chunk switcher topic:", topic, "partition:", partition, "startingMessageIdForNewChunk:", startingMessageIdForNewChunk, "new chunk:", newChunk)

	return newChunk
}

func (brake *brakeContext) openFile(topic string, partition int, fileName string) *os.File {

	brake.Lock()
	res := brake.files[topic+strconv.Itoa(partition)+fileName]

	if res == nil {

		if _, err := os.Stat(brake.config.DataPath + topic); os.IsNotExist(err) {
			err = os.Mkdir(brake.config.DataPath+topic, 0777)
			if err != nil {
				log.Fatalln(err)
			}
		}

		var err error
		res, err = os.OpenFile(brake.config.DataPath+topic+"/"+fileName+"_"+strconv.Itoa(partition), os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			log.Fatalln(err)
		}
		brake.files[topic+strconv.Itoa(partition)+fileName] = res
	}
	brake.Unlock()

	return res

}

func messageCountByIndexFileSize(indexFileSize int) uint64 {
	if (indexFileSize>>3 - 1) <= 0 {
		return 0
	}
	return uint64(indexFileSize>>3 - 1)
}

func (brake *brakeContext) push(topic string, partition int, message string) {

	/*
		data := []byte(message)
		buf := make([]byte, len(data))
		ht := make([]int, 64<<10) // buffer for the compression table

		n, err := lz4.CompressBlock(data, buf, ht)
		if err != nil {
			fmt.Println(err)
		}
		if n >= len(data) {
			fmt.Printf("`%s` is not compressible", message)
		} else {
			fmt.Printf("orig size %d, compress size %d \n", len(data), n)
		}
		buf = buf[:n] // compressed data

		// Allocated a very large buffer for decompression.
		out := make([]byte, 10*len(data))
		n, err = lz4.UncompressBlock(buf, out)
		if err != nil {
			fmt.Println(err)
		}
		out = out[:n] // uncompressed data

		fmt.Println(string(out[:len(message)]))
	*/
	/////////////////////////////////////////////

	topicCtx := brake.getTopicContext(topic, partition)
	topicCtx.Lock()
	defer topicCtx.Unlock()

	chunk, startingMessageIdFowNewChunk := brake.activePushChunk(topic, partition)

	f_idx := brake.openFile(topic, partition, indexFileName+chunkDisplayName(chunk))

	indexFileSize, _ := f_idx.Seek(0, io.SeekEnd)
	if indexFileSize == 0 {
		writeUint64(f_idx, uint64(startingMessageIdFowNewChunk))
	}

	f_data := brake.openFile(topic, partition, dataFileName+chunkDisplayName(chunk))

	offset_data, _ := f_data.Seek(0, io.SeekEnd)

	writeMessage(f_data, message)

	writeUint64(f_idx, uint64(offset_data)) //update index data

	brake.updatePredictData(topic, partition, chunk,
		startingMessageIdFowNewChunk,
		messageCountByIndexFileSize(int(indexFileSize+1)))

	secondsElapsed := int((time.Now().Unix() - brake.getTopicContext(topic, partition).startTime))

	if secondsElapsed > topicCtx.ChunkDataRetentionSec || offset_data > topicCtx.ChunkDataRetentionBytes {
		currentStartMessageId := readUint64At(f_idx, 0)
		idx_fileLen, _ := f_idx.Seek(0, io.SeekEnd)
		firstMessageId := currentStartMessageId + messageCountByIndexFileSize(int(idx_fileLen))
		newChunk := switchChunk(brake, topic, partition, firstMessageId)
		brake.updatePredictData(topic, partition, newChunk, firstMessageId, 0)
	}
}

func (brake *brakeContext) pull(topic string, partition int, off uint64) (id uint64, timestamp int64, message string) {

	if off == 0 {
		_, off = brake.info(topic, partition)
	}

	id = 0
	var beside_f_idx *os.File
	beside_chunk := 0
	beside_idDelta := uint64(0xffffffffffffffff)
	var beside_idxStartId uint64

	for _, chunk := range chunks(brake.predictChunk(topic, partition, off)) {
		f_idx, firstId, count := brake.openIndexFile(topic, partition, chunkDisplayName(chunk))
		if count > 0 {
			brake.updatePredictData(topic, partition, chunk, firstId, count)
			if off >= firstId && off < (firstId+count) {
				dataFilePos := int64(readUint64At(f_idx, int64(off-firstId+1)<<3))
				f_data := brake.openFile(topic, partition, dataFileName+chunkDisplayName(chunk))
				id = off
				timestamp, message = readMessage(f_data, dataFilePos)
				return id, timestamp, message
			}
			if off < firstId {
				tempIdDelta := firstId - off
				if tempIdDelta > 0 && (tempIdDelta < beside_idDelta) {
					beside_idDelta = tempIdDelta
					beside_f_idx = f_idx
					beside_chunk = chunk
					beside_idxStartId = firstId
				}
			}
		}
	}

	if beside_f_idx != nil {
		f_data := brake.openFile(topic, partition, dataFileName+chunkDisplayName(beside_chunk))
		id = beside_idxStartId
		timestamp, message = readMessage(f_data, 0)
	}

	return id, timestamp, message
}
