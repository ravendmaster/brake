package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {

	brake := Brake()
	defer brake.Close()

	//brake.info("test")

	go brake.doTest("test", 60_000)
	//go brake.doTest("alpha", 50_000)
	//go brake.doTest("beta", 100_000)
	//go brake.doTest("gamma", 200_000)
	//go brake.doTest("teta", 200_000)

	http.HandleFunc("/push/v1", func(w http.ResponseWriter, r *http.Request) {

		if !checkAuthorization(w, r) {
			return
		}
		switch r.Method {
		case "POST":
			processPush(brake, w, r)
		}
	})
	http.HandleFunc("/pull/v1", func(w http.ResponseWriter, r *http.Request) {

		if !checkAuthorization(w, r) {
			return
		}
		switch r.Method {
		case "GET":
			processPull(brake, w, r)

		}
	})

	err := http.ListenAndServe(":"+strconv.Itoa(brake.config.TcpPort), nil)
	if err != nil {
		panic(err)
	}

}

func checkAuthorization(w http.ResponseWriter, r *http.Request) bool {
	AuthorizationKey := r.Header["Authorization"]
	if AuthorizationKey == nil {
		return false
	}
	//println(AuthorizationKey[0])
	return true
}

func (brake *brakeContext) doTest(topic string, count int) {

	//brake.send("betta2", "hi")
	//Get(ctx, "test", 9872938293874)

	message := strings.Repeat("1", 512)

	_, lastId := brake.info(topic)

	id := lastId + 1
	if lastId == 0 {
		id = 1
	}

	idForRead := id

	var k int
	start := time.Now().UnixNano()
	for k = 1; k <= count; k++ {
		testString := strconv.Itoa(int(id)) + "             " + message
		brake.push(topic, testString)
		id++
	}
	println(topic, "put time", (time.Now().UnixNano()-start)/1000/1000)

	start = time.Now().UnixNano()

	for i := 1; i < k; i++ {
		id, message := brake.pull(topic, idForRead)
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

	firstId, lastId := brake.info(topic)
	println(topic, "messages from", firstId, "to", lastId)
}

const dataFileName = "data"
const indexFileName = "index"
const postStateFileName = "pst"

type brakeInfo struct {
	Error string `json:"error"`
}

type incommingHttpBody struct {
	Topic    string   `json:"topic"`
	Messages []string `json:"messages"`
}

type rootConfigStruct struct {
	DataPath string              `json:"data_path"`
	TcpPort  int                 `json:"tcp_port"`
	Topics   []topicConfigStruct `json:"topics"`
}

type topicConfigStruct struct {
	Name              string `json:"name"`
	ChunkStoreTimeSec int    `json:"chunk_store_time_sec"`
}

func loadConfigFromFile() (rootConfigStruct, error) {

	f_cfg, err := os.OpenFile("config.json", os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		println(err)
	}
	defer f_cfg.Close()

	fileSize, _ := f_cfg.Seek(0, io.SeekEnd)
	messageBytesBuff := make([]byte, fileSize)
	f_cfg.ReadAt(messageBytesBuff, 0)

	result := rootConfigStruct{DataPath: "", TcpPort: 80}
	err = json.Unmarshal(messageBytesBuff, &result)

	for idx, topicConfig := range result.Topics {
		if topicConfig.ChunkStoreTimeSec == 0 {
			result.Topics[idx].ChunkStoreTimeSec = 86400 * 30 // 30 days
		}
	}

	return result, err
}

func Brake() *brakeContext {

	brake := new(brakeContext)
	brake.files = make(map[string]*os.File)
	brake.topicsContexts = make(map[string]*topicContext)

	var err error
	var config rootConfigStruct
	if config, err = loadConfigFromFile(); err != nil {
		panic(err)
	}
	brake.applyConfig(&config)

	fmt.Println("data_path", brake.config.DataPath)
	fmt.Println("tcp_port", brake.config.TcpPort)
	fmt.Println(brake.config.Topics)

	go func() {
		for {
			time.Sleep(time.Duration(1) * time.Second)

			config, err := loadConfigFromFile()
			if err == nil {
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
		topicContext := brake.getTopicContext(el.Name)
		topicContext.ChunkStoreTimeSec = el.ChunkStoreTimeSec
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

func processPush(brake *brakeContext, w http.ResponseWriter, r *http.Request) {

	r.Header.Add("Content-Type", "text/plain; charset=utf-8")

	body, _ := io.ReadAll(r.Body)
	var incommingMessage incommingHttpBody
	json.Unmarshal([]byte(body), &incommingMessage)

	if !brake.accessGranted(incommingMessage.Topic) {
		w.WriteHeader(http.StatusBadRequest)
		jsonData, _ := json.Marshal(brakeInfo{Error: "Topic '" + incommingMessage.Topic + "' not registered"})
		fmt.Fprint(w, string(jsonData))
		return
	}

	for _, message := range incommingMessage.Messages {

		brake.push(incommingMessage.Topic, message)
	}
	res := brakeInfo{Error: ""}
	json_data, _ := json.Marshal(res)
	fmt.Fprint(w, string(json_data))
}

type responseHttpMessage struct {
	Id  uint64 `json:"id"`
	Msg string `json:"msg"`
}

func (brake *brakeContext) accessGranted(topic string) (granted bool) {

	if brake.topicsContexts[topic] == nil {
		return false
	}
	return true
}

func processPull(brake *brakeContext, w http.ResponseWriter, r *http.Request) {

	r.Header.Add("Content-Type", "text/plain; charset=utf-8")

	queryParams := r.URL.Query()

	topic := queryParams["topic"][0]

	if !brake.accessGranted(topic) {
		w.WriteHeader(http.StatusBadRequest)
		jsonData, _ := json.Marshal(brakeInfo{Error: "Topic '" + topic + "'not registered"})
		fmt.Fprint(w, string(jsonData))
		return
	}

	off, _ := strconv.ParseUint(queryParams["off"][0], 10, 64)
	limit, _ := strconv.Atoi(queryParams["limit"][0])

	if off == 0 {
		w.Header().Add("Cache-Control", "max-age=1")
	}

	maxSize := 10_000_000
	if queryParams["max_size"] != nil {
		maxSize, _ = strconv.Atoi(queryParams["max_size"][0])
	}

	res := []responseHttpMessage{}

	for {
		if limit <= 0 {
			break
		}

		id, message := brake.pull(topic, off)
		if id == 0 {
			break
		}
		var httpMessage = responseHttpMessage{Id: id, Msg: message}
		res = append(res, httpMessage)
		off = id + 1
		limit--
		maxSize -= len(httpMessage.Msg)
		if maxSize < 0 {
			break
		}

	}

	jsonData, _ := json.Marshal(res)
	fmt.Fprint(w, string(jsonData))
}

func (brake *brakeContext) Close() {
	for _, f := range brake.files {
		f.Close()
	}
}

type chunkContext struct {
	firstMessageId uint64
	lastMessageId  uint64
}

type topicContext struct {
	initialized               bool
	activeChunkNo             int
	ChunkStoreTimeSec         int
	firstMessageIdForNewChunk uint64
	startTime                 int64

	chunksContexts []*chunkContext

	sync.Mutex
}

type brakeContext struct {
	config         rootConfigStruct
	files          map[string]*os.File
	topicsContexts map[string]*topicContext
	sync.Mutex
}

func (brake *brakeContext) openIndexFile(topic string, chunk string) (f_idx *os.File, firstId uint64, count uint64) {

	f_idx = brake.openFile(topic, indexFileName+chunk)

	//var firstId uint64
	//var count uint64

	pos, err := f_idx.Seek(0, io.SeekEnd)
	if err != nil {
		println(err)
	}
	if pos > 0 {
		firstId = readUint64At(f_idx, 0)
		count = messageCountByIndexFileSize(int(pos))
	}

	return f_idx, firstId, count
}

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

	panic("error")
}

func chunkDisplayName(chunk int) string {

	switch chunk {
	case 0:
		return "_0"
	case 1:
		return "_1"
	case 2:
		return "_2"
	case 3:
		return "_3"
	}
	panic("chunkDisplayName() overflow")
}

func (brake *brakeContext) info(topic string) (FirstId uint64, LastId uint64) {

	for _, chunk := range chunks(0) {
		_, firstId, count := brake.openIndexFile(topic, chunkDisplayName(chunk))
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

func (brake *brakeContext) getTopicContext(topic string) *topicContext {
	if brake.topicsContexts[topic] == nil {
		brake.topicsContexts[topic] = &topicContext{chunksContexts: make([]*chunkContext, len(chunks(0)))}
		for _, chunk := range chunks(0) {
			brake.topicsContexts[topic].chunksContexts[chunk] = &chunkContext{}
		}
	}
	return brake.topicsContexts[topic]
}

func (brake *brakeContext) predictChunk(topic string, id uint64) int {
	result := 0

	for _, chunk := range chunks(0) {

		chunkCtx := brake.getTopicContext(topic).chunksContexts[chunk]

		if id >= chunkCtx.firstMessageId && id <= chunkCtx.lastMessageId {
			result = chunk
			break
		}
	}
	return result
}

func (brake *brakeContext) updatePredictData(topic string, chunk int, firstId uint64, count uint64) {

	chunkCtx := brake.getTopicContext(topic).chunksContexts[chunk]

	if (chunkCtx.firstMessageId != firstId) || (chunkCtx.lastMessageId != firstId+count-1) {
		chunkCtx.firstMessageId = firstId
		chunkCtx.lastMessageId = firstId + count - 1
	}
}

func readMessage(f_data *os.File, dataFilePos int64) (message string) {
	readUint8At(f_data, dataFilePos) //version
	messageSize := readUint32At(f_data, dataFilePos+1)
	messageBytesBuff := make([]byte, messageSize)
	f_data.ReadAt(messageBytesBuff, int64(dataFilePos+1+4))
	return string(messageBytesBuff)
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

func (brake *brakeContext) activeChunk(topic string) (int, uint64) {

	topicCtx := brake.getTopicContext(topic)
	if !topicCtx.initialized {

		f_putState := brake.openFile(topic, postStateFileName)
		pos, _ := f_putState.Seek(0, io.SeekEnd)
		if pos == 0 {
			//initialize
			topicCtx.startTime = time.Now().Unix()
			writeUint32(f_putState, uint32(topicCtx.activeChunkNo))
			writeUint64(f_putState, uint64(topicCtx.startTime))

			topicCtx.firstMessageIdForNewChunk = 1
		} else {
			topicCtx.activeChunkNo = int(readUint32At(f_putState, 0))
			topicCtx.startTime = int64(readUint64At(f_putState, 4))
			topicCtx.firstMessageIdForNewChunk = readUint64At(f_putState, 4+8)
		}

		topicCtx.initialized = true

	}

	return int(topicCtx.activeChunkNo), topicCtx.firstMessageIdForNewChunk
}

func switchChunk(brake *brakeContext, topic string, startingMessageIdForNewChunk uint64) (newChunk int) {

	topicCtx := brake.getTopicContext(topic)

	f_putState := brake.openFile(topic, postStateFileName)

	newChunk = int(readUint32At(f_putState, 0) + 1)
	if newChunk >= len(chunks(0)) {
		newChunk = 0
	}

	f_idx := brake.openFile(topic, indexFileName+chunkDisplayName(newChunk))
	f_idx.Truncate(0)

	f_date := brake.openFile(topic, dataFileName+chunkDisplayName(newChunk))
	f_date.Truncate(0)

	timeNow := time.Now().Unix()

	f_putState.Seek(0, io.SeekStart)
	writeUint32(f_putState, uint32(newChunk))
	writeUint64(f_putState, uint64(timeNow))
	writeUint64(f_putState, startingMessageIdForNewChunk)

	topicCtx.activeChunkNo = newChunk
	topicCtx.firstMessageIdForNewChunk = startingMessageIdForNewChunk
	topicCtx.startTime = timeNow

	return newChunk
}

func (brake *brakeContext) openFile(topic string, name string) *os.File {

	brake.Lock()
	res := brake.files[topic+name]

	if res == nil {

		if _, err := os.Stat(brake.config.DataPath + topic); os.IsNotExist(err) {
			err = os.Mkdir(brake.config.DataPath+topic, 0777)
			if err != nil {
				panic(err)
			}
		}

		var err error
		res, err = os.OpenFile(brake.config.DataPath+topic+"/"+name, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			println(err)
		}
		brake.files[topic+name] = res
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

func getTopicConfigByName(brake *brakeContext, topic string) *topicConfigStruct {
	for _, el := range brake.config.Topics {
		if el.Name == topic {
			return &el
		}
	}
	return nil
}

func (brake *brakeContext) push(topic string, message string) {

	/*
		var b bytes.Buffer
		w := zlib.NewWriter(&b)
		w.Write([]byte(message))
		w.Close()
		f_data.Write(b.Bytes())
	*/

	topicCtx := brake.getTopicContext(topic)
	topicCtx.Lock()

	chunk, startingMessageIdFowNewChunk := brake.activeChunk(topic)

	f_idx := brake.openFile(topic, indexFileName+chunkDisplayName(chunk))

	indexFileSize, _ := f_idx.Seek(0, io.SeekEnd)
	if indexFileSize == 0 {
		writeUint64(f_idx, uint64(startingMessageIdFowNewChunk))
	}

	f_data := brake.openFile(topic, dataFileName+chunkDisplayName(chunk))

	offset_data, _ := f_data.Seek(0, io.SeekEnd)

	var b bytes.Buffer
	b.WriteByte(0)
	buff := make([]byte, 4)
	binary.LittleEndian.PutUint32(buff, uint32(len(message)))
	b.Write(buff)
	b.WriteString(message)
	f_data.Write(b.Bytes())

	writeUint64(f_idx, uint64(offset_data))

	brake.updatePredictData(topic, chunk,
		startingMessageIdFowNewChunk,
		messageCountByIndexFileSize(int(indexFileSize+1)))

	daysElapsed := int((time.Now().Unix() - brake.getTopicContext(topic).startTime))

	if daysElapsed > topicCtx.ChunkStoreTimeSec || offset_data >= 100_000_000_000 {
		currentStartMessageId := readUint64At(f_idx, 0)
		idx_fileLen, _ := f_idx.Seek(0, io.SeekEnd)
		firstMessageId := currentStartMessageId + messageCountByIndexFileSize(int(idx_fileLen))
		newChunk := switchChunk(brake, topic, firstMessageId)
		brake.updatePredictData(topic, newChunk, firstMessageId, 0)
	}
	topicCtx.Unlock()
}

func (brake *brakeContext) pull(topic string, off uint64) (id uint64, message string) {

	if off == 0 {
		_, off = brake.info(topic)
	}

	id = 0
	var beside_f_idx *os.File
	beside_chunk := 0
	beside_idDelta := uint64(0xffffffffffffffff)
	var beside_idxStartId uint64

	for _, chunk := range chunks(brake.predictChunk(topic, off)) {
		f_idx, firstId, count := brake.openIndexFile(topic, chunkDisplayName(chunk))
		if count > 0 {
			brake.updatePredictData(topic, chunk, firstId, count)
			if off >= firstId && off < (firstId+count) {
				dataFilePos := int64(readUint64At(f_idx, int64(off-firstId+1)<<3))
				f_data := brake.openFile(topic, dataFileName+chunkDisplayName(chunk))
				id = off
				message = readMessage(f_data, dataFilePos)
				return id, message
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
		f_data := brake.openFile(topic, dataFileName+chunkDisplayName(beside_chunk))
		id = beside_idxStartId
		message = readMessage(f_data, 0)
	}

	return id, message
}
