package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {

	brakes := Brakes()
	defer brakes.Close()

	go brakes.doTest("test")
	go brakes.doTest("alpha")
	go brakes.doTest("beta")
	go brakes.doTest("gamma")

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		switch r.Method {
		case "POST":
			processPOSTMethod(brakes, w, r)
		case "GET":
			processGETMethod(brakes, w, r)
		}
	})
	http.ListenAndServe(":"+strconv.Itoa(brakes.config.TcpPort), nil)
}

func (brakes *brakes) doTest(queue string) {

	//brakes.send("betta2", "hi")
	//Get(ctx, "test", 9872938293874)

	message := strings.Repeat("1", 512)

	queueInfo := brakes.info(queue)

	id := queueInfo.LastId + 1
	if queueInfo.LastId == -1 {
		id = 1
	}

	idForRead := id

	var k int
	start := time.Now().UnixNano()
	for k = 1; k <= 500000; k++ {
		brakes.send(queue, strconv.Itoa(id)+"             "+message)
		id++
	}
	println(queue, "put time", (time.Now().UnixNano()-start)/1000/1000)

	start = time.Now().UnixNano()

	for i := 1; i < k; i++ {
		message := brakes.read(queue, idForRead)
		if message.Id == -1 {
			println(queue, "no more", idForRead)
			break
		}

		idForRead = message.Id + 1

		res, _ := strconv.Atoi(strings.Split(message.Message, " ")[0])
		if res != message.Id {
			print(queue, "error!")
		}

		//println(message.id)

	}
	println(queue, "get time", (time.Now().UnixNano()-start)/1000/1000)

	queueInfo = brakes.info(queue)
	println(queue, "messages from", queueInfo.FirstId, "to", queueInfo.LastId)
}

const dataFileExtension = ".data"
const indexFileExtension = ".idx"
const putStateFileExtension = ".pst"

type brakesInfo struct {
	Error string `json:"error"`
}

type incommingHttpBody struct {
	Queue    string   `json:"queue"`
	Messages []string `json:"messages"`
}

type topic struct {
	Name string `json:"name"`
}

type config struct {
	DataPath string  `json:"data_path"`
	TcpPort  int     `json:"tcp_port"`
	Topics   []topic `json:"topics"`
}

func loadConfig() config {

	f_cfg, err := os.OpenFile("config.json", os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		println(err)
	}
	fileSize, _ := f_cfg.Seek(0, io.SeekEnd)
	messageBytesBuff := make([]byte, fileSize)
	f_cfg.ReadAt(messageBytesBuff, 0)

	result := config{DataPath: "", TcpPort: 80}
	json.Unmarshal(messageBytesBuff, &result)
	fmt.Println("data_path", result.DataPath)
	fmt.Println("tcp_port", result.TcpPort)
	fmt.Println(result.Topics)

	return result
}

func Brakes() *brakes {

	brakes := new(brakes)

	brakes.config = loadConfig()

	//brakes.rootDataDir = config.DataPath

	brakes.files = make(map[string]*os.File)
	brakes.queuesContexts = make(map[string]*queueContext)

	go func() {
		for {
			for _, f := range brakes.files {
				f.Sync()
			}
			time.Sleep(time.Duration(1) * time.Second)
		}

	}()

	return brakes
}

func processPOSTMethod(brakes *brakes, w http.ResponseWriter, r *http.Request) {

	body, _ := io.ReadAll(r.Body)
	var incommingMessage incommingHttpBody // = make([]incommingHttpBody, 0)
	json.Unmarshal([]byte(body), &incommingMessage)

	if !brakes.accessGranted(incommingMessage.Queue) {
		return
	}

	for _, message := range incommingMessage.Messages {
		go brakes.send(incommingMessage.Queue, message)
	}
	res := brakesInfo{Error: ""}
	json_data, _ := json.Marshal(res)
	fmt.Fprint(w, string(json_data))
}

type responseHttpMessage struct {
	Id  int64  `json:"id"`
	Msg string `json:"msg"`
}

func (brakes *brakes) accessGranted(queue string) (granted bool) {

	for _, el := range brakes.config.Topics {
		if el.Name == queue {
			granted = true
			break
		}
	}

	return granted
}

func processGETMethod(brakes *brakes, w http.ResponseWriter, r *http.Request) {

	AuthorizationKey := r.Header["Authorization"]
	if AuthorizationKey == nil {
		return
	}
	//println(AuthorizationKey[0])

	queryParams := r.URL.Query()

	queueName := queryParams["queue"][0]

	if !brakes.accessGranted(queueName) {
		return
	}

	id, _ := strconv.ParseInt(queryParams["id"][0], 10, 64)
	limit, _ := strconv.Atoi(queryParams["limit"][0])

	if id == 0 {
		w.Header().Add("Cache-Control", "max-age=1")
	}

	maxSize := 10000000
	if queryParams["max_size"] != nil {
		maxSize, _ = strconv.Atoi(queryParams["max_size"][0])
	}

	var res []responseHttpMessage

	for {
		if limit <= 0 {
			break
		}

		msg := brakes.read(queueName, int(id))
		if msg.Id == -1 {
			break
		}
		var message = responseHttpMessage{Id: int64(msg.Id), Msg: msg.Message}
		res = append(res, message)
		id = int64(msg.Id) + 1
		limit--
		maxSize -= len(message.Msg)
		if maxSize < 0 {
			break
		}

	}

	jsonData, _ := json.Marshal(res)
	fmt.Fprint(w, string(jsonData))
}

func (brakes *brakes) Close() {
	for _, f := range brakes.files {
		f.Close()
	}
}

type chunkContext struct {
	firstMessageId int
	lastMessageId  int
}

type queueContext struct {
	initialized               bool
	activeChunkNo             int
	firstMessageIdFowNewChunk int
	startTime                 int64

	chunksContexts []*chunkContext

	sync.Mutex
}

type brakes struct {
	config config
	//rootDataDir    string
	files          map[string]*os.File
	queuesContexts map[string]*queueContext
	sync.Mutex
}

func (brakes *brakes) openIndexFile(queue string, chunk string) (*os.File, int, int) {

	f_idx := brakes.openFile(queue + chunk + indexFileExtension)

	firstId := -1
	count := 0

	pos, err := f_idx.Seek(0, io.SeekEnd)
	if err != nil {
		println(err)
	}
	if pos > 0 {
		firstId = int(readUint64At(f_idx, 0))
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

type QueueInfo struct {
	FirstId int
	LastId  int
}

func (brakes *brakes) info(queue string) (info QueueInfo) {

	info.FirstId = -1
	info.LastId = -1

	for _, chunk := range chunks(0) {

		_, firstId, count := brakes.openIndexFile(queue, chunkDisplayName(chunk))

		if (info.FirstId == -1 || firstId < info.FirstId) && firstId != -1 {
			info.FirstId = firstId
		}

		lastId := firstId + count - 1
		if lastId > info.LastId {
			info.LastId = lastId
		}
	}

	return info
}

type Message struct {
	Id      int
	Message string
}

func (brakes *brakes) getQueueContext(queue string) *queueContext {
	if brakes.queuesContexts[queue] == nil {
		brakes.queuesContexts[queue] = &queueContext{chunksContexts: make([]*chunkContext, len(chunks(0)))}
		for _, chunk := range chunks(0) {
			brakes.queuesContexts[queue].chunksContexts[chunk] = &chunkContext{}
		}
	}
	return brakes.queuesContexts[queue]
}

func (brakes *brakes) predictChunk(queue string, id int) int {
	result := 0

	for _, chunk := range chunks(0) {

		chunkCtx := brakes.getQueueContext(queue).chunksContexts[chunk]

		if id >= chunkCtx.firstMessageId && id <= chunkCtx.lastMessageId {
			result = chunk
			break
		}
	}
	return result
}

func (brakes *brakes) updatePredictData(queue string, chunk int, firstId int, count int) {

	chunkCtx := brakes.getQueueContext(queue).chunksContexts[chunk]

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

func (brakes *brakes) activeChunk(queue string) (int, int) {

	queueCtx := brakes.getQueueContext(queue)
	if !queueCtx.initialized {

		f_putState := brakes.openFile(queue + putStateFileExtension)
		pos, _ := f_putState.Seek(0, io.SeekEnd)
		if pos == 0 {
			//initialize
			writeUint32(f_putState, uint32(queueCtx.activeChunkNo))
			queueCtx.startTime = time.Now().Unix()
			writeUint64(f_putState, uint64(queueCtx.startTime))

			queueCtx.firstMessageIdFowNewChunk = 1
		} else {
			queueCtx.activeChunkNo = int(readUint32At(f_putState, 0))
			queueCtx.startTime = int64(readUint64At(f_putState, 4))
			queueCtx.firstMessageIdFowNewChunk = int(readUint64At(f_putState, 4+8))
		}

		queueCtx.initialized = true

	}

	return int(queueCtx.activeChunkNo), queueCtx.firstMessageIdFowNewChunk
}

func switchChunk(brakes *brakes, queue string, startingMessageIdForNewChunk int) (newChunk int) {

	queueCtx := brakes.getQueueContext(queue)

	f_putState := brakes.openFile(queue + putStateFileExtension)

	newChunk = int(readUint32At(f_putState, 0) + 1)
	if newChunk >= len(chunks(0)) {
		newChunk = 0
	}

	f_idx := brakes.openFile(queue + chunkDisplayName(newChunk) + indexFileExtension)
	f_idx.Truncate(0)
	f_date := brakes.openFile(queue + chunkDisplayName(newChunk) + dataFileExtension)
	f_date.Truncate(0)

	f_putState.Seek(0, io.SeekStart)
	writeUint32(f_putState, uint32(newChunk))
	writeUint64(f_putState, uint64(time.Now().Unix()))
	writeUint64(f_putState, uint64(startingMessageIdForNewChunk))

	queueCtx.activeChunkNo = newChunk
	queueCtx.firstMessageIdFowNewChunk = startingMessageIdForNewChunk

	return newChunk
}

func (brakes *brakes) openFile(name string) *os.File {

	brakes.Lock()
	res := brakes.files[name]

	if res == nil {
		var err error
		res, err = os.OpenFile(brakes.config.DataPath+name, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			println(err)
		}
		brakes.files[name] = res
	}
	brakes.Unlock()

	return res

}

func messageCountByIndexFileSize(indexFileSize int) int {
	return int(indexFileSize>>3) - 1
}

func (brakes *brakes) send(queue string, message string) {

	/*
		var b bytes.Buffer
		w := zlib.NewWriter(&b)
		w.Write([]byte(message))
		w.Close()
		f_data.Write(b.Bytes())
	*/

	queueCtx := brakes.getQueueContext(queue)
	queueCtx.Lock()

	defer queueCtx.Unlock()

	chunk, startingMessageIdFowNewChunk := brakes.activeChunk(queue)

	f_idx := brakes.openFile(queue + chunkDisplayName(chunk) + indexFileExtension)

	indexFileSize, _ := f_idx.Seek(0, io.SeekEnd)
	if indexFileSize == 0 {
		writeUint64(f_idx, uint64(startingMessageIdFowNewChunk))
	}

	f_data := brakes.openFile(queue + chunkDisplayName(chunk) + dataFileExtension)

	offset_data, _ := f_data.Seek(0, io.SeekEnd)

	writeUint8(f_data, 0) //version
	writeUint32(f_data, uint32(len(message)))
	f_data.WriteString(message)

	writeUint64(f_idx, uint64(offset_data))

	brakes.updatePredictData(queue, chunk,
		int(startingMessageIdFowNewChunk),
		messageCountByIndexFileSize(int(indexFileSize))+1)

	daysElapsed := (time.Now().Unix() - brakes.getQueueContext(queue).startTime) / 60 / 60 / 24
	if offset_data >= 100000000 && daysElapsed > 30 {
		currentStartMessageId := int(readUint64At(f_idx, 0))
		idx_fileLen, _ := f_idx.Seek(0, io.SeekEnd)
		firstMessageId := currentStartMessageId + messageCountByIndexFileSize(int(idx_fileLen))
		newChunk := switchChunk(brakes, queue, firstMessageId)

		brakes.updatePredictData(queue, newChunk, firstMessageId, 0)
	}

}

func (brakes *brakes) read(queue string, id int) (msg Message) {

	if id == 0 {
		id = brakes.info(queue).LastId
	}

	msg.Id = -1
	var beside_f_idx *os.File
	beside_chunk := 0
	beside_idDelta := 9223372036854775807 //max int
	var beside_idxStartId int

	priorityChunk := brakes.predictChunk(queue, id)

	for _, chunk := range chunks(priorityChunk) {

		f_idx, firstId, count := brakes.openIndexFile(queue, chunkDisplayName(chunk))
		if count > 0 {

			brakes.updatePredictData(queue, chunk, firstId, count)

			if id >= firstId && id < (firstId+count) {

				dataFilePos := int64(readUint64At(f_idx, int64(id-firstId+1)<<3))
				f_data := brakes.openFile(queue + chunkDisplayName(chunk) + dataFileExtension)
				msg.Id = int(id)
				msg.Message = readMessage(f_data, dataFilePos)
				return msg
			}
			if id < firstId {
				tempIdDelta := int(firstId - id)
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
		f_data := brakes.openFile(queue + chunkDisplayName(beside_chunk) + dataFileExtension)
		dataFilePos := int64(0)
		msg.Id = beside_idxStartId
		msg.Message = readMessage(f_data, dataFilePos)
	}

	return msg
}
