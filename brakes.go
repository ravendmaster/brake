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

	ctx := Open()
	defer Close(ctx)

	go doFillTest(ctx, "test")
	go doFillTest(ctx, "alpha")
	go doFillTest(ctx, "betta")
	go doFillTest(ctx, "gamma")

	/*


		println("first:", Info(state, "test").FirstId)
		println("last:", Info(state, "test").LastId)
	*/

	/*
		start = time.Now().UnixNano()
		for i := 0; i < 1700; i++ {
			get("test", 100)
		}
		println("long", (time.Now().UnixNano()-start)/1000/1000)
	*/

	//println(get(state, "test", 19181).message)
	//println(get("test", -1).message)

	//go massGetInfo("alpha")

	//massFill("base")
	//go massFill("alpha")
	//go massFill("betta")
	//go massFill("gamma")
	//go massFill("tetta")

	//put("new", "hello 2")

	//queue_info := info("base")
	//println(queue_info.first_id, queue_info.last_id)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		switch r.Method {
		case "POST":
			processPOSTMethod(ctx, w, r)
		case "GET":
			processGETMethod(ctx, w, r)
		}
	})

	http.ListenAndServe(":80", nil)

}

func doFillTest(ctx *brakesContext, queue string) {

	//Put(ctx, "test", "hi")
	//Get(ctx, "test", 9872938293874)

	message := ""
	for i := 0; i < 512; i++ {
		message += "1"
	}

	queueInfo := Info(ctx, queue)

	id := queueInfo.LastId + 1
	if queueInfo.LastId == -1 {
		id = 1
	}

	idForRead := id

	start := time.Now().UnixNano()
	for i := 1; i <= 5000000; i++ {
		Put(ctx, queue, strconv.Itoa(id)+"             "+message)
		id++
	}
	println("put time", (time.Now().UnixNano()-start)/1000/1000)

	start = time.Now().UnixNano()

	for i := 0; i < 50000; i++ {
		message := Get(ctx, queue, idForRead)
		if message.Id == -1 {
			println("no more", idForRead)
			break
		}

		idForRead = message.Id + 1

		res, _ := strconv.Atoi(strings.Split(message.Message, " ")[0])
		if res != message.Id {
			//	panic("error!")
		}

		//println(message.id)

	}
	println("get time", (time.Now().UnixNano()-start)/1000/1000)

	queueInfo = Info(ctx, queue)
	println("messages from", queueInfo.FirstId, "to", queueInfo.LastId)
}

const dataFileExtension = ".data"
const indexFileExtension = ".idx"
const putStateFileExtension = ".pst"

type brakesInfo struct {
	LastId uint64
}

type incommingHttpMessage struct {
	Queue string `json:"queue"`
	Msg   string `json:"msg"`
}

func processPOSTMethod(ctx *brakesContext, w http.ResponseWriter, r *http.Request) {

	body, _ := io.ReadAll(r.Body)

	var incommingMessage []incommingHttpMessage = make([]incommingHttpMessage, 0)

	json.Unmarshal([]byte(body), &incommingMessage)

	for _, item := range incommingMessage {
		go Put(ctx, item.Queue, item.Msg)
	}

	res := brakesInfo{LastId: 1}

	json_data, _ := json.Marshal(res)
	fmt.Fprint(w, string(json_data))
}

type responseHttpMessage struct {
	Id  int64  `json:"id"`
	Msg string `json:"msg"`
}

func processGETMethod(ctx *brakesContext, w http.ResponseWriter, r *http.Request) {

	values := r.URL.Query()

	queueName := values["queue"][0]
	id, _ := strconv.ParseInt(values["id"][0], 10, 64)
	limit, _ := strconv.Atoi(values["limit"][0])

	var res []responseHttpMessage

	for {
		msg := Get(ctx, queueName, int(id))
		if msg.Id == -1 {
			break
		}

		var message = responseHttpMessage{Id: int64(msg.Id), Msg: msg.Message}

		res = append(res, message)

		id = int64(msg.Id) + 1
		limit--
		if limit <= 0 {
			break
		}

	}

	jsonData, _ := json.Marshal(res)
	fmt.Fprint(w, string(jsonData))
}

func Open() *brakesContext {
	ctx := new(brakesContext)

	ctx.rootDataDir = `d:\temp\`

	ctx.files = make(map[string]*os.File)
	ctx.queuesContexts = make(map[string]*queueContext)

	go func() {
		for {
			for _, f := range ctx.files {
				f.Sync()
			}
			time.Sleep(time.Duration(1) * time.Second)
		}

	}()

	return ctx
}

func Close(ctx *brakesContext) {
	for _, f := range ctx.files {
		f.Close()
	}
}

type partContext struct {
	firstMessageId int
	lastMessageId  int
}

type queueContext struct {
	initialized              bool
	activePartNo             int
	firstMessageIdFowNewPart int
	startTime                int64

	partsContexts []*partContext

	sync.Mutex
}

type brakesContext struct {
	rootDataDir    string
	files          map[string]*os.File
	queuesContexts map[string]*queueContext
}

func openIndexFile(ctx *brakesContext, queue string, storagePart string) (*os.File, int, int) {

	f_idx := openFile(ctx, queue+storagePart+indexFileExtension)

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

func storageParts(priorityPart int) (result []int) {

	switch priorityPart {
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

func suffixOfPart(part int) string {
	switch part {
	case 0:
		return "_0"
	case 1:
		return "_1"
	case 2:
		return "_2"
	case 3:
		return "_3"
	}
	panic("suffixOfPart() overflow")
}

type QueueInfo struct {
	FirstId int
	LastId  int
}

func Info(ctx *brakesContext, queue string) (info QueueInfo) {

	info.FirstId = -1
	info.LastId = -1

	for _, storagePart := range storageParts(0) {

		_, firstId, count := openIndexFile(ctx, queue, suffixOfPart(storagePart))

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

func getQueueContext(ctx *brakesContext, queue string) *queueContext {
	if ctx.queuesContexts[queue] == nil {
		ctx.queuesContexts[queue] = &queueContext{partsContexts: make([]*partContext, len(storageParts(0)))}
		for _, storagePart := range storageParts(0) {
			ctx.queuesContexts[queue].partsContexts[storagePart] = &partContext{}
		}
	}
	return ctx.queuesContexts[queue]
}

func predictPart(ctx *brakesContext, queue string, id int) int {
	result := 0
	for _, storagePart := range storageParts(0) {

		partCtx := getQueueContext(ctx, queue).partsContexts[storagePart]

		if id >= partCtx.firstMessageId && id <= partCtx.lastMessageId {
			result = storagePart
			break
		}
	}
	return result
}

func updatePredictData(ctx *brakesContext, queue string, storagePart int, firstId int, count int) {

	partCtx := getQueueContext(ctx, queue).partsContexts[storagePart]

	if (partCtx.firstMessageId != firstId) || (partCtx.lastMessageId != firstId+count-1) {
		partCtx.firstMessageId = firstId
		partCtx.lastMessageId = firstId + count - 1
	}
}

func Get(ctx *brakesContext, queue string, id int) (msg Message) {

	if id == 0 {
		id = Info(ctx, queue).LastId
	}

	msg.Id = -1
	var beside_f_idx *os.File
	beside_storagePart := ""
	beside_idDelta := 9223372036854775807 //max int
	var beside_idxStartId int

	priorityPart := predictPart(ctx, queue, id)

	for _, storagePart := range storageParts(priorityPart) {

		f_idx, firstId, count := openIndexFile(ctx, queue, suffixOfPart(storagePart))
		if count > 0 {

			updatePredictData(ctx, queue, storagePart, firstId, count)

			if id >= firstId && id < (firstId+count) {

				dataFilePos := readUint64At(f_idx, int64(id-firstId+1)<<3)
				f_data := openFile(ctx, queue+suffixOfPart(storagePart)+dataFileExtension)
				msg.Id = int(id)
				msg.Message = readMessage(f_data, dataFilePos)
				return msg
			}
			if id < firstId {
				tempIdDelta := int(firstId - id)
				if tempIdDelta > 0 && (tempIdDelta < beside_idDelta) {
					beside_idDelta = tempIdDelta
					beside_f_idx = f_idx
					beside_storagePart = suffixOfPart(storagePart)
					beside_idxStartId = firstId
				}
			}
		}

	}

	if beside_f_idx != nil {
		f_data := openFile(ctx, queue+beside_storagePart+dataFileExtension)
		dataFilePos := uint64(0)
		msg.Id = beside_idxStartId
		msg.Message = readMessage(f_data, dataFilePos)
	}

	return msg
}

func readMessage(f_data *os.File, dataFilePos uint64) (message string) {

	messageSize := readUint32At(f_data, int64(dataFilePos))
	messageBytesBuff := make([]byte, messageSize)
	f_data.ReadAt(messageBytesBuff, int64(dataFilePos+4))
	return string(messageBytesBuff)
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

func getActivePutPart(ctx *brakesContext, queue string) (int, int) {

	queueCtx := getQueueContext(ctx, queue)
	if !queueCtx.initialized {

		f_putState := openFile(ctx, queue+putStateFileExtension)
		pos, _ := f_putState.Seek(0, io.SeekEnd)
		if pos == 0 {
			//initialize
			writeUint32(f_putState, uint32(queueCtx.activePartNo))
			queueCtx.startTime = time.Now().Unix()
			writeUint64(f_putState, uint64(queueCtx.startTime))

			queueCtx.firstMessageIdFowNewPart = 1
		} else {
			queueCtx.activePartNo = int(readUint32At(f_putState, 0))
			queueCtx.startTime = int64(readUint64At(f_putState, 4))
			queueCtx.firstMessageIdFowNewPart = int(readUint64At(f_putState, 4+8))
		}

		queueCtx.initialized = true

	}

	return int(queueCtx.activePartNo), queueCtx.firstMessageIdFowNewPart
}

func switchPart(ctx *brakesContext, queue string, messageIdForNewPart int) (newPartNo int) {

	queueCtx := getQueueContext(ctx, queue)

	f_putState := openFile(ctx, queue+putStateFileExtension)

	newPartNo = int(readUint32At(f_putState, 0) + 1)
	if newPartNo >= len(storageParts(0)) {
		newPartNo = 0
	}

	f_idx := openFile(ctx, queue+suffixOfPart(newPartNo)+indexFileExtension)
	f_idx.Truncate(0)
	f_date := openFile(ctx, queue+suffixOfPart(newPartNo)+dataFileExtension)
	f_date.Truncate(0)

	f_putState.Seek(0, io.SeekStart)
	writeUint32(f_putState, uint32(newPartNo))
	writeUint64(f_putState, uint64(time.Now().Unix()))
	writeUint64(f_putState, uint64(messageIdForNewPart))

	queueCtx.activePartNo = newPartNo
	queueCtx.firstMessageIdFowNewPart = messageIdForNewPart

	return newPartNo
}

func openFile(ctx *brakesContext, name string) *os.File {

	res := ctx.files[name]

	if res == nil {
		var err error
		res, err = os.OpenFile(ctx.rootDataDir+name, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			println(err)
		}
		ctx.files[name] = res
	}

	return res

}

func messageCountByIndexFileSize(indexFileSize int) int {
	return int(indexFileSize>>3) - 1
}

func Put(ctx *brakesContext, queue string, message string) {

	/*
		var b bytes.Buffer
		w := zlib.NewWriter(&b)
		w.Write([]byte(message))
		w.Close()
		f_data.Write(b.Bytes())
	*/

	queueCtx := getQueueContext(ctx, queue)
	queueCtx.Lock()

	defer queueCtx.Unlock()

	storagePart, firstMessageIdFowNewPart := getActivePutPart(ctx, queue)

	f_idx := openFile(ctx, queue+suffixOfPart(storagePart)+indexFileExtension)

	indexFileSize, _ := f_idx.Seek(0, io.SeekEnd)
	if indexFileSize == 0 {
		writeUint64(f_idx, uint64(firstMessageIdFowNewPart))
	}

	f_data := openFile(ctx, queue+suffixOfPart(storagePart)+dataFileExtension)

	offset_data, _ := f_data.Seek(0, io.SeekEnd)

	writeUint32(f_data, uint32(len(message)))
	f_data.WriteString(message)

	writeUint64(f_idx, uint64(offset_data))

	updatePredictData(ctx, queue, storagePart,
		int(firstMessageIdFowNewPart),
		messageCountByIndexFileSize(int(indexFileSize))+1)

	daysElapsed := (time.Now().Unix() - getQueueContext(ctx, queue).startTime) / 60 / 60 / 24
	if offset_data >= 100000000 && daysElapsed > 30 {
		currentStartMessageId := int(readUint64At(f_idx, 0))
		idx_fileLen, _ := f_idx.Seek(0, io.SeekEnd)
		firstMessageId := currentStartMessageId + messageCountByIndexFileSize(int(idx_fileLen))
		newPartNo := switchPart(ctx, queue, firstMessageId)

		updatePredictData(ctx, queue, newPartNo, firstMessageId, 0)
	}

}
