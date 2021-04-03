package brakes

import (
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"time"
)

func tests() {

	state := Open()

	message := ""
	for i := 0; i < 512; i++ {
		message += "1"
	}
	start := time.Now().UnixNano()
	for i := 1; i <= 32000; i++ {
		//put(state, "test", strconv.Itoa(i)+" "+message)
	}
	println("put time", (time.Now().UnixNano()-start)/1000/1000)

	start = time.Now().UnixNano()
	id := 1
	for i := 0; i < 32000; i++ {
		message := Get(state, "test", id)
		if message.Id == -1 {
			println("no more")
			break
		}
		id = message.Id + 1

		//println(message.id)

	}
	println("get time", (time.Now().UnixNano()-start)/1000/1000)

	println("first:", Info(state, "test").FirstId)
	println("last:", Info(state, "test").LastId)

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

	/*
		id := 0
		for i := 0; i < 2000; i++ {

			println("try get id", id)
			msg := get("base", id)
			if msg.id == -1 {
				break
			}
			id = msg.id + 1

			println("return id", msg.id)
			println("message: ", msg.message[:10])

		}
	*/

	//reader := bufio.NewReader(os.Stdin)
	//reader.ReadString('\n')

	Close(state)
}

func Open() *State {
	state := new(State)
	state.files = make(map[string]*os.File)

	go func() {
		for {
			for _, f := range state.files {
				f.Sync()
			}
			time.Sleep(time.Duration(1) * time.Second)
		}

	}()

	return state
}

func Close(state *State) {
	for _, f := range state.files {
		f.Close()
	}
}

type State struct {
	files map[string]*os.File
}

func openIndexFileForRead(state *State, queue string, tic_tac_suffix string) (*os.File, int, int) {

	f_idx := getFile(state, queue+tic_tac_suffix+".idx")

	first_id := -1
	count := 0

	pos, err := f_idx.Seek(0, io.SeekEnd)
	if err != nil {
		println(err)
	}
	if pos > 0 {
		first_id = int(readUint64At(f_idx, 0))
		count = int(pos>>3 - 1)
	}

	return f_idx, first_id, count
}

func ticTacList() (result []string) {

	return []string{"_0", "_1", "_2", "_3", "_4", "_5", "_6", "_7"}
}

type QueueInfo struct {
	FirstId int
	LastId  int
}

func Info(state *State, queue string) (info QueueInfo) {

	info.FirstId = -1
	info.LastId = -1

	for _, tic_tac_part := range ticTacList() {

		_, first_id, count := openIndexFileForRead(state, queue, tic_tac_part)

		if (info.FirstId == -1 || first_id < info.FirstId) && first_id != -1 {
			info.FirstId = first_id
		}

		last_id := first_id + count - 1
		if last_id > info.LastId {
			info.LastId = last_id
		}
	}

	return info
}

type Message struct {
	Id      int
	Message string
}

func Get(state *State, queue string, message_id int) (msg Message) {

	if message_id == 0 {
		message_id = Info(state, queue).LastId
	}

	msg.Id = -1
	var beside_f_idx *os.File
	beside_tic_tac_preffix := ""
	beside_id_delta := 9223372036854775807 //max int
	var beside_idx_start_id int

	for _, tic_tac_preffix := range ticTacList() {

		f_idx, first_id, count := openIndexFileForRead(state, queue, tic_tac_preffix)

		if f_idx != nil && count > 0 {

			if message_id >= first_id && message_id < (first_id+count) {

				data_file_pos := readUint64At(f_idx, int64(message_id-first_id+1)<<3)
				f_data := getFile(state, queue+tic_tac_preffix+".data")

				message_size := readUint32At(f_data, int64(data_file_pos))
				message_raw := make([]byte, message_size)
				f_data.ReadAt(message_raw, int64(data_file_pos+4))
				msg.Id = int(message_id)
				msg.Message = string(message_raw)
				return msg
			}

			if message_id < first_id {
				temp_id_delta := int(first_id - message_id)

				//println("temp delta to beside ", tic_tac_preffix, temp_id_delta)
				if temp_id_delta > 0 && (temp_id_delta < beside_id_delta) {
					beside_id_delta = temp_id_delta
					beside_f_idx = f_idx
					beside_tic_tac_preffix = tic_tac_preffix
					beside_idx_start_id = first_id
				}

			}

		}

	}

	if beside_f_idx != nil {
		//println("winer: ", beside_tic_tac_preffix, beside_id_delta, beside_f_idx)

		f_data := getFile(state, queue+beside_tic_tac_preffix+".data")

		data_file_pos := 0
		message_size := readUint32At(f_data, int64(data_file_pos))
		message_raw := make([]byte, message_size)
		f_data.ReadAt(message_raw, int64(data_file_pos+4))
		msg.Id = beside_idx_start_id
		msg.Message = string(message_raw)
	}

	return msg
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

func getPartForQueue(state *State, queue string) (string, uint64) {

	message_id_for_new_part := uint64(1)
	result := uint32(0)

	f_put_state := getFile(state, queue+".pst")

	pos, _ := f_put_state.Seek(0, io.SeekEnd)
	if pos == 0 {
		//initialize
		writeUint32(f_put_state, result)
	} else {
		result = readUint32At(f_put_state, 0)
		message_id_for_new_part = readUint64At(f_put_state, 4)
	}

	return strconv.Itoa(int(result)), message_id_for_new_part
}

func switchPartForQueue(state *State, queue string, message_id_for_new_part uint64) {

	f_put_state := getFile(state, queue+".pst")

	new_part := readUint32At(f_put_state, 0) + 1
	if new_part >= 8 {
		new_part = 0
	}

	f_idx := getFile(state, queue+"_"+strconv.Itoa(int(new_part))+".idx")
	f_idx.Truncate(0)
	f_date := getFile(state, queue+"_"+strconv.Itoa(int(new_part))+".data")
	f_date.Truncate(0)

	f_put_state.Seek(0, io.SeekStart)
	writeUint32(f_put_state, uint32(new_part))
	writeUint64(f_put_state, message_id_for_new_part)
}

func getFile(state *State, name string) *os.File {

	res := state.files[name]

	if res == nil {
		var err error
		res, err = os.OpenFile(`d:\temp\`+name, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			println(err)
		}
		state.files[name] = res
	}

	return res

}

func Put(state *State, queue string, message string) {

	part, message_id_for_new_part := getPartForQueue(state, queue)

	f_idx := getFile(state, queue+"_"+part+".idx")

	pos, _ := f_idx.Seek(0, io.SeekEnd)
	if pos == 0 {
		writeUint64(f_idx, message_id_for_new_part)
	}

	f_data := getFile(state, queue+"_"+part+".data")

	offset_data, _ := f_data.Seek(0, io.SeekEnd)

	/*
		buf := new(bytes.Buffer)
		gob.NewEncoder(buf).Encode(message)

		file_buf := new(bytes.Buffer)
		file_buf.Grow(4 + buf.Len())
		binary.Write(file_buf, binary.LittleEndian, uint32(len(message)))

		binary.Write(file_buf, binary.LittleEndian, buf.Bytes())
		f_data.Write(file_buf.Bytes())
	*/
	writeUint32(f_data, uint32(len(message)))
	f_data.WriteString(message)

	writeUint64(f_idx, uint64(offset_data))

	if offset_data >= 1000000000 {
		current_start_message_id := readUint64At(f_idx, 0)
		pos, _ := f_idx.Seek(0, io.SeekEnd)
		switchPartForQueue(state, queue, current_start_message_id+uint64(pos)>>3-1)
	}

}
