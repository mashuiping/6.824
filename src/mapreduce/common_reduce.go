package mapreduce

import (
	"sort"
	"encoding/json"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	var KeyValueList []KeyValue
	var tempKeyValue KeyValue
	for m := 0; m < nMap; m++ {
		rName := reduceName(jobName, m, reduceTaskNumber)
		itFile, err := os.Open(rName)
		defer itFile.Close()
		checkError(err)
		dec := json.NewDecoder(itFile)
		for {
			err := dec.Decode(&tempKeyValue)
			if err != nil {
				break
			} else {
				// 可以在这里采用堆排序(先开辟空间), 这样不用在下面再次使用sort
				KeyValueList = append(KeyValueList, tempKeyValue)
			}
		}
	}
	// sort: https://golang.org/pkg/sort/
	sort.Slice(KeyValueList, func(i, j int) bool {
		return KeyValueList[i].Key < KeyValueList[j].Key
	})
	mName := mergeName(jobName, reduceTaskNumber)
	mergeFile, err := os.Create(mName)
	defer mergeFile.Close()
	checkError(err)
	enc := json.NewEncoder(mergeFile)

	keys := make(map[string]bool)
	kvs := make(map[string][]string)
	for _, kv := range KeyValueList {
		keys[kv.Key] = true
		kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
	}
	for ks := range keys {
		err := enc.Encode(KeyValue{ks, reduceF(ks, kvs[ks])})
		checkError(err)
	}
}
