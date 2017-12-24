package mapreduce

import "fmt"
import "github.com/golang-collections/collections/stack"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var tasks stack.Stack
	for i := 0; i < ntasks; i++ {
		tasks.Push(i)
	}
	var chans [] chan int
	for {
		mr.Lock()
		if tasks.Len() == 0 {
			for _, iChan := range chans {
				<- iChan
			}
			if tasks.Len() == 0 {
				mr.Unlock()
				break
			}
		}
		mr.Unlock()
		worker := <- mr.registerChannel
		go func() {
			iWorker := worker
			mr.Lock()
			taskNumber := tasks.Pop()
			mr.Unlock()

			iChan := make(chan int)
			mr.Lock()
			chans = append(chans, iChan)
			mr.Unlock()

			var doTaskArgs DoTaskArgs
			doTaskArgs.Phase = phase
			doTaskArgs.JobName = mr.jobName
			if taskNumber == nil {
				go func() {
					mr.registerChannel <- iWorker
				}()
				return
			}
			doTaskArgs.TaskNumber = taskNumber.(int)
			if phase == mapPhase {
				doTaskArgs.File = mr.files[taskNumber.(int)]
			}
			doTaskArgs.NumOtherPhase = nios

			ok := call(iWorker, "Worker.DoTask", doTaskArgs, new(struct{}))
			if !ok {
				mr.Lock()
				tasks.Push(taskNumber)
				mr.Unlock()
				fmt.Println("call worker ", iWorker, " error")
				iChan <- taskNumber.(int)
				return
			}
			go func() {
				mr.registerChannel <- iWorker
			}()
			iChan <- taskNumber.(int)
		}()
	}
}
