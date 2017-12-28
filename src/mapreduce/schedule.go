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
	// 同步全部任务
	var chans [] chan int
	for {
		mr.Lock()
		if tasks.Len() == 0 {
			// 等待全部任务结束
			for _, iChan := range chans {
				<- iChan
			}
			// 如果任务全部完成, schedule 返回
			if tasks.Len() == 0 {
				mr.Unlock()
				return
			}
		}
		mr.Unlock()
		// 拿worker
		worker := <- mr.registerChannel
		go func() {
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
			// pop 到最后可能出来nil值
			if taskNumber == nil {
				// worker没有执行错误,记得把worker写回
				go func() {
					mr.registerChannel <- worker
				}()
				return
			}
			doTaskArgs.TaskNumber = taskNumber.(int)
			if phase == mapPhase {
				doTaskArgs.File = mr.files[taskNumber.(int)]
			}
			doTaskArgs.NumOtherPhase = nios

			ok := call(worker, "Worker.DoTask", doTaskArgs, new(struct{}))
			if !ok {
				mr.Lock()
				tasks.Push(taskNumber)
				mr.Unlock()
				fmt.Println("call worker ", worker, " error")
				// 方便调试
				iChan <- taskNumber.(int)
				return
			}
			go func() {
				mr.registerChannel <- worker
			}()
			iChan <- taskNumber.(int)
		}()
	}
}
