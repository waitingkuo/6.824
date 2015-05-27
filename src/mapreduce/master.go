package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	//failedMapJob := []int{}

	for i := 0; i < nMap; i++ {
		worker := <-mr.registerChannel
		go func(jobNumber int) {
			args := &DoJobArgs{
				File:          mr.file,
				Operation:     "Map",
				JobNumber:     jobNumber,
				NumOtherPhase: mr.nReduce,
			}
			var reply DoJobReply
			call(worker, "Worker.DoJob", args, &reply)
			for !reply.OK {
				fmt.Println("do again")
				worker = <-mr.registerChannel
				call(worker, "Worker.DoJob", args, &reply)
			}
			mr.registerChannel <- worker
		}(i)
	}

	/*
			for {
		    failedMapJob
				worker := <-mr.registerChannel
				go func(jobNumber int) {
					args := &DoJobArgs{
						File:          mr.file,
						Operation:     "Map",
						JobNumber:     jobNumber,
						NumOtherPhase: mr.nReduce,
					}
					var reply DoJobReply
					call(worker, "Worker.DoJob", args, &reply)
					if reply.OK {
						mr.registerChannel <- worker
					} else {
						failedMapJob = append(failedMapJob, jobNumber)
					}
				}(i)
			}
	*/

	for i := 0; i < nReduce; i++ {
		worker := <-mr.registerChannel
		go func(jobNumber int) {
			args := &DoJobArgs{
				File:          mr.file,
				Operation:     "Reduce",
				JobNumber:     jobNumber,
				NumOtherPhase: mr.nMap,
			}
			var reply DoJobReply
			call(worker, "Worker.DoJob", args, &reply)
			for !reply.OK {
				worker = <-mr.registerChannel
				call(worker, "Worker.DoJob", args, &reply)
			}
			mr.registerChannel <- worker
		}(i)
	}

	return mr.KillWorkers()
}
