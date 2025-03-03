package mapreduce

func runTask(workerArgs RunTaskArgs, workerChan chan string, taskCompleteChan chan bool) {
	workerAddress := <-workerChan
	taskSuccess := call(workerAddress, "Worker.RunTask", &workerArgs, &struct{}{})
	if taskSuccess {
		taskCompleteChan <- true
	}
	workerChan <- workerAddress // Reuse workers
}

func monitorFileCompletion(nTasks int, taskCompleteChan chan bool, tasksDoneChan chan bool) {
	for range taskCompleteChan {
		nTasks--
		if nTasks == 0 {
			tasksDoneChan <- true
			close(taskCompleteChan)
			close(tasksDoneChan)
			break
		}
	}
}

func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var numOtherPhase int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)     // number of map tasks
		numOtherPhase = mr.nReduce // number of reducers
	case reducePhase:
		ntasks = mr.nReduce           // number of reduce tasks
		numOtherPhase = len(mr.files) // number of map tasks
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, numOtherPhase)

	// ToDo: Complete this function. See the description in the assignment.
	taskCompleteChan := make(chan bool) // Channel that receives signal that file is done
	tasksDoneChan := make(chan bool)    // Channel signaling that all work is done

	// Thread that monitors task completions
	go monitorFileCompletion(ntasks, taskCompleteChan, tasksDoneChan)

	// Fire up worker threads
	for i := range ntasks {
		workerArgs := RunTaskArgs{
			JobName:       mr.jobName,
			File:          mr.files[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: numOtherPhase,
		}

		go runTask(workerArgs, mr.registerChannel, taskCompleteChan)
	}

	<-tasksDoneChan
}
