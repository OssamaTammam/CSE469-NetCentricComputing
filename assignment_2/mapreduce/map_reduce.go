package mapreduce

import (
	"hash/fnv"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input file that was assigned to this task
	nReduce int, // The number of reduce tasks
	mapFn func(file string, contents string) []KeyValue, // The user-defined map function
) {
	// ToDo: Write this function. See the description in the assignment.
}

func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func runReduceTask(
	jobName string, // the name of the whole MapReduce job
	reduceTaskIndex int, // the index of the reduce task
	nMap int, // the number of map tasks
	reduceFn func(key string, values []string) string,
) {

	// ToDo: Write this function. See the description in the assignment.
}
