package mapreduce

import (
	"bufio"
	"encoding/json"
	"hash/fnv"
	"io/fs"
	"os"
	"sort"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input file that was assigned to this task
	nReduce int, // The number of reduce tasks
	mapFn func(file string, contents string) []KeyValue, // The user-defined map function
) {
	// ToDo: Write this function. See the description in the assignment.
	// Check if fileInput is a valid file path
	if !fs.ValidPath(inputFile) {
		panic("Invalid file path: " + inputFile)
	}

	// Open the file
	file, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer file.Close() // Close after function is done executing

	// Read and store content into variable fileContent
	lineScanner := bufio.NewScanner(file) // Reading line by lie for memory efficiency in case file is too big
	var fileContent string
	for lineScanner.Scan() {
		fileContent += lineScanner.Text() + " "
	}

	if err := lineScanner.Err(); err != nil {
		panic(err)
	}

	// Call mapFn using filePath and content
	outputPairs := mapFn(inputFile, fileContent)

	// Generate files and their perspective encoders
	files := make([]*os.File, nReduce) // To close files at the end
	fileEncoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		// Create file with write permissions
		fileName := getIntermediateName(jobName, mapTaskIndex, i)
		file, err := os.Create(fileName)
		if err != nil {
			panic("Error creating file: " + fileName)
		}
		files[i] = file

		// Create JSON encoder for the file and store it the map
		encoder := json.NewEncoder(file)
		fileEncoders[i] = encoder
	}

	// Loop over the outputMap
	for _, keyValuePair := range outputPairs {
		// Get intermediate file name for the key-value pair
		fileHash := hash32(keyValuePair.Key)
		fileIndex := fileHash % uint32(nReduce)

		// Write output to file
		fileEncoder := fileEncoders[int(fileIndex)]
		err := fileEncoder.Encode(&keyValuePair)
		if err != nil {
			panic("Error encoding key-value pair" + err.Error())
		}
	}

	// Close all files
	for _, file := range files {
		file.Close()
	}
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
	// Map of freq
	freqMap := make(map[string][]string)

	// Read from files, decode and store in freqMap
	for i := range nMap {
		// Create file decoder
		fileName := getIntermediateName(jobName, i, reduceTaskIndex)
		file, err := os.Open(fileName)
		if err != nil {
			panic("Error opening file " + err.Error())
		}
		decoder := json.NewDecoder(file)

		for {
			// Decode each pair and store them in the freqMap
			var keyValuePair KeyValue
			if err := decoder.Decode(&keyValuePair); err != nil {
				if err.Error() == "EOF" {
					break
				}
				panic("Error decoding key-value pair: " + err.Error())
			}
			freqMap[keyValuePair.Key] = append(freqMap[keyValuePair.Key], keyValuePair.Value)
		}

		file.Close()
	}

	// Run reduce on the freqMap
	reduceOutput := []KeyValue{}
	for key, values := range freqMap {
		reducedString := reduceFn(key, values)
		keyValuePair := KeyValue{key, reducedString}
		reduceOutput = append(reduceOutput, keyValuePair)
	}

	// Sort the output based on keys
	sort.Slice(reduceOutput, func(i, j int) bool {
		return reduceOutput[i].Key <= reduceOutput[j].Key
	})

	// Create out file
	outFileName := getReduceOutName(jobName, reduceTaskIndex)
	file, err := os.Create(outFileName)
	if err != nil {
		panic("Error creating file: " + outFileName)
	}
	defer file.Close()
	outWriter := bufio.NewWriter(file)
	encoder := json.NewEncoder(outWriter)

	// Write to out file
	for _, value := range reduceOutput {
		err := encoder.Encode(&value)
		if err != nil {
			panic("Error encoding key-value pair" + err.Error())
		}
	}

	outWriter.Flush() // Ensure all data is written (Might be redundant cause we are using encoder but better safe than sorry if the data is sent to writer and get it accumulated as chunks)
}
