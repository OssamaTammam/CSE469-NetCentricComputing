package asg1

import (
	"math"
	"strings"
	"sync"
)

// Task 1
// This function should output 0^2 + 1^2 + 2^2 + ... + (|n|)^2
func getSumSquares(n int) int {
	// To Do
	var sum int

	for i := 0; i <= int(math.Abs(float64(n))); i++ {
		sum += int(math.Pow(float64(i), 2))
	}

	return sum
}

// Task 2
// This function extracts all words ending with endLetter from the string text
// Hints:
// - You may find the strings.Fields method useful.
// - Read about the difference between the types "rune" and "byte", and see how we test the function.
func getWords(text string, endLetter rune) []string {
	// To Do
	// Split into an array of words
	splitWords := strings.Fields(text)
	var result []string

	// Loop over every word checking if the last letter equals the endLetter
	for _, word := range splitWords {
		wordRunes := []rune(word)
		if wordRunes[len(wordRunes)-1] == endLetter {
			result = append(result, word)
		}
	}

	return result
}

type RegRecord struct {
	studentId  int
	courseName string
}

// Task 3
// This function receives a list of student registration records and should return
// a map that shows the number of students registered per course.
// Note that duplicates in records may appear in the input list, and they should not be included in the count.
func getCourseInfo(records []RegRecord) map[string]int {
	// To Do
	// Hashmap to hold results
	coursesFreq := make(map[string]int)
	studentSet := make(map[int]struct{})

	// Iterate over records if it exists skip unless +1 no of student per that course
	for _, record := range records {
		_, exists := studentSet[record.studentId]
		if exists {
			continue
		}

		// Add student to the set
		studentSet[record.studentId] = struct{}{}
		coursesFreq[record.courseName] += 1
	}

	return coursesFreq
}

// Task 4
// This function is required to count the occurrences of an input key in a list of integers.
// This should be done in parallel. Each invoked go routine should run the countWorker function on part of the list.
// The communication between the main thread and the workers should be done via channels. You can create more than one input channel.
// You can use any way to divide the input list across your workers. Try to distribute the work evenly among the worker routines.
// numThreads will not exceed the length of the array

// Note: The way we are asking you to implement this task is obviously not the most efficient.
// However, the purpose here is to make you more familiar with channels and thread communication, not the performance itself.

func count(list []int, key int, numThreads int) int {
	// To Do
	inChannels := make([]chan int, numThreads)
	outChannel := make(chan int, numThreads)
	var waitGroup sync.WaitGroup

	// Fire up the thread workers to count the occurrences
	for i := 0; i < numThreads; i++ {
		inChannels[i] = make(chan int)
		waitGroup.Add(1)
		go func(inChannel chan int) {
			countWorker(key, inChannel, outChannel)
			waitGroup.Done()
		}(inChannels[i])
	}

	// Feed the input channel
	noElementsPerChan := len(list) / numThreads
	lastEnd := 0
	for i := 0; i < numThreads; i++ {
		start := lastEnd
		end := start + noElementsPerChan

		// Out of bounds check
		if i == numThreads-1 {
			end = len(list)
		}

		listSlice := list[start:end]
		lastEnd = end

		// Fire go routines to feed the channels
		go func(inChannel chan int, values []int) {
			for _, number := range values {
				inChannel <- number
			}
			close(inChannel)
		}(inChannels[i], listSlice)
	}

	// Wait until in threads are done then close out channel
	go func() {
		waitGroup.Wait()
		close(outChannel)
	}()

	// Read from out channel till it closes
	var result int
	for freq := range outChannel {
		result += freq
	}

	return result
}

// This worker function receives inputs via inputChan, and outputs the number of occurrences to outChan
// Note: The worker does not have any information about the number of inputs it will process, i.e.,
// the function should keep working as long as inputChan is open.
func countWorker(key int, inputChan chan int, outChan chan int) {
	// To Do
	var freq int

	// As long as channel's buffer has elements keep reading
	for number := range inputChan {
		if number == key {
			freq += 1
		}
	}

	// Write to out channel once main threads closes channel
	outChan <- freq
}
