package asg1

import (
	"math"
	"strings"
)

// Task 1
// This function should output 0^2 + 1^2 + 2^2 + ... + (|n|)^2
func getSumSquares(n int) int {
	// To Do
	var sum int

	for i:=0; i <= int(math.Abs(float64(n))); i++ {
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
}

// This worker function receives inputs via inputChan, and outputs the number of occurrences to outChan
// Note: The worker does not have any information about the number of inputs it will process, i.e.,
// the function should keep working as long as inputChan is open.
func countWorker(key int, inputChan chan int, outChan chan int) {
	// To Do
}
