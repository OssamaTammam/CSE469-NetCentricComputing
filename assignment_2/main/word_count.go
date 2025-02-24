package main

import (
	"fmt"
	"mapreduce"
	"os"
)

func mapFn(docName string, value string) []mapreduce.KeyValue {
	// TODO: write this function -- See the description in the assignment

}

func reduceFn(key string, values []string) string {
	// TODO: write this function -- See the description in the assignment
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run word_count.go master sequential papers)
// 2) Master (e.g., go run word_count.go master localhost_7777 papers &)
// 3) Worker (e.g., go run word_count.go worker localhost_7777 localhost_7778 &) // change 7778 when you run other workers
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcnt_seq", os.Args[3], 3, mapFn, reduceFn)
		} else {
			mr = mapreduce.Distributed("wcnt_dist", os.Args[3], 3, os.Args[2])
		}
		mr.Wait()
	} else if os.Args[1] == "worker" {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapFn, reduceFn, 100, true)
	} else {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	}
}
