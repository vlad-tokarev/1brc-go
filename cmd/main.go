package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
)

const keySize = 50

type Agg struct {
	sum   float64
	count int
	min   float64
	max   float64
}

func main() {

	// Create and open a file to write the CPU profile to
	cpuProfile, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal("Could not create CPU profile: ", err)
	}
	defer cpuProfile.Close()

	// Start the CPU profiling
	if err := pprof.StartCPUProfile(cpuProfile); err != nil {
		log.Fatal("Could not start CPU profile: ", err)
	}

	// Ensure the CPU profile is stopped when the function returns
	defer pprof.StopCPUProfile()

	t0 := time.Now()
	run()
	fmt.Printf("took %s\n", time.Now().Sub(t0))
}

func run() {

	data := readData()

	workers := runtime.GOMAXPROCS(0)

	results := mapScan(data, scan, workers)

	mergedResults := reduce(results...)

	writeResultsToFile(mergedResults)

}

// scan reads chunk of data without extra allocations
func scan(data []byte, i int, end int) map[string]Agg {
	m := make(map[[keySize]byte]Agg, 0)
	var (
		key           [keySize]byte
		keyPos        int
		keyPrevLength int // keyPrevLength used to clean (set 0x0) for bytes that are garbage for new key

		value      float64
		valueStart int

		agg Agg
		ok  bool
	)

	// skip not full part
	if i != 0 {
		for data[i] != '\n' {
			i++
		}
		i++
	}

	for i < end {
		// parse key
		for data[i] != ';' {
			key[keyPos] = data[i]
			i++
			keyPos++
		}
		i++

		// clean rest of key
		for j := keyPos; j < keyPrevLength; j++ {
			key[j] = 0x0
		}

		keyPrevLength = keyPos
		keyPos = 0

		// parse value
		valueStart = i
		for data[i] != '\n' {
			i++
		}
		value = fastFloat(data[valueStart:i])
		i++

		// update value
		agg, ok = m[key]
		if ok {
			agg.min = min(agg.min, value)
			agg.max = max(agg.max, value)
			agg.sum = agg.sum + value
			agg.count++
		} else {
			agg.min = value
			agg.max = value
			agg.count++
			agg.sum = value
		}
		m[key] = agg
	}

	return fixMap(m)
}

// mapScan splits data to chunks and run scanning in goroutines
func mapScan(
	data []byte,
	scanFunc func(data []byte, i int, end int) map[string]Agg,
	workers int,
) []map[string]Agg {

	n := len(data)
	fmt.Printf("%d CPUs\n", workers)
	shift := n / workers

	results := make([]map[string]Agg, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			from := i * shift
			to := i*shift + shift
			if i == workers-1 {
				to = n
			}
			res := scanFunc(data, from, to)
			results[i] = res
		}()
	}
	wg.Wait()

	return results
}

// reduce merges chunks results together
func reduce(data ...map[string]Agg) map[string]Agg {
	out := data[0]
	for i := 1; i < len(data); i++ {
		set := data[i]
		for key, value := range set {
			outValue, ok := out[key]
			if !ok {
				out[key] = value
				continue
			}

			outValue.sum += value.sum
			outValue.min = min(outValue.min, value.min)
			outValue.max = max(outValue.max, value.max)
			outValue.count += value.count
			out[key] = outValue
		}
	}
	return out
}

// fastFloat parses slice of bytes into float64 without conversion to string
func fastFloat(b []byte) float64 {
	var sign float64 = 1
	var result float64
	var divisor float64 = 1
	decimalPointPassed := false

	var i int
	if b[i] == '-' {
		sign = -1
		i++
	}

	var char byte
	for ; i < len(b); i++ {
		char = b[i]
		if char == '.' {
			decimalPointPassed = true
			continue
		}

		if char < '0' || char > '9' {
			panic(errors.New("expected [0,9]"))
		}
		digit := float64(char - '0')

		if decimalPointPassed {
			divisor *= 10
			result += digit / divisor
		} else {
			result = result*10 + digit
		}
	}

	return result * sign

}

// ---
// NOT SIGNIFICANT FUNCTIONS BELOW (helpers for read and simple conversions)
// ---

// fixMap converts map from [keySize]int keyed into `string` keyed
func fixMap(m1 map[[keySize]byte]Agg) map[string]Agg {
	out := make(map[string]Agg, len(m1))
L:
	for key := range m1 {
		for i, b := range key {
			if b == 0x0 {
				out[string(key[:i])] = m1[key]
				continue L
			}
		}
		out[string(key[:])] = m1[key]
	}
	return out
}

// readData reads data from ./data/measurements.txt file
// Data can be generated via tools in
// https://github.com/gunnarmorling/1brc repository
func readData() []byte {
	f, err := os.Open("./data/measurements.txt")
	if err != nil {
		panic(err)
	}

	stat, err := f.Stat()
	if err != nil {
		panic(err)
	}
	size := stat.Size()
	data := make([]byte, size)
	n, err := io.ReadFull(f, data)
	if err != nil {
		panic(err)
	}
	if n != int(size) {
		panic("n != size")
	}
	return data
}

func writeResultsToFile(results map[string]Agg) {
	resF, err := os.Create("result.txt")
	if err != nil {
		panic(err)
	}
	defer resF.Close()
	printResults(results, resF)
}

func printResults(data map[string]Agg, w io.Writer) {
	var keys = make([]string, 0, len(data))
	for key, _ := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	w.Write([]byte{'{'})

	var res string
	for _, key := range keys[:len(keys)-1] {
		v := data[key]
		res = fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", key, v.min, v.sum/float64(v.count), v.max)
		w.Write([]byte(res))
	}

	key := keys[len(keys)-1]
	v := data[key]
	res = fmt.Sprintf("%s=%.1f/%.1f/%.1f", key, v.min, v.sum/float64(v.count), v.max)
	w.Write([]byte(res))

	w.Write([]byte{'}'})
}
