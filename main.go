package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"sort"
	"sync"

	"golang.org/x/exp/maps"
)

// 0 = min, 1 = max, 2 = avg, 3 = count
type measure10x struct {
	min   int16
	max   int16
	count int32
	sum   int64
}

func main() {
	// Start CPU profiling
	f, err := os.Create("cpu.pprof")
	if err != nil {
		panic(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	if len(os.Args) < 2 {
		panic("missing arg")
	}

	rawChunks := make(chan []byte, 1000)
	validChunks := make(chan []byte, 1000)
	statsChan := make(chan map[cityName]*measure10x, 1000)
	// load file
	go loadFile(os.Args[1], rawChunks)

	// merge chunks
	go mergeChunks(rawChunks, validChunks)

	wg := sync.WaitGroup{}
	for i := 0; i <= 1024; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processValidChunks(validChunks, statsChan)
		}()
	}

	go func() {
		wg.Wait()
		close(statsChan)
	}()

	// stream chunks
	aggResults := make(map[cityName]*measure10x, 10000)
	for sc := range statsChan {
		for c, stats := range sc {
			r := aggResults[c]
			if r == nil {
				aggResults[c] = stats
				continue
			}

			if r.min > stats.min {
				r.min = stats.min
			} else if r.max < stats.max {
				r.max = stats.max
			}
			r.sum += stats.sum
			r.count += stats.count
		}
	}

	// sort
	cityNames := maps.Keys(aggResults)
	sort.Slice(cityNames, func(i, j int) bool {
		return bytes.Compare(cityNames[i][:], cityNames[j][:]) < 0
	})

	// print
	var count int32
	fmt.Println("{")
	for idx, name := range cityNames {
		stats := aggResults[name]
		fmt.Printf("\t{\"%s\":[%.1f,%.1f,%.1f]}", name, float64(stats.min)/10., float64(stats.max)/10., float64(stats.sum)/10./float64(stats.count))
		if idx != len(cityNames)-1 {
			fmt.Printf(",")
		}
		fmt.Println()
		count += stats.count
	}
	fmt.Println("}")

	fmt.Println(count)
}

func loadFile(filePath string, rawChunks chan<- []byte) {
	f, err := os.Open(filePath)
	if err != nil {
		panic("can't open file: " + err.Error())
	}

	buf := make([]byte, 1024*1024*100) // 1MB

	for {
		read, err := f.Read(buf)
		if read > 0 {
			tmp := make([]byte, read)
			copy(tmp, buf)
			rawChunks <- tmp
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			panic("unable to read file: %s" + err.Error())
		}
	}

	close(rawChunks)
}

func mergeChunks(raw <-chan []byte, valid chan<- []byte) {
	remainder := make([]byte, 0)
	for r := range raw {
		// split it between the usable (to be merged with previously usable remainder) and remainder
		validData, newRemainder := mergeChunk(remainder, r)
		if len(validData) > 0 {
			valid <- validData
		}
		remainder = newRemainder
	}

	// flush
	if len(remainder) > 0 {
		valid <- remainder
	}

	close(valid)
}

var delim = []byte{'\n'}

func mergeChunk(previousData, newData []byte) (valid, remainder []byte) {
	// This tries to merge previousData + merge
	// need to split on \n
	firstLine := bytes.Index(newData, delim)
	lastNewLine := bytes.LastIndex(newData, delim)

	if firstLine != -1 {
		// prefix the previousData to the newData
		valid = append(previousData, newData[:lastNewLine+1]...)
		remainder = newData[lastNewLine+1:]
	} else {
		remainder = append(previousData, newData...)
	}

	return valid, remainder
}

type cityName [64]byte

func processValidChunks(validChunks <-chan []byte, statsChan chan map[cityName]*measure10x) {
	cityStats := make(map[cityName]*measure10x, 10000)

	var name cityName
	var letterInName int

	var temp int16
	var neg bool

	for c := range validChunks {
		for _, b := range c {
			switch b {
			case '\n':
				if neg {
					temp = -temp
				}
				// register city
				cs := cityStats[name]
				if cs == nil {
					cityStats[name] = &measure10x{
						min:   temp,
						max:   temp,
						count: 1,
						sum:   int64(temp),
					}
				} else {
					if cs.min > temp {
						cs.min = temp
					} else if cs.max < temp {
						cs.max = temp
					}
					cs.sum += int64(temp)
					cs.count++
				}

				// Reset
				neg = false
				name = cityName{}
				letterInName = 0
				temp = 0
			case ';':
				letterInName = 64 // no longer reading name
			case '.':
				// Ignore this. Temps are x10.
			case '-':
				neg = true
			default:
				if letterInName < 64 {
					name[letterInName] = b
					letterInName++
				} else {
					// reading temp
					temp = 10*temp + int16(b-'0')
				}

			}
		}
	}

	statsChan <- cityStats
}

func customHash(s string) int64 {
	var hash int64 = 5381

	for i := 0; i < len(s); i++ {
		hash = ((hash << 5) + hash) + int64(s[i])
	}

	return hash
}
