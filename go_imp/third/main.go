package third

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StationData struct {
	Name  string
	Min   float64
	Max   float64
	Sum   float64
	Count int

	Lock sync.Mutex
}

type Stations struct {
	Lock sync.RWMutex
	St   map[string]*StationData
}

const BUFFER_SIZE = 1000 * 1024 * 1024
const numWorkers = 7

func Third() {
	started := time.Now()
	run()
	en := time.Since(started).Seconds()

	// station.printResult()
	fmt.Println("TIME TO PROCESS: ", en)
	fmt.Printf("set ct:: %v update ct:: %v total:: %v\n", setCt, updateCT, setCt+updateCT)
}

var stations = Stations{
	St: map[string]*StationData{},
}

func run() {
	f, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	r := bufio.NewReader(f)

	buffer := make([]byte, BUFFER_SIZE)
	for {
		bytesRead, err := r.Read(buffer)
		buffer = buffer[:bytesRead]
		if err == io.EOF {
			fmt.Println(":::end of file:::")
			f.Close()
			break
		}
		if err != nil {
			log.Println("error reading file:", err)
			panic("shutting down")
		}

		nnl, err := r.ReadBytes('\n')
		if err != io.EOF {
			buffer = append(buffer, nnl...)
		}

		buildStationMap(buffer)
	}
}

// NewStations returns new instance of Stations map object
func NewStations() *Stations {
	return &Stations{
		Lock: sync.RWMutex{},
		St:   make(map[string]*StationData),
	}
}

// Get the value associated with the key
func (s *Stations) Get(key string) (*StationData, bool) {
	s.Lock.RLock()
	v, ok := s.St[key]
	s.Lock.RUnlock()

	if ok {
		v.Lock.Lock()
		defer v.Lock.Unlock()

		return v, true
	}

	return nil, false
}

var setCt = 0

// Set the value for a specific key, used for initial creation of key/value pair
func (s *Stations) Set(station string, temp float64) {
	setCt++
	s.Lock.Lock()
	defer s.Lock.Unlock()

	s.St[station] = &StationData{
		Name:  station,
		Min:   temp,
		Max:   temp,
		Sum:   temp,
		Count: 1,
		Lock:  sync.Mutex{},
	}
}

var updateCT = 0

// Update updates the value associated with the key without locking the entire map
func (s *Stations) Update(temp float64, currSt *StationData) {
	updateCT++
	currSt.Lock.Lock()
	defer currSt.Lock.Unlock()

	currSt.Max = max(temp, currSt.Max)
	currSt.Min = min(temp, currSt.Min)
	currSt.Sum += temp
	currSt.Count++
}

func mapWork(d string) {
	st, tempStr, present := strings.Cut(d, ";")

	if !present {
		return
	}

	tempNum, err := strconv.ParseFloat(tempStr, 64)
	if err != nil {
		fmt.Println("error converting temp to int:", err)
	}

	stations.Lock.RLock()

	if v, exists := stations.St[st]; !exists {
		stations.Lock.RUnlock()

		stations.Set(st, tempNum)
	} else {
		stations.Lock.RUnlock()
		stations.Update(tempNum, v)
	}
}

func worker(workerID int, jobs <-chan string) {
	for job := range jobs {
		// fmt.Printf("Worker %d is processing job %d\n", workerID, job)
		mapWork(job)
		// fmt.Printf("Worker %d finished job %d\n", workerID, job)
	}
}

func buildStationMap(data []byte) {
	// fmt.Println("byte data::", string(data))
	lineData := strings.Split(string(data), "\n")

	jobs := make(chan string)

	// Create fixed number of worker goroutines: 1 - numWorkers
	for w := 1; w <= numWorkers; w++ {
		go worker(w, jobs)
	}

	// send work to worker pool over jobs channel
	for _, d := range lineData {
		jobs <- d // Send jobs to workers
	}
	close(jobs) // Close job channel after sending all jobs
}

func (s *Stations) printResult() {
	var arr []StationData
	for _, v := range s.St {
		arr = append(arr, *v)
	}

	sort.Slice(arr, func(i, j int) bool {
		return arr[i].Name < arr[j].Name
	})

	for _, d := range arr {
		fmt.Printf("city:: %v max:: %v min:: %v sum:: %v count:: %v avg:: %v\n", d.Name, d.Max, d.Min, d.Sum, d.Count, d.Sum/float64(d.Count))
	}
}
