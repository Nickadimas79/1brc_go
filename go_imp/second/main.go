package second

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

type data struct {
	hi    float64
	low   float64
	avg   float64
	temps []float64
}

func Second() {
	log.Println(":::STARTING:::")
	start := time.Now()

	_ = consumeLine(produceLine("measurements.txt"))

	end := time.Now()
	log.Printf("time taken:: %v\n", end.Sub(start))
	log.Println(":::DONE:::")
}

func produceLine(file string) <-chan [100]string {
	resChan := make(chan [100]string)
	fileInfo, err := os.Stat("measurements.txt")
	if err != nil {
		fmt.Println("error getting file stats:", err)
	}
	fmt.Println("FILE INFO::", fileInfo.Size())

	//  reading 100MB per request
	chunkSize := 100 * 1024 * 1024
	buf := make([]byte, chunkSize)

	f, err := os.Open(file)
	if err != nil {
		log.Println("error opening file:", err)
	}

	go func() {
		defer close(resChan)

		r := bufio.NewReader(f)
		for {
			bytesRead, err := r.Read(buf)
			buf = buf[:bytesRead]
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
				buf = append(buf, nnl...)
			}

			// process chunk here
			processChunk(buf, resChan)
		}
	}()

	return resChan
}

func processChunk(bufChunk []byte, resStream chan [100]string) {
	resArr := [100]string{}
	ct := 0

	bufArr := strings.Split(string(bufChunk), "\n")
	for _, line := range bufArr {
		resArr[ct] = line
		ct++

		if ct == 100 {
			resStream <- resArr
			ct = 0
		}
	}
}

func consumeLine(lineData <-chan [100]string) map[string]*data {
	m := make(map[string]*data)

	for l := range lineData {
		for _, line := range l {
			idx := strings.Index(line, ";")
			if idx == -1 {
				continue
			}

			city := line[:idx]
			tempFloat, err := strconv.ParseFloat(line[idx+1:], 64)
			if err != nil {
				log.Println("error parsing temp to float64:", err)
			}

			if _, ok := m[city]; !ok {
				m[city] = &data{}
				m[city].temps = make([]float64, 0)
				m[city].temps = append(m[city].temps, tempFloat)
			} else {
				m[city].temps = append(m[city].temps, tempFloat)
			}

			m[city].hi = slices.Max(m[city].temps)
			m[city].low = slices.Min(m[city].temps)
		}
	}

	for _, info := range m {
		tot := 0.0
		for _, n := range info.temps {
			tot += n
		}
		info.avg = tot / float64(len(info.temps))
	}

	return m
}
