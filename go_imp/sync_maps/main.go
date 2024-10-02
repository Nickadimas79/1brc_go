package sync_maps

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func Run() {
	log.Println(":::STARTING:::")
	start := time.Now()

	// dataMap := make(map[string]*info)
	dataMap := sync.Map{}
	// ans := make(map[string]float64)
	ans := sync.Map{}
	ct := 0

	chunkSize := 4 * 1024
	buffer := make([]byte, chunkSize)

	// dir, err := os.Getwd()
	// fmt.Print("WD::", dir)

	file, err := os.Open("measurements.txt")
	if err != nil {
		log.Println("error in file open:", err)
	}
	defer file.Close()

	r := bufio.NewReader(file)

	// process file into usable object
	for {
		// loading chunk into buffer
		bytesRead, err := r.Read(buffer)
		if err == io.EOF {
			log.Println("end of file processing")

			break
		}

		// check for when bytes gotten are less than buffer
		buffer = buffer[:bytesRead]

		// read the next line and concat to buffer accommodating for line
		// splits in byte chunks
		nnl, err := r.ReadBytes('\n')
		if err != io.EOF {
			buffer = append(buffer, nnl...)
		}

		// begin processing chunk

		// split chunk into lines
		dataSlice := strings.Split(string(buffer), "\n")
		for _, data := range dataSlice {
			if data == "" {
				continue
			}

			ct++
			str := strings.Split(data, ";")

			var wg sync.WaitGroup
			go func() {
				wg.Add(1)
				defer wg.Done()

				res, ok := dataMap.Load(str[0])

				run := true
				for run {
					switch {
					case ok:
						temps := res.([]string)
						temps = append(temps, str[1])
						dataMap.Store(str[0], temps)

						run = false
					case !ok:
						dataMap.Store(str[0], []string{str[1]})

						run = false
					default:
						continue
					}
				}
			}()

			wg.Wait()
			// if _, ok := dataMap[str[0]]; ok && dataMap[str[0]].work == false {
			// 	dataMap[str[0]].temps = append(dataMap[str[0]].temps, str[1])
			// 	dataMap[str[0]].temps[0] = str[1]
			// } else {
			// 	dataMap[str[0]] = &info{
			// 		work: false,
			// 	}
			// 	dataMap[str[0]].temps = append(dataMap[str[0]].temps, str[1])
			// }
		}
	}

	// f := func(k any, v any) bool {
	// 	fmt.Printf("town:: %v temps:: %v\n", k, v)
	// 	return true
	// }
	// dataMap.Range(f)

	var wg sync.WaitGroup
	f := func(k, v interface{}) bool {
		// log.Println("working on:::", k)
		tot := 0.0
		l := 0
		arr := v.([]string)

		go func() {
			wg.Add(1)
			defer wg.Done()
			for _, t := range arr {
				intN, err := strconv.ParseFloat(t, 64)
				if err != nil {
					log.Println("error converting number string:", err)
				}

				l++
				tot += intN
			}

			ans.Store(k.(string), tot/float64(l))
		}()

		return true
	}

	dataMap.Range(f)
	wg.Wait()

	// f1 := func(k any, v any) bool {
	// 	fmt.Printf("town:: %v temps:: %v\n", k, v)
	// 	return true
	// }
	// ans.Range(f1)

	// for k, v := range dataMap {
	// 	if len(v.temps) > 1 {
	// 		tot := 0.0
	// 		l := len(v.temps)
	// 		for _, strN := range v.temps {
	// 			intN, err := strconv.ParseFloat(strN, 64)
	// 			if err != nil {
	// 				log.Println("error converting number string:", err)
	// 			}
	//
	// 			tot += intN
	// 		}
	//
	// 		ans[k] = tot / float64(l)
	// 	} else {
	// 		intN, err := strconv.ParseFloat(v.temps[0], 64)
	// 		if err != nil {
	// 			log.Println("error converting number string:", err)
	// 		}
	//
	// 		ans[k] = intN
	// 	}
	// }

	end := time.Now()
	fmt.Println()
	log.Printf("time taken:: %v\n", end.Sub(start))
	// log.Printf("dataMap size:: %v\n", len(dataMap))
	// log.Printf("completed ans size:: %v\n", len(ans))
	log.Printf("items items processed:: %v\n", ct)

	log.Println(":::DONE:::")
}
