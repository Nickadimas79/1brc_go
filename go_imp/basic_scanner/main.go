package basic_scanner

import (
	"bufio"
	"fmt"
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

func BasicScanner() {
	log.Println(":::STARTING:::")
	start := time.Now()

	f, err := os.Open("measurements.txt")
	if err != nil {
		log.Println("error opening file:", err)
	}
	defer f.Close()

	m := make(map[string]*data)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		station, temp, present := strings.Cut(line, ";")
		if !present {
			continue
		}

		tempFloat, err := strconv.ParseFloat(temp, 64)
		if err != nil {
			log.Println("error parsing temp to float64:", err)
		}

		if _, ok := m[station]; !ok {
			m[station] = &data{}
			m[station].temps = make([]float64, 0)
			m[station].temps = append(m[station].temps, tempFloat)
		} else {
			m[station].temps = append(m[station].temps, tempFloat)
		}
	}

	for station, info := range m {
		tot := 0.0

		for _, temp := range info.temps {
			tot += temp
		}

		m[station].avg = tot / float64(len(info.temps))
		m[station].hi = slices.Max(info.temps)
		m[station].low = slices.Min(info.temps)
	}

	for k, v := range m {
		fmt.Printf("station:: %v avg temp:: %v\n", k, v)
	}

	end := time.Now()
	log.Printf("time taken:: %v\n", end.Sub(start))
	log.Println(":::DONE:::")
}
