package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	bloom "github.com/bits-and-blooms/bloom/v3"
)

var pool = sync.Pool{
	New: func() interface{} {
		buf := make([][]byte, 10000)
		for i := range buf {
			buf[i] = make([]byte, 100)
		}
		return buf
	},
}

func loadData() (*bloom.BloomFilter, error) {
	bl := bloom.NewWithEstimates(300_000_000, 0.001)

	//read file to exctract campaigns and list url
	tlsFile, err := os.Open("dtl.csv")
	if err != nil {
		return bl, err
	}
	defer tlsFile.Close() // Ensure the file is closed after finishing reading

	// Create a new scanner to read the file
	scanner := bufio.NewScanner(tlsFile)
	ch := make(chan [][]byte, 100)

	wg := sync.WaitGroup{}
	wg.Add(1)
	counter := 0
	colision := 0
	go func() {
		for p := range ch {
			for _, line := range p {
				bl.Add(line)
				counter++
			}
			pool.Put(p)
		}
		wg.Done()
		fmt.Println("Total lines processed:", counter, "Colisions:", colision, "Colision rate:", float64(colision)/float64(counter))
	}()

	i := 0
	// Iterate over each line using the scanner
	for scanner.Scan() {
		i++
		line := scanner.Text() // Get the current line
		frags := strings.Split(line, ",")
		campaign, err := strconv.ParseInt(frags[0], 10, 34)
		if err != nil {
			return bl, err
		}
		file := frags[1]
		if err != nil {
			return bl, err
		}
		fmt.Println(campaign, file)
		processTL(int(campaign), file, ch)
	}
	close(ch)

	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}
	wg.Wait()
	//shrink slices
	//read each line
	return bl, nil
}

func processTL(campaign int, path string, ch chan [][]byte) error {
	fmt.Println("Processing file:", path)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close() // Ensure the file is closed after finishing reading

	// Create a new scanner to read the file
	scanner := bufio.NewScanner(file)

	// Iterate over each line using the scanner
	p := pool.Get().([][]byte)
	i := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if e := copy(p[i], line); e < len(line) {
			log.Fatal("Error copying line ", string(line), " to buffer ", e, len(line), len(p[i]))
		}
		i++

		if i == len(p) {
			ch <- p
			p = pool.Get().([][]byte)
			i = 0
		}
		//bf.Add(line)
	}
	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}
	if i > 0 {
		ch <- p[:i]
	}
	return nil
}

func main() {
	go func() {
		http.ListenAndServe("localhost:8080", nil)
	}()

	begin := time.Now()
	bf, err := loadData()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Time to load data:", time.Since(begin))
	b, err := bf.MarshalBinary()
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.Create("filter.bin")
	if err != nil {
		log.Fatal("Error opening file", err)
	}
	_, err = file.Write(b)
	if err != nil {
		log.Fatal("Error writing file", err)
	}
	file.Close()
	fmt.Println("Bloom filter loaded with", bf.Cap(), "elements.", bf.K(), "hash functions.", "Size:", bf.ApproximatedSize())
	ActualfpRate := bloom.EstimateFalsePositiveRate(bf.Cap(), bf.K(), 200_000_000)
	fmt.Println("Actual False Positive Rate:", ActualfpRate)
}
