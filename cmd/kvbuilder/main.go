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

	"slices"

	"github.com/bytedance/sonic"
	"go.etcd.io/bbolt"
)

type record struct {
	campaign int
	deviceID string
}

var count = 0

func loadData(db *bbolt.DB) error {

	//read file to exctract campaigns and list url
	//tlsFile, err := os.Open("dtl.csv")
	tlsFile, err := os.Open("dtl.csv")
	if err != nil {
		return err
	}
	defer tlsFile.Close() // Ensure the file is closed after finishing reading

	// Create a new scanner to read the file
	scanner := bufio.NewScanner(tlsFile)

	ch := make(chan []record, 3)
	wg := sync.WaitGroup{}
	f := func() {
		for p := range ch {
			updateDb(db, p)
		}
		wg.Done()
	}
	for range 4 {
		wg.Add(1)
		go f()
	}

	i := 0
	limit := make(chan interface{}, 4)
	// Iterate over each line using the scanner
	wg2 := sync.WaitGroup{}
	for scanner.Scan() {
		i++
		line := scanner.Text() // Get the current line
		frags := strings.Split(line, ",")
		campaign, err := strconv.ParseInt(frags[0], 10, 34)
		if err != nil {
			return err
		}
		file := frags[1]
		fmt.Println(campaign, file)
		limit <- struct{}{}
		wg2.Add(1)
		go processTL(int(campaign), file, db, ch, limit, &wg2)
	}

	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}
	wg2.Wait()
	close(ch)
	wg.Wait()
	//shrink slices
	//read each line
	return nil
}

func processTL(campaign int, path string, db *bbolt.DB, ch chan []record, limit chan interface{}, wg *sync.WaitGroup) error {
	defer func() {
		<-limit
		wg.Done()
	}()
	fmt.Println("Processing file:", path)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close() // Ensure the file is closed after finishing reading

	// Create a new scanner to read the file
	scanner := bufio.NewScanner(file)

	size := 1000
	// Iterate over each line using the scanner
	lines := make([]record, 0, size)
	for scanner.Scan() {
		lines = append(lines, record{deviceID: scanner.Text(), campaign: campaign})
		if len(lines) == cap(lines) {
			ch <- lines
			lines = make([]record, 0, size)
		}
	}

	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}
	if len(lines) > 0 {
		ch <- lines
	}
	return nil
}

func updateDb(db *bbolt.DB, lines []record) error {
	defautlCampaign, err := sonic.Marshal([]int{lines[0].campaign})
	if err != nil {
		return err
	}
	return db.Batch(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("devices"))
		if err != nil {
			return err
		}
		for _, line := range lines {
			if line.deviceID == "" {
				continue
			}
			count++
			deviceID := []byte(line.deviceID)
			bcampaigns := bucket.Get(deviceID)
			if bcampaigns == nil {
				err = bucket.Put([]byte(line.deviceID), defautlCampaign)
				if err != nil {
					panic(err)
				}
				continue
			}
			var campaigns []int
			err = sonic.Unmarshal(bcampaigns, &campaigns)
			if err != nil {
				panic(err)
			}
			if !slices.Contains(campaigns, line.campaign) {
				campaigns = append(campaigns, line.campaign)
				bcampaigns, err = sonic.Marshal(campaigns)
				if err != nil {
					panic(err)
				}
				err = bucket.Put(deviceID, bcampaigns)
				if err != nil {
					panic(err)
				}
			}
		}
		return nil
	})
}

func main() {
	go func() {
		http.ListenAndServe("localhost:8080", nil)
	}()

	db, err := bbolt.Open("db.bolt", 0666, &bbolt.Options{
		FreelistType:   bbolt.FreelistMapType,
		NoFreelistSync: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	begin := time.Now()
	err = loadData(db)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Time to load data:", time.Since(begin), "nb lines:", count)
}
