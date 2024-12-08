package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "net/http/pprof"

	"github.com/bytedance/sonic"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
)

var pCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "device_inserted",
})
var pTimer = promauto.NewCounter(prometheus.CounterOpts{
	Name: "device_inserted_time",
})

type Operation int

const AddOperation Operation = 1
const RemoveOperation Operation = 2

type record struct {
	campaign int
	deviceID string
}

var linesPool = sync.Pool{
	New: func() interface{} {
		return make([]string, 0, 100000)
	},
}

func processTL(campaign int, path string, db *bbolt.DB) error {
	start := time.Now()
	count := 0
	fmt.Println("Processing file:", path)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close() // Ensure the file is closed after finishing reading

	lines := linesPool.Get().([]string)
	// Create a new scanner to read the file
	scanner := bufio.NewScanner(file)
	ch := make(chan []string, 1)
	group := errgroup.Group{}
	group.Go(func() error {
		for devices := range ch {
			err := updateDb(db, campaign, devices)
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Iterate over each line using the scanner
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) >= cap(lines) {
			count += len(lines)
			ch <- lines
			lines = linesPool.Get().([]string)
		}
	}
	if len(lines) > 0 {
		ch <- lines
	}

	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}
	close(ch)
	err = group.Wait()
	if err != nil {
		return err
	}
	time := time.Since(start)
	pCounter.Add(float64(count))
	pTimer.Add(float64(time.Seconds()))
	return nil
}

func encode(op Operation, campaign int, previousValue []byte) (newValue []byte, needUpdate bool, err error) {
	if previousValue == nil {
		switch op {
		case AddOperation:
			b, err := sonic.Marshal([]int{campaign})
			return b, true, err
		case RemoveOperation:
			return nil, false, nil
		}
	}
	var campaigns []int
	err = sonic.Unmarshal(previousValue, &campaigns)
	if err != nil {
		return nil, false, err
	}
	campaignContained := slices.Contains(campaigns, campaign)
	if !campaignContained && op == AddOperation {
		campaigns = append(campaigns, campaign)
		newValue, err := sonic.Marshal(campaigns)
		return newValue, true, err
	}
	if campaignContained && op == RemoveOperation {
		campaigns = slices.DeleteFunc(campaigns, func(c int) bool {
			if c == campaign {
				return true
			}
			return false
		})
		newValue, err := sonic.Marshal(campaigns)
		return newValue, true, err
	}
	return previousValue, false, nil
}

func updateDb(db *bbolt.DB, campaign int, lines []string) error {
	defer func() {
		lines = lines[:0]
		linesPool.Put(lines)
	}()
	total := len(lines)
	updated := 0
	uptodate := 0
	return db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("device"))
		for _, line := range lines {
			if line == "" {
				continue
			}
			deviceID := []byte(line)
			previousValue := bucket.Get(deviceID)
			newValue, needUpdate, err := encode(AddOperation, campaign, previousValue)
			if err != nil {
				return err
			}
			if needUpdate {
				err := bucket.Put(deviceID, newValue)
				if err != nil {
					return err
				}
				updated++
			} else {
				uptodate++
			}

			err = bucket.Put(deviceID, newValue)
		}
		fmt.Println("total:", total, "Updated:", updated, "Uptodate:", uptodate)
		return nil
	})
}

func main() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe("localhost:8080", nil)
	}()

	//campaign := flag.Int("campaign", 1, "Campaign ID")
	//list := flag.String("list", "/home/kinou/Downloads/202412071544000", "List of files to process")

	flag.Parse()

	db, err := bbolt.Open("db-update.bolt", 0o666, &bbolt.Options{
		FreelistType:   bbolt.FreelistMapType,
		NoFreelistSync: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.Update(func(tx *bbolt.Tx) error {
		for _, name := range []string{"device", "campaigns"} {
			bucket, err := tx.CreateBucketIfNotExists([]byte(name))
			if err != nil {
				panic(err)
			}
			fmt.Println("DB loaded, items:", bucket.Stats().KeyN)
		}

		return nil
	})
	//err = processTL(*campaign, *list, db)
	matches, err := filepath.Glob("updates/*.csv")
	err = loadTLs(matches, db)
	if err != nil {
		log.Fatal(err)
	}
}

func loadTLs(paths []string, db *bbolt.DB) error {

	for _, path := range paths {
		campaign, err := strconv.Atoi(strings.Split(filepath.Base(path), ".")[0])
		if err != nil {
			return err
		}
		// ttype
		err = processTL(campaign, path, db)
		if err != nil {
			return err
		}
	}
	return nil
}
