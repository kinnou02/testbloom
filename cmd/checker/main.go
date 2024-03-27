package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	bloom "github.com/bits-and-blooms/bloom/v3"
	"github.com/gin-gonic/gin"
)

func main() {
	go func() {
		http.ListenAndServe("localhost:8080", nil)
	}()

	begin := time.Now()
	var bf bloom.BloomFilter
	file, err := os.Open("filter.bin")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	_, err = bf.ReadFrom(bufio.NewReader(file))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Time to load data:", time.Since(begin))
	fmt.Println("Bloom filter loaded with", bf.Cap(), "elements.", bf.K(), "hash functions.")
	router := gin.Default()
	router.GET("/contains", func(c *gin.Context) {
		rs := make([]bool, 0, 100)
		for i := 0; i < 100; i++ {
			hash := c.Query("hash")
			rs = append(rs, bf.Test([]byte(hash)))
		}
		status := http.StatusOK
		if !rs[0] {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"result": rs})
	})
	router.Run(":6060")

}
