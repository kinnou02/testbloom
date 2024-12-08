package main

import (
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	_ "net/http/pprof"

	"gopkg.in/pipe.v2"
)

func main() {
	go func() {
		http.ListenAndServe("localhost:8080", nil)
	}()

	//campaign := flag.Int("campaign", 1, "Campaign ID")
	//list := flag.String("list", "/home/kinou/Downloads/202412071544000", "List of files to process")
	tls := flag.String("tls", "tl.csv", "List of files to process")

	flag.Parse()

	campaigns, err := buildCampaignsState(*tls)
	if err != nil {
		panic(err)
	}

	err = buildUpdate(campaigns)
	if err != nil {
		panic(err)
	}

}

type Campaign struct {
	id    int
	white string
	black string
}

func buildUpdate(campaigns map[int]Campaign) error {
	for _, c := range campaigns {
		fmt.Printf("Processing campaign %+v\n", c)
		if c.white == "" {
			fmt.Printf("Campaign %d has no white list\n", c.id)
			continue
		}
		var p pipe.Pipe
		if c.black == "" {
			p = pipe.Script(
				pipe.System(fmt.Sprintf("sort -c \"%s\" || sort -o \"%s\" \"%s\"", c.white, c.white, c.white)),
				pipe.System(fmt.Sprintf("cp \"%s\" updates/%d.csv", c.white, c.id)),
			)
		} else {
			// should short before
			p = pipe.Script(
				pipe.Line(pipe.System(fmt.Sprintf("sort -c \"%s\" || sort -o \"%s\" \"%s\"", c.white, c.white, c.white))),
				pipe.Line(pipe.System(fmt.Sprintf("sort -c \"%s\" || sort -o \"%s\" \"%s\"", c.black, c.black, c.black))),
				pipe.Line(
					pipe.Exec("comm", "-23", c.white, c.black),
					pipe.WriteFile(fmt.Sprintf("updates/%d.csv", c.id), 0644),
				),
			)
		}
		ouptup, err := pipe.CombinedOutput(p)
		if err != nil {
			fmt.Println(string(ouptup))
			return err
		}

	}
	return nil

}

func buildCampaignsState(path string) (map[int]Campaign, error) {

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	csvReader.Comma = ';'
	campaigns := map[int]Campaign{}
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		campaign, err := strconv.Atoi(row[0])
		if err != nil {
			return nil, err
		}
		// ttype
		ttype := row[1]
		url := row[3]
		md := md5.Sum([]byte(url))
		filePath := fmt.Sprintf("lists/%s  -", hex.EncodeToString(md[:]))
		if c, ok := campaigns[campaign]; ok {
			if ttype == "white" {
				c.white = filePath
			} else {
				c.black = filePath
			}
			campaigns[campaign] = c
		} else {
			c := Campaign{id: campaign}
			if ttype == "white" {
				c.white = filePath
			} else {
				c.black = filePath
			}
			campaigns[campaign] = c
		}
	}
	return campaigns, nil
}
