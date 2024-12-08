package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bytedance/sonic"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	redcon "github.com/tidwall/redcon"
	"go.etcd.io/bbolt"
)

var histo = promauto.NewSummary(prometheus.SummaryOpts{
	Name:       "check_latency",
	Help:       "Latency of check",
	Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	MaxAge:     time.Second * 30,
})

func main() {
	go func() {
		http.ListenAndServe("localhost:8080", nil)
	}()

	begin := time.Now()
	fmt.Println("Loading data")
	db, err := bbolt.Open("db.bolt", 0444, &bbolt.Options{ReadOnly: true})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	/*
		db.View(func(tx *bbolt.Tx) error {
						fmt.Println("data loaded, size:", tx.Bucket([]byte("devices")).Stats().KeyN)
						return nil
					})
	*/
	go func() {
		s := <-signals
		fmt.Println("Signal:", s)
		db.Close()
		fmt.Println("DB closed")
		os.Exit(0)
	}()

	fmt.Println("Time to load data:", time.Since(begin))
	/*
		router := gin.New()
		//router := gin.Default()
		router.GET("/metrics", gin.WrapH(promhttp.Handler()))

		var v []byte
		router.GET("/contains", func(c *gin.Context) {
			hash := c.Query("hash")
			// b := time.Now()
			//bv := time.Now()
			timer := prometheus.NewTimer(histo)
			db.View(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket([]byte("devices"))
				if bucket == nil {
					return fmt.Errorf("Bucket not found")
				}
				//begin := time.Now()
				v = bucket.Get([]byte(hash))
				//fmt.Println("Value:", v, "Time to get:", time.Since(begin))
				if err != nil {
					c.String(http.StatusInternalServerError, err.Error())
					return nil
				}
				if v == nil {
					c.String(http.StatusNotFound, "")
					return nil
				}
				timer.ObserveDuration()

				var campaigns []int
				if err := sonic.Unmarshal(v, &campaigns); err != nil {
					c.String(http.StatusInternalServerError, err.Error())
					return nil
				}
				c.JSON(http.StatusOK, campaigns)
				return nil

			})
			//fmt.Println("Time total:", time.Since(bv))
		})
	*/
	//go router.Run(":6060")
	go func() {
		http.Handle("/metrics", promhttp.Handler())

		http.ListenAndServe("localhost:6060", nil)
	}()

	redcon.ListenAndServe(":6379", func(conn redcon.Conn, cmd redcon.Command) {
		/*
			cmds := make([]string, len(cmd.Args))
			for arg := range cmd.Args {
				cmds[arg] = string(cmd.Args[arg])
			}
			fmt.Println("Command:", cmds)
		*/

		switch string(cmd.Args[0]) {
		case "GETss":
			if len(cmd.Args) != 2 {
				conn.WriteError("wrong number of arguments")
				return
			}
			db.View(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket([]byte("devices"))
				if bucket == nil {
					conn.WriteNull()
					return nil
				}
				v := bucket.Get(cmd.Args[1])
				if v == nil {
					conn.WriteNull()
					return nil
				}
				conn.WriteBulk(v)
				return nil
			})
		case "GET":
			timer := prometheus.NewTimer(histo)
			err := db.View(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket([]byte("devices"))
				if bucket == nil {
					conn.WriteError("Bucket not found")
					return nil
				}
				v := bucket.Get(cmd.Args[1])
				timer.ObserveDuration()
				if v == nil {
					conn.WriteNull()
					return nil
				}

				var campaigns []int
				if err := sonic.Unmarshal(v, &campaigns); err != nil {
					conn.WriteError("Error unmarshalling data")
					return nil
				}
				conn.WriteAny(campaigns)
				return nil
			})
			if err != nil {
				conn.WriteError("Error getting data")
			}

		case "PING":
			conn.WriteString("PONG")
		default:
			conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
		}

	}, func(conn redcon.Conn) bool {
		return true
	},
		func(conn redcon.Conn, err error) {
		})
}
