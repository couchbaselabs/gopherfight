package main

import (
	"flag"
	"fmt"
	"github.com/couchbaselabs/gocb"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"runtime/pprof"
	"time"
	"strconv"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var numconcur = flag.Int("num-concur", 1000, "number of concurrent worker threads")
var numitems = flag.Int("num-items", 5000, "total number of items")
var setpct = flag.Int("set-pct", 50, "percentage of operations which should be mutations")
var nopop = flag.Bool("no-population", false, "skip the initial item load")
var minsize = flag.Int("min-size", 128, "minimum item size")
var maxsize = flag.Int("max-size", 4096, "maximum item size")
var prefix = flag.String("prefix", "gopherfight_", "key prefix for all keys")
var numops = flag.Int("num-ops", 500000, "total number of operations to perform")

var spec = flag.String("spec", "couchbase://127.0.0.1", "connection string for cluster")
var bucket = flag.String("bucket", "default", "bucket name to use")
var password = flag.String("password", "", "bucket password")

type testItem struct {
	Data string
}

func makeItem(size int) testItem {
	return testItem{
		Data: strings.Repeat("x", size),
	}
}

type opResults struct {
	Err int64
	MinTime time.Duration
	MaxTime time.Duration
	TotalTime time.Duration
	NumItems int64
}

func (r *opResults) Add(s opResults) {
	r.Err += s.Err
	if r.MinTime == 0 || s.MinTime < r.MinTime {
		r.MinTime = s.MinTime
	}
	if r.MaxTime == 0 || s.MaxTime > r.MaxTime {
		r.MaxTime = s.MaxTime
	}
	r.TotalTime += s.TotalTime
	r.NumItems += s.NumItems
}

func (r *opResults) AddItem(d time.Duration) {
	if r.MinTime == 0 || d < r.MinTime {
		r.MinTime = d
	}
	if r.MaxTime == 0 || d > r.MaxTime {
		r.MaxTime = d
	}
	r.NumItems++
	r.TotalTime += d
}

func (r *opResults) Print() {
	avgTime := time.Duration(int64(r.TotalTime)/r.NumItems)
	fmt.Printf("  Err: %d/%d, Min: %s, Max: %s, Avg: %s\n", r.Err, r.NumItems, r.MinTime, r.MaxTime, avgTime)
}

type workerResults struct {
	Id int
	GetRes opResults
	SetRes opResults
}

func (r *workerResults) Add(s workerResults) {
	r.GetRes.Add(s.GetRes)
	r.SetRes.Add(s.SetRes)
}

func (r *workerResults) Print() {
	fmt.Printf("Get: \n")
	r.GetRes.Print()
	fmt.Printf("Set: \n")
	r.SetRes.Print()
}

func main() {
	runtime.GOMAXPROCS(2)

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	cluster, err := gocb.Connect(*spec)
	fmt.Printf("OpenCluster: %v, %v\n", cluster, err)

	bucket, err := cluster.OpenBucket(*bucket, *password)
	fmt.Printf("OpenBucket: %v, %v\n", bucket, err)

	minItemSize := int(*minsize)
	maxItemSize := int(*maxsize)
	numItems := int(*numitems)
	numWorkers := int(*numconcur)
	pctSetOps := int(*setpct)
	numConcur := int(*numconcur)
	totalOps := int(*numops)
	opsPerWorker := totalOps / numConcur

	if !*nopop {
		signal := make(chan bool, 1)
		for i := 0; i < numItems; i++ {
			go func(i int) {
				itemKey := *prefix + strconv.Itoa(i)
				itemSize := rand.Intn(maxItemSize-minItemSize) + minItemSize
				item := makeItem(itemSize)
				_, err := bucket.Upsert(itemKey, item, 0)
				if err != nil {
					log.Fatal("Failed during initial insertion of items.")
				}
				signal <- true
			}(i)
		}
		for i := 0; i < numItems; i++ {
			<-signal
		}
		fmt.Printf("Completed initial insertion.\n")
	}

	signal := make(chan workerResults, numWorkers)
	workerThread := func(workerId int) {
		results := workerResults{
			Id: workerId,
		}

		rng := rand.New(rand.NewSource(int64(workerId)))

		for i := 0; i < opsPerWorker; i++ {
			itemKey := *prefix + strconv.Itoa(rng.Intn(numItems))

			if rng.Intn(100) < pctSetOps {
				// Do a Set
				itemSize := rng.Intn(maxItemSize-minItemSize) + minItemSize
				item := makeItem(itemSize)

				opSTime := time.Now()
				_, err := bucket.Upsert(itemKey, item, 0)
				opETime := time.Now()

				if err != nil {
					results.SetRes.Err++
				}

				opDTime := opETime.Sub(opSTime)
				results.SetRes.AddItem(opDTime)
			} else {
				// Do a Get
				var item testItem

				opSTime := time.Now()
				_, err := bucket.Get(itemKey, &item)
				opETime := time.Now()

				if err != nil {
					results.GetRes.Err++
				}

				opDTime := opETime.Sub(opSTime)
				results.GetRes.AddItem(opDTime)
			}
		}

		signal <- results
	}

	sTime := time.Now()

	// Start all the workers
	for i := 0; i < numWorkers; i++ {
		go workerThread(i)
	}

	// Join all the workers
	resultTotals := &workerResults{}
	for i := 0; i < numWorkers; i++ {
		result := <-signal
		resultTotals.Add(result)

		//fmt.Printf("Worker %d finished with %d:%d sets and %d:%d gets (success:error)\n", result.Id,
		//	result.SetOkCount, result.SetErrCount, result.GetOkCount, result.GetErrCount)
	}

	eTime := time.Now()
	dTime := eTime.Sub(sTime)
	opsPerSec := float64(totalOps) / (float64(dTime) / float64(time.Second))

	fmt.Printf("%d concurrent of %d ops (%d total) took %s (%f ops/s)\n", numConcur, opsPerWorker, totalOps, dTime, opsPerSec)
	resultTotals.Print()

	return
}
