package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type config struct {
	workload                                                   string
	tasks, iterations, capacity, work, payload, active, warmup int
}

type result struct {
	Phase      string `json:"phase"`
	Operations int    `json:"operations"`
	Checksum   uint64 `json:"checksum"`
	Elapsed    uint64 `json:"elapsed_ns"`
	CPU        uint64 `json:"cpu_ns"`
	Setup      uint64 `json:"setup_ns"`
	Drain      uint64 `json:"drain_ns"`
	Completed  int    `json:"completed"`
}

var input = bufio.NewReader(os.Stdin)

func command() {
	line, err := input.ReadString('\n')
	if err != nil || line != "\n" {
		panic("protocol: expected newline")
	}
}

func emit(value any) {
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		panic(err)
	}
}

func cpu() uint64 {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		panic(err)
	}
	return uint64(r.Utime.Sec+r.Stime.Sec)*1e9 + uint64(r.Utime.Usec+r.Stime.Usec)*1000
}

func affine(steps int) (uint64, uint64) {
	a, b := uint64(1), uint64(0)
	for i := 0; i < steps; i++ {
		a *= 1664525
		b = b*1664525 + 1013904223
	}
	return a, b
}

func sleeper(input <-chan uint64, done chan<- uint64, ready chan<- struct{}, payload []byte, expected uint64, finished *sync.WaitGroup) {
	defer finished.Done()
	ready <- struct{}{}
	<-input
	var sum uint64
	for _, v := range payload {
		sum += uint64(v)
	}
	if sum != expected {
		panic("bad payload")
	}
	done <- sum
}

func execute(c config, protocol bool) result {
	setup := time.Now()
	count, capacity := c.tasks, 1
	if c.workload == "pipeline" {
		count, capacity = c.tasks-1, c.capacity
	}
	channels := make([]chan uint64, count)
	for i := range channels {
		channels[i] = make(chan uint64, capacity)
	}
	ready := make(chan struct{})
	done := make([]chan uint64, c.tasks)
	for i := range done {
		done[i] = make(chan uint64, 1)
	}
	gate := make(chan struct{}, 1)
	var payload []byte
	if c.workload == "parked" {
		payload = make([]byte, c.tasks*c.payload)
		for i := 0; i < c.tasks; i++ {
			for j := 0; j < c.payload; j++ {
				payload[i*c.payload+j] = byte(i % 251)
			}
		}
	}
	a, b := uint64(1), uint64(0)
	if c.workload == "pipeline" {
		a, b = affine((c.tasks - 2) * c.work)
	}
	var finished sync.WaitGroup
	finished.Add(c.tasks)
	for i := 0; i < c.tasks; i++ {
		id := i
		if c.workload == "parked" {
			go sleeper(channels[id], done[id], ready, payload[id*c.payload:(id+1)*c.payload], uint64((id%251)*c.payload), &finished)
			continue
		}
		go func() {
			defer finished.Done()
			ready <- struct{}{}
			var checksum uint64
			switch c.workload {
			case "handoff":
				for k := 0; k < c.iterations; k++ {
					value := <-channels[id]
					if value != uint64(k*c.tasks+id) {
						panic("bad token")
					}
					value++
					if id == c.tasks-1 && k == c.iterations-1 {
						checksum = value
					} else {
						channels[(id+1)%c.tasks] <- value
					}
				}
			case "pipeline":
				if id == 0 {
					<-gate
					for k := 0; k < c.iterations; k++ {
						channels[0] <- uint64(k)
					}
				} else {
					expected := b
					for k := 0; k < c.iterations; k++ {
						value := <-channels[id-1]
						if id == c.tasks-1 {
							if value != expected {
								panic("bad pipeline value")
							}
							expected += a
							checksum += value
						} else {
							for w := 0; w < c.work; w++ {
								value = value*1664525 + 1013904223
							}
							channels[id] <- value
						}
					}
				}
			}
			done[id] <- checksum
		}()
	}
	for i := 0; i < c.tasks; i++ {
		<-ready
	}
	// The last rendezvous sender must reach its workload wait before the snapshot.
	runtime.Gosched()
	setupNS := uint64(time.Since(setup).Nanoseconds())
	if protocol {
		emit(map[string]any{"phase": "ready", "tasks": c.tasks})
		command()
	}
	cpuStart := cpu()
	begin := time.Now()
	var checksum uint64
	operations := c.iterations
	if c.workload == "parked" {
		operations = c.active
		for i := 0; i < c.active; i++ {
			id := i * c.tasks / c.active
			channels[id] <- 0
			checksum += <-done[id]
		}
	} else {
		if c.workload == "handoff" {
			operations = c.tasks * c.iterations
			channels[0] <- 0
		} else {
			gate <- struct{}{}
		}
		for i := 0; i < c.tasks; i++ {
			value := <-done[i]
			if i == c.tasks-1 {
				checksum = value
			}
		}
	}
	if c.workload != "parked" {
		finished.Wait()
	}
	elapsed := uint64(time.Since(begin).Nanoseconds())
	cpuNS := cpu() - cpuStart
	var drainNS uint64
	if c.workload == "parked" {
		begin = time.Now()
		next := 0
		for id := 0; id < c.tasks; id++ {
			if next < c.active && id == next*c.tasks/c.active {
				next++
				continue
			}
			channels[id] <- 0
			<-done[id]
		}
		finished.Wait()
		drainNS = uint64(time.Since(begin).Nanoseconds())
	}
	return result{"result", operations, checksum, elapsed, cpuNS, setupNS, drainNS, c.tasks}
}

func parse(args []string) config {
	if len(args) != 8 {
		panic("usage: workload tasks iterations capacity work payload active warmup")
	}
	nums := make([]int, 7)
	for i, arg := range args[1:] {
		n, e := strconv.ParseUint(arg, 10, 32)
		if e != nil {
			panic(e)
		}
		nums[i] = int(n)
	}
	c := config{args[0], nums[0], nums[1], nums[2], nums[3], nums[4], nums[5], nums[6]}
	if (c.workload != "handoff" && c.workload != "pipeline" && c.workload != "parked") ||
		c.tasks < 2 || c.tasks > 100000 || c.iterations < 1 || c.iterations > 10000000 ||
		c.capacity < 1 || c.capacity > 65536 || c.work > 10000 || c.payload > 65536 ||
		c.active < 1 || c.active > c.tasks || c.warmup > 1000000 ||
		c.tasks*c.payload > 256*1024*1024 || c.tasks*c.capacity > 10000000 ||
		(c.workload == "pipeline" && c.tasks < 3) ||
		(c.workload == "parked" && (c.iterations != 1 || c.warmup != 0)) ||
		c.tasks*max(c.iterations, c.warmup)*max(c.work, 1) > 100000000 {
		panic("invalid workload size")
	}
	return c
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}()
	runtime.GOMAXPROCS(1)
	c := parse(os.Args[1:])
	if c.workload != "parked" && c.warmup != 0 {
		warm := c
		warm.iterations = c.warmup
		execute(warm, false)
	}
	emit(map[string]string{"phase": "baseline"})
	command()
	emit(execute(c, true))
}
