// Package fakes implements only the engine surfaces needed by lifecycle tests.
// Its ping protocol is deliberately not the llama.cpp or vLLM tensor protocol.
package fakes

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

func Run(kind string) {
	if err := run(kind); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(kind string) error {
	address := net.JoinHostPort(argument("--host"), argument("--port"))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer listener.Close()
	if kind == "rpc" {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return err
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				var ping [4]byte
				if _, err := io.ReadFull(conn, ping[:]); err == nil && string(ping[:]) == "ping" {
					_, _ = conn.Write([]byte("pong"))
				}
			}()
		}
	}
	headless := hasArgument("--headless")
	if kind == "llama" && (len(strings.Split(argument("--rpc"), ",")) != 2 ||
		len(strings.Split(argument("--tensor-split"), ",")) != 2) {
		return fmt.Errorf("llama head requires two RPC workers and a two-node tensor split")
	}
	if kind == "vllm" {
		if argument("--nnodes") != "2" || argument("--master-addr") == "" ||
			argument("--pipeline-parallel-size") != "2" || argument("--tensor-parallel-size") != "1" ||
			(headless && argument("--node-rank") != "1") || (!headless && argument("--node-rank") != "0") ||
			os.Getenv("VLLM_HOST_IP") == "" {
			return fmt.Errorf("invalid vLLM distributed launch arguments")
		}
	}
	checkWorkers := func() error {
		if kind == "llama" {
			for _, worker := range strings.Split(argument("--rpc"), ",") {
				if err := pingRPC(worker); err != nil {
					return err
				}
			}
			return nil
		}
		return pingVLLM(os.Getenv("MAISTR0_FAKE_VLLM_WORKER"))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		if !headless {
			if err := checkWorkers(); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /worker", func(w http.ResponseWriter, _ *http.Request) {
		if !headless {
			http.Error(w, "not a headless worker", http.StatusConflict)
			return
		}
		_, _ = io.WriteString(w, "pong")
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if headless {
			http.Error(w, "headless worker cannot serve chat", http.StatusNotFound)
			return
		}
		var request struct {
			Model    string `json:"model"`
			Stream   bool   `json:"stream"`
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Model != "fake-sharded" ||
			len(request.Messages) != 1 || request.Messages[0].Content != "prove sharding" {
			http.Error(w, "unexpected chat payload", http.StatusBadRequest)
			return
		}
		if err := checkWorkers(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("X-Fake-Engine", kind)
		if request.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"shard-process-ok\"}}]}\n\ndata: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"shard-process-ok"}}]}`)
	})
	return (&http.Server{Handler: mux, ReadHeaderTimeout: time.Second}).Serve(listener)
}

func pingRPC(address string) error {
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	if _, err := io.WriteString(conn, "ping"); err != nil {
		return err
	}
	var pong [4]byte
	if _, err := io.ReadFull(conn, pong[:]); err != nil {
		return err
	}
	if string(pong[:]) != "pong" {
		return fmt.Errorf("unexpected worker response %q", pong)
	}
	return nil
}

func pingVLLM(endpoint string) error {
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(endpoint + "/worker")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("headless worker returned %s", response.Status)
	}
	return nil
}

func argument(name string) string {
	for i := 1; i+1 < len(os.Args); i++ {
		if os.Args[i] == name {
			return os.Args[i+1]
		}
	}
	return ""
}

func hasArgument(name string) bool {
	for _, arg := range os.Args[1:] {
		if arg == name {
			return true
		}
	}
	return false
}
