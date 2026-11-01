package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"time"
)

func main() {
	writePID(os.Getenv("MAISTR0_FAKE_ENGINE_PID_FILE"))
	if path := os.Getenv("MAISTR0_FAKE_ENGINE_ENV_FILE"); path != "" {
		_ = os.WriteFile(path, []byte(os.Getenv("VLLM_HOST_IP")), 0o600)
	}
	switch os.Getenv("MAISTR0_FAKE_ENGINE_MODE") {
	case "http":
		delay, _ := strconv.Atoi(os.Getenv("MAISTR0_FAKE_ENGINE_START_DELAY_MS"))
		time.Sleep(time.Duration(delay) * time.Millisecond)
		startHTTP()
		waitForever()
	case "crash":
		startHTTP()
		delay, _ := strconv.Atoi(os.Getenv("MAISTR0_FAKE_ENGINE_EXIT_AFTER_MS"))
		time.Sleep(time.Duration(delay) * time.Millisecond)
		os.Exit(17)
	case "never-ready":
		waitForever()
	case "tcp":
		listener, err := net.Listen("tcp", net.JoinHostPort(argument("--host"), argument("--port")))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		defer listener.Close()
		for {
			connection, err := listener.Accept()
			if err == nil {
				_ = connection.Close()
			}
		}
	case "tree":
		command := exec.Command(os.Args[0])
		command.Env = childEnvironment(os.Environ())
		if err := command.Start(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
		writePID(os.Getenv("MAISTR0_FAKE_ENGINE_CHILD_PID_FILE"), command.Process.Pid)
		startHTTP()
		waitForever()
	case "child":
		waitForever()
	default:
		fmt.Fprintln(os.Stderr, "unknown fake engine mode")
		os.Exit(4)
	}
}

func startHTTP() {
	port := argument("--port")
	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", port))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(5)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, _ *http.Request) {
		state := struct {
			Models      []string `json:"models"`
			Unavailable bool     `json:"unavailable,omitempty"`
		}{}
		if path := os.Getenv("MAISTR0_FAKE_ENGINE_MODELS_FILE"); path != "" {
			data, err := os.ReadFile(path)
			if err != nil {
				http.Error(w, "model inventory unavailable", http.StatusServiceUnavailable)
				return
			}
			if err := json.Unmarshal(data, &state); err != nil {
				http.Error(w, "invalid model inventory", http.StatusInternalServerError)
				return
			}
		}
		if state.Unavailable {
			http.Error(w, "model inventory unavailable", http.StatusServiceUnavailable)
			return
		}
		models := make([]struct {
			ID string `json:"id"`
		}, 0, len(state.Models))
		for _, model := range state.Models {
			models = append(models, struct {
				ID string `json:"id"`
			}{ID: model})
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}{Data: models}); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	})
	go func() {
		_ = http.Serve(listener, mux)
	}()
}

func argument(name string) string {
	for index := 0; index+1 < len(os.Args); index++ {
		if os.Args[index] == name {
			return os.Args[index+1]
		}
	}
	return ""
}

func writePID(path string, pids ...int) {
	if path == "" {
		return
	}
	pid := os.Getpid()
	if len(pids) != 0 {
		pid = pids[0]
	}
	_ = os.WriteFile(path, []byte(strconv.Itoa(pid)), 0o600)
}

func waitForever() {
	interrupted := make(chan os.Signal, 1)
	signal.Notify(interrupted, os.Interrupt)
	<-interrupted
}

func childEnvironment(environment []string) []string {
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if len(entry) >= len("MAISTR0_FAKE_ENGINE_MODE=") && entry[:len("MAISTR0_FAKE_ENGINE_MODE=")] == "MAISTR0_FAKE_ENGINE_MODE=" {
			continue
		}
		if len(entry) >= len("MAISTR0_FAKE_ENGINE_PID_FILE=") && entry[:len("MAISTR0_FAKE_ENGINE_PID_FILE=")] == "MAISTR0_FAKE_ENGINE_PID_FILE=" {
			continue
		}
		result = append(result, entry)
	}
	return append(result, "MAISTR0_FAKE_ENGINE_MODE=child")
}
