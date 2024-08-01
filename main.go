package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
)

var (
	store = struct {
		sync.RWMutex
		m map[string]string
	}{m: make(map[string]string)}

	ErrorNoSuchKey = errors.New("no such key")
)

type EventType byte

const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
}

type FileTransactionLogger struct {
	events      chan<- Event
	errors      <-chan error
	lastSequece uint64
	file        *os.File
}

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}

func NewFileTransactionLogger(filename string) (*FileTransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}
	return &FileTransactionLogger{file: file}, nil
}

func (l *FileTransactionLogger) Run() {
	events := make(chan Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		for e := range events {
			l.lastSequece++

			_, err := fmt.Fprintf(
				l.file,
				"%d\t%d\t%s\t%s\n",
				l.lastSequece, e.EventType, e.Key, e.Value)

			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file)
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		var e Event

		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s", &e.Sequence, &e.EventType, &e.Key, &e.Value); err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}

			if l.lastSequece >= e.Sequence {
				outError <- fmt.Errorf("transaction number out of sequece")
				return
			}

			l.lastSequece = e.Sequence

			outEvent <- e
		}

		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
			return
		}
	}()

	return outEvent, outError
}

func (l *FileTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *FileTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventPut, Key: key}
}

func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func Put(key, value string) error {
	store.Lock()
	store.m[key] = value
	store.Unlock()

	return nil
}

func Get(key string) (string, error) {
	store.RLock()
	value, ok := store.m[key]
	store.RUnlock()

	if !ok {
		return "", ErrorNoSuchKey
	}

	return value, nil
}

func Delete(key string) error {
	store.Lock()
	delete(store.m, key)
	store.Unlock()

	return nil
}

func main() {
	http.HandleFunc("PUT /v1/put", keyValuePutHandler)

	http.HandleFunc("GET /v1/key/{key}", keyValueGetHandler)

	http.HandleFunc("DELETE /v1/key/{key}", keyValueDeleteHandler)

	http.ListenAndServe(":8080", nil)
}

// curl -X PUT -d '{"key": "KEY", "value": "VALUE"}' -v http://localhost:8080/v1/put
func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	keyValue := KeyValue{}

	err = json.Unmarshal(body, &keyValue)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = Put(keyValue.Key, keyValue.Value)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// curl -X GET -v http://localhost:8080/v1/key/key
func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	value, err := Get(key)

	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))
}

// curl -X DELETE -v http://localhost:8080/v1/key/KEY
func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	err := Delete(key)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
