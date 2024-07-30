package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
)

var (
	store = struct {
		sync.RWMutex
		m map[string]string
	}{m: make(map[string]string)}

	ErrorNoSuchKey = errors.New("no such key")
)

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
