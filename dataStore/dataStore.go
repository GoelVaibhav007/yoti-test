package datastore

import (
	"sync"
	"yoti-test/persistence"
)

type KeyValueStore struct {
	mu          sync.Mutex
	data        map[string]string
	persistence *persistence.Persistence
}

func NewKeyValueStore(persistenceConfig *persistence.Persistence) *KeyValueStore {
	dataStore := &KeyValueStore{
		data:        make(map[string]string),
		persistence: persistenceConfig,
	}

	if dataStore.persistence != nil {
		dataStore.persistence.StartPersistence()
		dataStore.persistence.PopulatePersistedStore(dataStore.data)
	}

	return dataStore
}

func (kv *KeyValueStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value

	if kv.persistence != nil {
		go kv.persistence.PersistKey(key, value)
	}
}

func (kv *KeyValueStore) Get(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exists := kv.data[key]
	return value, exists
}

func (kv *KeyValueStore) Delete(key string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.data, key)

	if kv.persistence != nil {
		go kv.persistence.MarkKeyAsDeleted(key)
	}
}

func (kv *KeyValueStore) FlushDB() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data = make(map[string]string)

	if kv.persistence != nil {
		go kv.persistence.MarkKeyAsDeleted("*")
	}
}
