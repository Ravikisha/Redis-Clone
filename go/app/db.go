package main

type DB struct {
	data map[string]string
}

func MakeDB() *DB {
	return &DB{data: map[string]string{}}
}
func (db *DB) Set(key string, val string) {
	db.data[key] = val
}
func (db *DB) Get(key string) (string, bool) {
	val, found := db.data[key]
	return val, found
}
func (db *DB) Delete(key string) {
	delete(db.data, key)
}
