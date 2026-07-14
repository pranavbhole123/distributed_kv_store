package store

// this will have interface of the store 

type Store interface{
	// we need three methods
	Get(key string) (string , error) 
	Set(key string, value string) error
	Delete(key string) error 
}