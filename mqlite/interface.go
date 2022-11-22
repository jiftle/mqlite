package mqlite

import "time"

type Mqlite interface {
	Connect(timeout time.Duration) (err error)
	Insert(table string, doc interface{}) (id uint32, err error)
	FindOne(table string, id uint32, out interface{}) (err error)
	DeleteOne(table string, id uint32) (err error)
	Count(table string) (count uint32, err error)
	UpdateOne(table string, id uint32, doc interface{}) (err error)
	FindAll(table string, out interface{}) (err error)
}
