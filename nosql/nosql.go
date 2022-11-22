package nosql

type NoSqlClient interface {
	Connect() (err error)
	Insert(collection string, key string, doc interface{}) (err error)
	FindOne(collection string, key string, out interface{}) error
	DeleteOne(collection string, key string) (err error)
	Count(collection string, prefix string) (count int64, err error)
	UpdateOne(collection string, key string, doc interface{}) (err error)
	FindAll(collection string, prefix string, out interface{}) (err error)
}
