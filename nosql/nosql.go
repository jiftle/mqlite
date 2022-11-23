package nosql

type NoSqlClient interface {
	Connect() (err error)
	Insert(key string, doc interface{}) (err error)
	FindOne(key string, out interface{}) error
	DeleteOne(key string) (err error)
	Count(prefix string) (count int64, err error)
	UpdateOne(key string, doc interface{}) (err error)
	FindAll(prefix string, out interface{}) (err error)
}
