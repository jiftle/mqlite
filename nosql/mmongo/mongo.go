package mmongo

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"go.mongodb.org/mongo-driver/bson"
)

type MongoNewDriver struct {
	client      *mongo.Client
	Host        string
	Timeout     time.Duration
	PoolLimit   int
	DbName      string // 数据库名称
	CollectName string
	URI         string
	Connected   bool // 连接状态
}

type kv struct {
	Key string
	Val interface{}
}

func NewClient(uri string, dbname string, collectName string) *MongoNewDriver {
	return &MongoNewDriver{
		URI:         uri,
		DbName:      dbname,
		CollectName: collectName,
	}
}

func (s *MongoNewDriver) Connect() (err error) {
	uri := s.URI
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		g.Log().Errorf(context.TODO(), "create client fail, %v", err)
		return
	}
	ctxtimeout, _ := context.WithTimeout(context.Background(), 30*time.Second)
	err = client.Connect(ctxtimeout)
	if err != nil {
		g.Log().Errorf(context.TODO(), "connect mongodb fail, %v, uri: %v", err, uri)
		return
	}
	err = client.Ping(ctxtimeout, readpref.Primary())
	if err != nil {
		g.Log().Errorf(context.TODO(), "connect mongodb fail, %v, uri: %v", err, uri)
		return
	}
	g.Log().Infof(context.TODO(), "connect mongodb success, dbinfo: %v", clientOptions.Hosts)
	s.client = client
	s.Connected = true
	return
}

func (s *MongoNewDriver) Count(key string) (count int64, err error) {
	client := s.client
	coll := client.Database(s.DbName).Collection(s.CollectName)
	filter := bson.D{}
	filter = append(filter, bson.E{
		Key:   "key",
		Value: bson.M{"$regex": primitive.Regex{Pattern: ".*" + key + ".*", Options: "i"}},
	})
	count, err = coll.CountDocuments(
		context.TODO(),
		filter,
	)
	if err != nil {
		return
	}
	return
}

func (s *MongoNewDriver) Insert(key string, doc interface{}) (err error) {
	client := s.client
	collect := client.Database(s.DbName).Collection(s.CollectName)
	tmpKV := kv{
		Key: key,
		Val: doc,
	}
	_, err = collect.InsertOne(context.Background(), tmpKV)
	if err != nil {
		return
	}
	return
}

func (s *MongoNewDriver) FindOne(key string, out interface{}) (err error) {
	client := s.client
	collect := client.Database(s.DbName).Collection(s.CollectName)
	outv := &kv{}
	filter := bson.M{
		"key": key,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	err = collect.FindOne(ctx, filter).Decode(outv)
	if err != nil {
		return
	}
	if out == nil {
		return
	}
	byt, err := bson.Marshal(outv.Val)
	if err != nil {
		return err
	}

	err = bson.Unmarshal(byt, out)
	if err != nil {
		return err
	}
	return
}

func (s *MongoNewDriver) DeleteOne(key string) error {
	client := s.client
	coll := client.Database(s.DbName).Collection(s.CollectName)
	opts := options.Delete().SetCollation(&options.Collation{
		Locale:    "en_US",
		Strength:  1,
		CaseLevel: false,
	})
	filter := bson.D{{"key", key}}
	_, err := coll.DeleteOne(context.Background(), filter, opts)
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

func (s *MongoNewDriver) UpdateOne(key string, doc interface{}) (err error) {
	err = s.FindOne(key, nil)
	if err != nil {
		return
	}
	err = s.DeleteOne(key)
	if err != nil {
		return
	}
	err = s.Insert(key, doc)
	if err != nil {
		return
	}
	return
}

func (s *MongoNewDriver) FindAll(prefix string, out interface{}) (err error) {
	client := s.client
	coll := client.Database(s.DbName).Collection(s.CollectName)
	filter := bson.D{}
	filter = append(filter, bson.E{
		Key:   "key",
		Value: bson.M{"$regex": primitive.Regex{Pattern: ".*" + prefix + ".*", Options: "i"}},
	})
	opts := options.Find().SetSort(bson.D{{"id", 1}})
	cursor, err := coll.Find(context.TODO(), filter, opts)
	if err != nil {
		return
	}

	var results []kv
	if err = cursor.All(context.TODO(), &results); err != nil {
		return
	}

	result := out
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		err = fmt.Errorf("result argument must be a slice address")
		return
	}
	slicev := resultv.Elem()
	elemt := slicev.Type().Elem()
	count := len(results)
	for _, result := range results {
		elemp := reflect.New(elemt)
		byt, err := bson.Marshal(result.Val)
		if err != nil {
			return err
		}
		err = bson.Unmarshal(byt, elemp.Interface())
		if err != nil {
			return err
		}
		slicev = reflect.Append(slicev, elemp.Elem())
	}
	resultv.Elem().Set(slicev.Slice(0, count))
	return nil
}
