package mgolevel

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"go.mongodb.org/mongo-driver/bson"
)

type GoLevelDriver struct {
	LevelDB   *leveldb.DB
	DbName    string // 数据库名称
	URI       string
	StorePath string
	Connected bool
}

type GoLevelOptions struct {
	StorePath string
}

func NewClient(uri string) *GoLevelDriver {
	options := paserUri(uri)
	if options != nil {
		return &GoLevelDriver{
			URI:       uri,
			DbName:    "mqlitedb",
			StorePath: options.StorePath,
		}
	} else {
		return &GoLevelDriver{
			URI:       uri,
			DbName:    "mqlitedb",
			StorePath: "store",
		}
	}
}

func paserUri(uri string) (opton *GoLevelOptions) {
	// link : "leveldb://@file(resource/store/db)"
	st := strings.Index(uri, "@file(")
	ed := strings.LastIndex(uri, ")")
	if st != -1 && ed != -1 {
		path := uri[st+len("@file(") : ed]
		opton = &GoLevelOptions{
			StorePath: path,
		}
		return
	}
	return
}

func (s *GoLevelDriver) Connect() (err error) {
	// 创建或打开一个数据库
	dbFile := s.StorePath
	db, err := leveldb.OpenFile(dbFile, nil)
	if err != nil {
		g.Log().Errorf(context.TODO(), "leveldb connect fail, %v", err)
		return
	}

	s.LevelDB = db
	s.Connected = true
	return
}

// 数据库，集合
func (s *GoLevelDriver) Insert(collection string, key string, doc interface{}) (err error) {
	var k string
	var bk, bv []byte
	var val []byte

	// key
	k = fmt.Sprintf("%s-%s", collection, key)
	g.Log().Infof(context.TODO(), "[leveldb] insert, %v", k)

	val, err = bson.Marshal(doc)
	if err != nil {
		return fmt.Errorf("interface{} convert []byte fail, %v", err)
	}

	bk = []byte(k)
	bv = val
	err = s.LevelDB.Put(bk, bv, nil)
	if err != nil {
		return fmt.Errorf("put key fail, %v", err)
	}

	b, err := s.LevelDB.Get(bk, nil)
	if err != nil {
		return fmt.Errorf("get key fail, %v", err)
	}
	if len(bv) != len(b) {
		return fmt.Errorf("[leveldb] 写入数据和读取数据不一致，原数据长度=%v, 读到数据长度=%v", len(bv), len(b))
	}

	return nil
}

func (s *GoLevelDriver) FindOne(collection string, key string, out interface{}) error {
	k := fmt.Sprintf("%s-%s", collection, key)
	g.Log().Infof(context.TODO(), "[leveldb] find, %v", k)
	byt, err := s.LevelDB.Get([]byte(k), nil)
	if err != nil {
		return fmt.Errorf("get value fail, %v", err)
	}

	err = bson.Unmarshal(byt, out)
	if err != nil {
		return fmt.Errorf("[]byte unmarshal fail, %v", err)
	}
	return nil
}

func (s *GoLevelDriver) UpdateOne(collection string, key string, doc interface{}) error {
	var k string
	var bk, bv []byte
	var val []byte

	// key
	k = fmt.Sprintf("%s-%s", collection, key)
	g.Log().Infof(context.TODO(), "[leveldb] update, %v", k)

	val, err := bson.Marshal(doc)
	if err != nil {
		return fmt.Errorf("interface convert []byte fail, %v", err)
	}

	bk = []byte(k)
	bv = val
	err = s.LevelDB.Put(bk, bv, nil)
	if err != nil {
		return fmt.Errorf("put key fail, %v", err)
	}

	b, err := s.LevelDB.Get(bk, nil)
	if err != nil {
		return fmt.Errorf("get key fail, %v", err)
	}
	if len(bv) != len(b) {
		return fmt.Errorf("写入数据和读取数据不一致，原数据长度=%v, 读到数据长度=%v", len(bv), len(b))
	}

	return nil
}

func (s *GoLevelDriver) FindAll(collection string, pre string, out interface{}) (err error) {
	k := collection + "-" + pre
	g.Log().Infof(context.TODO(), "[leveldb] findAll, %v", k)
	m := make(map[string][]byte)
	iter := s.LevelDB.NewIterator(util.BytesPrefix([]byte(k)), nil)
	for iter.Next() {
		v := make([]byte, len(iter.Value()))
		copy(v, iter.Value())
		m[string(iter.Key())] = v
	}

	iter.Release()
	err = iter.Error()
	if err != nil {
		return
	}
	//g.Log().Infof("[leveldb] findAll, out: %v", m)

	result := out
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		err = fmt.Errorf("result argument must be a slice address")
		return
	}
	slicev := resultv.Elem()
	elemt := slicev.Type().Elem()

	count := len(m)
	var arKey []string
	for kk, outvByt := range m {
		elemp := reflect.New(elemt)
		err = bson.Unmarshal(outvByt, elemp.Interface())
		if err != nil {
			return
		}
		slicev = reflect.Append(slicev, elemp.Elem())
		arKey = append(arKey, kk)
	}
	resultv.Elem().Set(slicev.Slice(0, count))
	g.Log().Infof(context.TODO(), "[leveldb] findAll, count: %v, keys: %#v", len(m), arKey)
	return nil
}

func (s *GoLevelDriver) FindAllSort(ctx context.Context, collection string, pre string, out interface{}) error {
	k := collection + "-" + pre
	g.Log().Infof(ctx, "[leveldb] findAllSort, %v", k)
	m := make(map[string][]byte)
	iter := s.LevelDB.NewIterator(util.BytesPrefix([]byte(k)), nil)
	for iter.Next() {
		v := make([]byte, len(iter.Value()))
		copy(v, iter.Value())
		m[string(iter.Key())] = v
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		return err
	}
	//g.Log().Infof("[leveldb] findAllSort, out: %v", m)

	// 排序
	var x []string

	for k, _ := range m {
		x = append(x, k)
	}

	sort.Strings(x)
	//sort.Sort(sort.Reverse(sort.StringSlice(x)))

	result := out
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}
	slicev := resultv.Elem()
	elemt := slicev.Type().Elem()

	count := len(m)
	var arKey []string
	for i := 0; i < count; i++ {
		outvByt := m[x[i]]
		elemp := reflect.New(elemt)
		err = bson.Unmarshal(outvByt, elemp.Interface())
		if err != nil {
			return err
		}
		slicev = reflect.Append(slicev, elemp.Elem())
		kk := x[i]
		arKey = append(arKey, kk)
	}
	resultv.Elem().Set(slicev.Slice(0, count))
	//g.Log().Infof("[leveldb] findAllSort, --out: %v", out)
	g.Log().Infof(ctx, "[leveldb] findAll, count: %v, keys: %#v", len(m), arKey)
	return nil
}

func (s *GoLevelDriver) DeleteOne(collection string, key string) error {
	k := fmt.Sprintf("%s-%s", collection, key)
	g.Log().Infof(context.TODO(), "[leveldb] delete, %v", k)
	err := s.LevelDB.Delete([]byte(k), nil)
	if err != nil {
		return err
	}
	g.Log().Infof(context.TODO(), "[leveldb] delete success, %v", k)
	return nil
}

func (s *GoLevelDriver) DeleteAll(ctx context.Context, collection string, pre string) error {
	k := collection + "-" + pre
	g.Log().Infof(ctx, "[leveldb] deleteAll, %v", k)
	m := make(map[string][]byte)
	iter := s.LevelDB.NewIterator(util.BytesPrefix([]byte(k)), nil)
	for iter.Next() {
		v := make([]byte, len(iter.Value()))
		copy(v, iter.Value())
		m[string(iter.Key())] = v
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		return err
	}

	// 遍历删除
	var arKey []string
	for kk, _ := range m {
		err = s.LevelDB.Delete([]byte(kk), nil)
		if err != nil {
			return nil
		}
		arKey = append(arKey, kk)
	}

	g.Log().Infof(ctx, "[leveldb] deleteAll, count: %v, keys: %#v", len(m), arKey)

	return nil
}
func (s *GoLevelDriver) Count(collection string, prefix string) (count int64, err error) {
	k := collection + "-" + prefix
	g.Log().Infof(context.TODO(), "[leveldb] Count, %v", k)
	m := make(map[string][]byte)
	iter := s.LevelDB.NewIterator(util.BytesPrefix([]byte(k)), nil)
	for iter.Next() {
		v := make([]byte, len(iter.Value()))
		copy(v, iter.Value())
		m[string(iter.Key())] = v
	}

	iter.Release()
	err = iter.Error()
	if err != nil {
		return
	}
	count = int64(len(m))
	g.Log().Infof(context.TODO(), "[leveldb] Count, count: %v", count)
	return
}
