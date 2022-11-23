package mqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jiftle/mqlite/nosql"
	"github.com/jiftle/mqlite/nosql/mgolevel"
	"github.com/jiftle/mqlite/nosql/mmongo"

	"github.com/gogf/gf/v2/frame/g"
)

const (
	CNT_DataBaseName = "mqlitedb"
)

type MqliteImpl struct {
	StoreMode       int // 0 mongodb 1 golevel
	NoSqlClient     nosql.NoSqlClient
	ctx             context.Context
	Uri             string
	Name            string
	DbName          string
	_TableSchemeDao *MetaTableSchemeDao
}

func NewClient(uri string, dbname string) *MqliteImpl {
	mode := getStoreMode(uri)
	if mode == 0 {
		return &MqliteImpl{
			StoreMode:       mode,
			Name:            CNT_DataBaseName,
			DbName:          dbname,
			Uri:             uri,
			_TableSchemeDao: NewTableSchemeDao(dbname),
			NoSqlClient:     mmongo.NewClient(uri, CNT_DataBaseName, dbname),
		}
	} else {
		return &MqliteImpl{
			StoreMode:       mode,
			Name:            CNT_DataBaseName,
			Uri:             uri,
			_TableSchemeDao: NewTableSchemeDao(CNT_DataBaseName),
			NoSqlClient:     mgolevel.NewClient(uri),
		}
	}
}
func getStoreMode(uri string) (mode int) {
	// link : "mongodb://admin:zzyq2211@192.168.2.199:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false"
	// link : "leveldb://@file(resource/store/db)"
	if strings.HasPrefix(uri, "mongodb") {
		mode = 0
	} else if strings.HasPrefix(uri, "leveldb") {
		mode = 1
	} else {
		mode = 0
	}
	return
}

func (s *MqliteImpl) Connect(timeout time.Duration) (err error) {
	ctx := s.ctx
	err = s.NoSqlClient.Connect()
	if err != nil {
		g.Log().Errorf(ctx, "connect fail, %v", err)
		return
	}
	return
}

func (s *MqliteImpl) GetKeyPrefix(tableName string) (key string) {
	key = fmt.Sprintf("%v-%v-", s.DbName, tableName)
	return
}

func (s *MqliteImpl) _CreateKey(tableName string, id uint32) (key string) {
	key = fmt.Sprintf("%v-%v-%04d", s.DbName, tableName, id)
	return
}

func (s *MqliteImpl) Insert(table string, doc interface{}) (id uint32, err error) {
	// 创建表的元数据
	if s.StoreMode == 0 {
		client, b := s.NoSqlClient.(*mmongo.MongoNewDriver)
		if !b {
			err = fmt.Errorf("类型断言失败")
			return
		}
		nid := s._TableSchemeDao.MongoGetTableNewAutoID(table, client)
		key := s._CreateKey(table, nid)
		fmt.Println(key)
		err = s.NoSqlClient.Insert(key, doc)
		if err != nil {
			return
		}
		// update seq
		s._TableSchemeDao.MongoUpdateAutoId(table, client)
		id = nid
	} else {
		client, b := s.NoSqlClient.(*mgolevel.GoLevelDriver)
		if !b {
			err = fmt.Errorf("类型断言失败")
			return
		}
		nid := s._TableSchemeDao.GoLevelGetTableNewAutoID(table, client)
		key := s._CreateKey(table, nid)
		fmt.Println(key)
		err = s.NoSqlClient.Insert(key, doc)
		if err != nil {
			return
		}
		// update seq
		s._TableSchemeDao.GoLevelUpdateAutoId(table, client)
		id = nid
	}

	return
}
func (s *MqliteImpl) FindOne(table string, id uint32, out interface{}) (err error) {
	key := s._CreateKey(table, id)
	err = s.NoSqlClient.FindOne(key, out)
	if err != nil {
		g.Log().Warningf(context.TODO(), "%v", err)
	}
	return
}
func (s *MqliteImpl) DeleteOne(table string, id uint32) (err error) {
	key := s._CreateKey(table, id)
	err = s.NoSqlClient.DeleteOne(key)
	return
}
func (s *MqliteImpl) Count(table string) (count uint32, err error) {
	key := s.GetKeyPrefix(table)
	ncount, err := s.NoSqlClient.Count(key)
	count = uint32(ncount)
	return
}
func (s *MqliteImpl) UpdateOne(table string, id uint32, doc interface{}) (err error) {
	key := s._CreateKey(table, id)
	err = s.NoSqlClient.UpdateOne(key, doc)
	return
}

func (s *MqliteImpl) FindAll(table string, out interface{}) (err error) {
	key := s.GetKeyPrefix(table)
	err = s.NoSqlClient.FindAll(key, out)
	return
}
