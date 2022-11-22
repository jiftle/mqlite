package mqlite

import (
	"errors"
	"fmt"

	"github.com/jiftle/mqlite/nosql/mmongo"

	"github.com/gogf/gf/v2/os/gtime"
	"go.mongodb.org/mongo-driver/mongo"
)

func (s *MetaTableSchemeDao) Mongo_tableIsExisted(tableName string, client *mmongo.MongoNewDriver) (b bool) {
	key := s.MongoMeta_GetTableKey(tableName)
	err := client.FindOne(s.DbName, key, nil)
	fmt.Println("find", err)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			b = false
			return
		}
	}
	b = true
	return
}

func (s *MetaTableSchemeDao) MongoMeta_CreateTableSpace(tableName string, client *mmongo.MongoNewDriver) {
	v := MetaTableScheme{
		TableName:   tableName,
		AutoIncrSeq: 0,
	}
	createtime := gtime.Now().Layout("2006-01-02 15:04:05.000")
	v.CreateTime = createtime
	v.UpdateTime = createtime
	v.DeleteTime = createtime

	key := s.MongoMeta_GetTableKey(v.TableName)
	err := client.Insert(s.DbName, key, v)
	fmt.Println("create table space", tableName, err)
}

func (s *MetaTableSchemeDao) Mongocheck_table_meta(tableName string, client *mmongo.MongoNewDriver) {
	if !s.Mongo_tableIsExisted(tableName, client) {
		s.MongoMeta_CreateTableSpace(tableName, client)
	}
}

func (s *MetaTableSchemeDao) MongoGetTableNewAutoID(tableName string, client *mmongo.MongoNewDriver) (id uint32) {
	s.Mongocheck_table_meta(tableName, client)

	key := s.MongoMeta_GetTableKey(tableName)
	outv := &MetaTableScheme{}
	err := client.FindOne(s.DbName, key, outv)
	fmt.Println(err, outv)
	if err != nil {
		id = 1
		return
	}
	id = uint32(outv.AutoIncrSeq) + 1
	return
}

func (s *MetaTableSchemeDao) MongoUpdateAutoId(table string, client *mmongo.MongoNewDriver) {
	key := s.MongoMeta_GetTableKey(table)
	outv := &MetaTableScheme{}
	err := client.FindOne(s.DbName, key, outv)
	if err != nil {
		return
	}
	outv.UpdateTime = gtime.Now().Layout("2006-01-02 15:03:04.000")
	outv.AutoIncrSeq = outv.AutoIncrSeq + 1
	client.UpdateOne(s.DbName, key, outv)
}

func (s *MetaTableSchemeDao) MongoMeta_GetTableKey(tableName string) (key string) {
	key = fmt.Sprintf("%v-%v-%v", s.DbName, s.TableName, tableName)
	return
}
