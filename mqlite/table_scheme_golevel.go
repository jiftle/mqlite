package github.com/jiftle/mqlite

import (
	"fmt"
	"mqlite/nosql/mgolevel"
	"strings"

	"github.com/gogf/gf/v2/os/gtime"
)

func (s *MetaTableSchemeDao) GoLevel_tableIsExisted(tableName string, client *mgolevel.GoLevelDriver) (b bool) {
	key := s.GoLevelMeta_GetTableKey(tableName)
	err := client.FindOne(s.DbName, key, nil)
	if err != nil {
		if strings.Contains(err.Error(), "leveldb: not found") {
			b = false
			return
		}
	}
	b = true
	return
}

func (s *MetaTableSchemeDao) GoLevelMeta_CreateTableSpace(tableName string, client *mgolevel.GoLevelDriver) {
	v := MetaTableScheme{
		TableName:   tableName,
		AutoIncrSeq: 0,
	}
	createtime := gtime.Now().Layout("2006-01-02 15:04:05.000")
	v.CreateTime = createtime
	v.UpdateTime = createtime
	v.DeleteTime = createtime

	key := s.GoLevelMeta_GetTableKey(v.TableName)
	err := client.Insert(s.DbName, key, v)
	fmt.Println("create table space", tableName, err)
}

func (s *MetaTableSchemeDao) GoLevelcheck_table_meta(tableName string, client *mgolevel.GoLevelDriver) {
	if !s.GoLevel_tableIsExisted(tableName, client) {
		s.GoLevelMeta_CreateTableSpace(tableName, client)
	}
}

func (s *MetaTableSchemeDao) GoLevelGetTableNewAutoID(tableName string, client *mgolevel.GoLevelDriver) (id uint32) {
	s.GoLevelcheck_table_meta(tableName, client)

	key := s.GoLevelMeta_GetTableKey(tableName)
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

func (s *MetaTableSchemeDao) GoLevelUpdateAutoId(table string, client *mgolevel.GoLevelDriver) {
	key := s.GoLevelMeta_GetTableKey(table)
	outv := &MetaTableScheme{}
	err := client.FindOne(s.DbName, key, outv)
	if err != nil {
		return
	}
	outv.UpdateTime = gtime.Now().Layout("2006-01-02 15:03:04.000")
	outv.AutoIncrSeq = outv.AutoIncrSeq + 1
	client.UpdateOne(s.DbName, key, outv)
}

func (s *MetaTableSchemeDao) GoLevelMeta_GetTableKey(tableName string) (key string) {
	key = fmt.Sprintf("%v-%v-%v", s.DbName, s.TableName, tableName)
	return
}
