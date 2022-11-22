package mqlite

// 表空间的结构
type MetaTableSchemeDao struct {
	DbName    string // 数据库名称
	TableName string // 表名
}

type MetaTableScheme struct {
	TableName   string // 表名，最长64个字符
	AutoIncrSeq int    // 自动增长序列号，从1开始
	CreateTime  string // 创建时间
	UpdateTime  string // 更新时间
	DeleteTime  string // 删除时间
}

func NewTableSchemeDao(name string) *MetaTableSchemeDao {
	return &MetaTableSchemeDao{
		DbName:    name,
		TableName: "MetaTableScheme",
	}
}
