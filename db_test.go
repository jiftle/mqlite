package mqlite

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gogf/gf/v2/frame/g"
)

func TestConnectDB(t *testing.T) {
	ctx := context.TODO()
	// uri := "leveldb://@file(resource/store/db)"
	uri := "mongodb://admin:zzyq2211@192.168.2.199:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false"

	// link : "mongodb://admin:zzyq2211@192.168.2.199:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false"
	// link : "leveldb://@file(resource/store/db)"

	DB = NewClient(uri)

	err := DB.Connect(5 * time.Second)
	if err != nil {
		g.Log().Warningf(ctx, "连接失败, %v", err)
		return
	}
	g.Log().Infof(ctx, "连接成功")

	type User struct {
		Name string
		Sex  bool
		Age  int
	}
	user := User{
		Name: "瓦克犀利",
		Sex:  false,
		Age:  12,
	}
	fmt.Println(user)
	id, err := DB.Insert("user1", user)
	if err != nil {
		g.Log().Warningf(ctx, "insert失败, %v", err)
		return
	}
	g.Log().Infof(ctx, "insert成功, %v", id)

	nuser := &User{}
	err = DB.FindOne("user1", id, nuser)
	fmt.Println(err, nuser)

	count, err := DB.Count("user1")
	fmt.Println("记录个数", err, count)

	DB.DeleteOne("user1", id)

	count, err = DB.Count("user1")
	fmt.Println("记录个数", err, count)
}
