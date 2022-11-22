package mmongo

import (
	_ "fmt"
	"testing"
)

func TestConnect(t *testing.T) {
	uri := "mongodb://admin:zzyq2211@192.168.2.199:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false"
	mgo := NewClient(uri)
	err := mgo.Connect()
	if err != nil {
		t.Fatal(err)
	}

	type St struct {
		PublicKey  string `bson:"publickey"`
		PrivateKey string `bson:"privatekey"`
	}

	sst := St{
		PublicKey:  "1122",
		PrivateKey: "999999999999",
	}
	err = mgo.Insert("collect", "st", sst)
	if err != nil {
		t.Fatal(err)
	}

	// nst := St{}
	// err = MongoNew.FindOne(ctx, "sysparam", "10001", &nst)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// t.Log("nst", nst)

	// err = MongoNew.DeleteOne(ctx, "sysparam", "10001")
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// t.Log("delete one")

	// err = MongoNew.FindOne(ctx, "sysparam", "10001", &nst)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// t.Log("nst", nst)
}
