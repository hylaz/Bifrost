package mongo

import (
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (c *MongoInput) OpLogPosition2GTID(p *primitive.Timestamp) string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("{\"T\":%d,\"I\":%d}", p.T, p.I)
}

func (c *MongoInput) GTID2OpLogPosition(GTID string) *primitive.Timestamp {
	if GTID == "" {
		return nil
	}
	var p primitive.Timestamp
	err := json.Unmarshal([]byte(GTID), &p)
	if err != nil {
		logrus.Printf("[ERROR] %s GTID:%s GTID2OpLogPosition err:%+v", c.inputInfo.DbName, GTID, err)
		return nil
	}
	return &p
}
