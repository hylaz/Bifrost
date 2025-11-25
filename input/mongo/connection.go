package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"time"
)

func CreateMongoClient(uri string, ctx context.Context) (*mongo.Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	registry := bson.NewRegistry()
	registry.RegisterTypeMapEntry(bson.TypeDateTime, reflect.TypeOf(time.Time{}))
	clientOptions := options.Client()
	clientOptions.SetRegistry(registry)
	clientOptions.ApplyURI(uri)
	timeOutCtx, _ := context.WithTimeout(ctx, 15*time.Second)
	client, err := mongo.Connect(timeOutCtx, clientOptions)
	if err != nil {
		return nil, err
	}
	return client, nil
}
