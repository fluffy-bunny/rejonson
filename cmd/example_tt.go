package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fluffy-bunny/rejonson/v8"
	"github.com/go-redis/redis/v8"
)

func main() {
	goRedisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	goRedisClient.Del(context.Background(), "go-redis-cmd", "rejson-cmd", "rejson-cmd-pipeline", "go-redis-pipeline-command")
	_ = goRedisClient.Close()

	ExampleExtendClient()
	ExampleExtendPipeline()
}

func ExampleExtendClient() {
	goRedisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	client := rejonson.ExtendClient(goRedisClient)
	defer client.Close()

	arr := []interface{}{"hello", "world", 1, map[string]interface{}{"key": 12}}
	js, err := json.Marshal(arr)
	if err != nil {
		// handle
	}
	// redis "native" command
	client.Set(context.Background(), "go-redis-cmd", "hello", time.Second)
	client.JsonSet(context.Background(), "rejson-cmd", ".", string(js))

	// int command
	arrLen, err := client.JsonArrLen(context.Background(), "rejson-cmd", ".").Result()
	if err != nil {
		// handle
	}

	fmt.Printf("Array length: %d", arrLen)
	// Output: Array length: 4
}

func ExampleExtendPipeline() {
	goRedisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	client := rejonson.ExtendClient(goRedisClient)

	pipeline := client.Pipeline()
	pipeline.JsonSet(context.Background(), "rejson-cmd-pipeline", ".", "[10]")
	pipeline.JsonNumMultBy(context.Background(), "rejson-cmd-pipeline", "[0]", 10)
	pipeline.Set(context.Background(), "go-redis-pipeline-command", "hello from go-redis", time.Second)

	_, err := pipeline.Exec(context.Background())
	if err != nil {
		// handle error
	}
	jsonString, err := client.JsonGet(context.Background(), "rejson-cmd-pipeline").Result()
	if err != nil {
		// handle error
	}

	fmt.Printf("Array %s", jsonString)

	// Output: Array [100]
}
