package rejonson

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

var (
	redisHost     = "localhost:6379"
	redisPassword = "eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81"
)

var (
	letterRunes        = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	baseJsonTestObject = `
{
  "keyA": 56,
  "keyB": "some string",
  "numbersArray": [
    1,
    2,
    3,
    4,
    5
  ],
  "strArray": [
    "a",
    "b",
    "c"
  ]
}
`
	client           *Client
	redisTestsPrefix string
)

func concatKey(key string) string {
	return redisTestsPrefix + key
}

func insertBaseJsonToRedis(key string, t *testing.T) (success bool) {

	return assert.NoError(t, client.JsonSet(context.Background(), key, ".", baseJsonTestObject).Err())
}

func getBaseJsonFromRedis(key string) (map[string]interface{}, error) {
	b, err := client.JsonGet(context.Background(), key).Bytes()

	if err != nil {
		return nil, err
	}
	var data map[string]interface{}
	return data, json.Unmarshal(b, &data)
}

func getBaseJsonTestObject() map[string]interface{} {
	var res map[string]interface{}
	if err := json.Unmarshal([]byte(baseJsonTestObject), &res); err != nil {
		panic(fmt.Errorf("corrupted test base json object %s -  %w", baseJsonTestObject, err))
	}
	return res
}

func assertEqualJson(t *testing.T, redisKey string, expected interface{}) {
	actual, err := getBaseJsonFromRedis(redisKey)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, expected, actual)
}

// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestMain(m *testing.M) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		return
	}
	getBaseJsonTestObject()
	rand.Seed(time.Now().UnixNano())
	if v, ok := os.LookupEnv("REJONSON_REDIS_HOST"); ok {
		redisHost = v
	}

	if v, ok := os.LookupEnv("REJONSON_REDIS_PASSWORD"); ok {
		redisPassword = v
	}

	client = ExtendClient(redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: redisPassword,
	}))

	if err := client.Ping(context.Background()).Err(); err != nil {
		panic(fmt.Errorf("unable to ping redis %w", err))
	}
	defer client.Close()
	// clear resources
	defer func() {
		if keys, err := client.Keys(context.Background(), redisTestsPrefix+"*").Result(); err != nil {
			client.Del(context.Background(), keys...)
		}
	}()

	m.Run()

}

func TestRedisProcessor_JsonDel(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	originalJs := getBaseJsonTestObject()

	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		return
	}

	delRes, err := client.JsonDel(context.Background(), key, "keyA").Result()
	if !assert.NoError(t, err) {
		return
	}

	assert.NotEqual(t, 1, delRes)

	delete(originalJs, "keyA")
	assertEqualJson(t, key, originalJs)
}

func TestRedisProcessor_JsonGet(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	originalJS := getBaseJsonTestObject()
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		t.FailNow()
	}

	// check first that the entire object returns
	getRes, err := client.JsonGet(context.Background(), key).Bytes()
	if !assert.NoError(t, err) {
		return
	}
	assertEqualJson(t, key, originalJS)

	// check that nested object returned
	getRes, err = client.JsonGet(context.Background(), key, "numbersArray").Bytes()
	var theMap interface{}
	if assert.NoError(t, err) && assert.NoError(t, json.Unmarshal(getRes, &theMap)) {
		assert.Equal(t, originalJS["numbersArray"], theMap)
	}
}

func TestRedisProcessor_JsonSet(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	setRes, err := client.JsonSet(context.Background(), key, ".", baseJsonTestObject).Result()
	if assert.NoError(t, err) {
		assert.Equal(t, "OK", setRes)
	}
}

func TestRedisProcessor_JsonMGet(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	originalJS := getBaseJsonTestObject()
	keyA := concatKey(randStringRunes(32))
	keyB := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), keyA, keyB)

	if !insertBaseJsonToRedis(keyA, t) {
		return
	}

	if !insertBaseJsonToRedis(keyB, t) {
		return
	}

	mGetRes, err := client.JsonMGet(context.Background(), keyA, keyB, "strArray").Result()
	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, mGetRes, 2)

	for _, m := range mGetRes {
		var data interface{}
		if assert.NoError(t, json.Unmarshal([]byte(m), &data)) {
			assert.Equal(t, originalJS["strArray"], data)
		}
	}
}

func TestRedisProcessor_JsonType(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		return
	}

	typeRes, err := client.JsonType(context.Background(), key, "").Result()
	if assert.NoError(t, err) {
		assert.Equal(t, "object", typeRes)
	}

	typeRes, err = client.JsonType(context.Background(), key, "keyB").Result()
	if assert.NoError(t, err) {
		assert.Equal(t, "string", typeRes)
	}
}

func TestRedisProcessor_JsonNumIncrBy(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		return
	}

	incRes, err := client.JsonNumIncrBy(context.Background(), key, "keyA", 4).Result()
	if assert.NoError(t, err) {
		assert.Equal(t, "60", incRes)
	}

}

func TestRedisProcessor_JsonNumMultBy(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		return
	}

	multRes, err := client.JsonNumMultBy(context.Background(), key, "numbersArray[1]", 4).Result()
	if assert.NoError(t, err) {
		assert.Equal(t, "8", multRes)
	}
}

func TestRedisProcessor_JsonStrAppend(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		return
	}

	strAppRes, err := client.JsonStrAppend(context.Background(), key, "keyB", " \"hello\"").Result()

	if assert.NoError(t, err) {
		assert.Equal(t, 16, int(strAppRes))
	}
}

func TestRedisProcessor_JsonStrLen(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		return
	}

	strLenRes, err := client.JsonStrLen(context.Background(), key, "keyB").Result()
	if assert.NoError(t, err) {
		assert.Equal(t, len("some string"), int(strLenRes))
	}
}

func TestRedisProcessor_JsonArrAppend(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		return
	}

	arrAppendRes, err := client.JsonArrAppend(context.Background(), key, "numbersArray", 12).Result()
	if assert.NoError(t, err) {
		assert.Equal(t, 6, int(arrAppendRes))
	}
}

func TestRedisProcessor_JsonArrIndex(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	jsn := make([]interface{}, 0, 100)

	for i := 0; i < 100; i++ {
		jsn = append(jsn, i)
	}
	b, err := json.Marshal(jsn)
	if !assert.NoError(t, err) {
		return
	}

	if !assert.NoError(t, client.JsonSet(context.Background(), key, ".", string(b)).Err()) {
		return
	}

	arrIndexRes, err := client.JsonArrIndex(context.Background(), key, ".", 5).Result()
	if assert.NoError(t, err) {
		assert.Equal(t, 5, int(arrIndexRes))
	}

	arrIndexRes, err = client.JsonArrIndex(context.Background(), key, ".", 5, 10, 90).Result()
	if assert.NoError(t, err) {
		assert.Equal(t, -1, int(arrIndexRes))
	}

}

func TestRedisProcessor_JsonArrInsert(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		return
	}

	arrInsertRes, err := client.JsonArrInsert(context.Background(), key, "numbersArray", 1, "2").Result()
	if assert.NoError(t, err) {
		assert.Equal(t, 6, int(arrInsertRes))
	}
}

func TestRedisProcessor_JsonArrLen(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		return
	}

	arrLenRes, err := client.JsonArrLen(context.Background(), key, "numbersArray").Result()
	if assert.NoError(t, err) {
		assert.Equal(t, 5, int(arrLenRes))
	}
}

func TestRedisProcessor_JsonArrPop(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		return
	}

	arrPopRes, err := client.JsonArrPop(context.Background(), key, "numbersArray", 1).Result()
	if assert.NoError(t, err) {
		assert.Equal(t, "2", arrPopRes)
	}
}

func TestRedisProcessor_JsonArrTrim(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		return
	}

	arrTrimRes, err := client.JsonArrTrim(context.Background(), key, "numbersArray", 1, 3).Result()
	if assert.NoError(t, err) {
		assert.Equal(t, 3, int(arrTrimRes))
	}

	trimArr, err := client.JsonGet(context.Background(), key, "numbersArray").Result()
	if assert.NoError(t, err) {
		var arr []float64
		if assert.NoError(t, json.Unmarshal([]byte(trimArr), &arr)) {
			assert.Equal(t, []float64{2, 3, 4}, arr)
		}
	}

}

func TestRedisProcessor_JsonObjKeys(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	originalJS := getBaseJsonTestObject()
	keys := make([]string, 0, len(originalJS))
	for k := range originalJS {
		keys = append(keys, k)
	}

	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		return
	}

	objKeysRes, err := client.JsonObjKeys(context.Background(), key, ".").Result()
	if assert.NoError(t, err) {
		sort.Strings(keys)
		sort.Strings(objKeysRes)
		assert.Equal(t, keys, objKeysRes)
	}
}

func TestRedisProcessor_JsonObjLen(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	originalJS := getBaseJsonTestObject()
	key := concatKey(randStringRunes(32))
	defer client.Del(context.Background(), key)

	if !insertBaseJsonToRedis(key, t) {
		t.FailNow()
	}

	objLenRes, err := client.JsonObjLen(context.Background(), key, ".").Result()
	if assert.NoError(t, err) {
		assert.Equal(t, len(originalJS), int(objLenRes))
	}
}

func TestClient_Pipeline(t *testing.T) {
	appEnv := os.Getenv("APPLICATION_ENVIRONMENT")
	if appEnv != "Development" {
		t.Skip("skipping redis tests")
	}
	allKeys := make([]string, 0)
	pipeline := client.Pipeline()

	for i := 0; i < 10; i++ {
		key := concatKey(randStringRunes(32))
		pipeline.JsonSet(context.Background(), key, ".", baseJsonTestObject)
		allKeys = append(allKeys, key)
	}

	// here we expected that delete counter will be 0
	delRes, err := client.Del(context.Background(), allKeys...).Result()
	if assert.NoError(t, err) {
		assert.Equal(t, 0, int(delRes))
	}

	_, err = pipeline.Exec(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	// now we expect deleted count to be same as allKeysLength
	delRes, err = client.Del(context.Background(), allKeys...).Result()
	if assert.NoError(t, err) {
		assert.Equal(t, len(allKeys), int(delRes))
	}
}
