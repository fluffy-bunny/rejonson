package rejonson

import (
	"context"

	"github.com/go-redis/redis/v8"
)

type redisProcessor struct {
	Process func(ctx context.Context, cmd redis.Cmder) error
}

/*
Client is an extended redis.Client, stores a pointer to the original redis.Client
*/
type Client struct {
	*redis.Client
	*redisProcessor
}

/*
Pipeline is an extended redis.Pipeline, stores a pointer to the original redis.Pipeliner
*/
type Pipeline struct {
	redis.Pipeliner
	*redisProcessor
}

func (cl *Client) Pipeline() *Pipeline {
	pip := cl.Client.Pipeline()
	return ExtendPipeline(pip)
}

func (cl *Client) TXPipeline() *Pipeline {
	pip := cl.Client.TxPipeline()
	return ExtendPipeline(pip)
}
func (pl *Pipeline) Pipeline() *Pipeline {
	pip := pl.Pipeliner.Pipeline()
	return ExtendPipeline(pip)
}

/*
JsonDel

returns intCmd -> deleted 1 or 0
read more: https://oss.redislabs.com/rejson/commands/#jsondel
*/
func (cl *redisProcessor) JsonDel(ctx context.Context, key, path string) *redis.IntCmd {
	return jsonDelExecute(cl, ctx, key, path)
}

/*
JsonGet

Possible args:

(Optional) INDENT + indent-string
(Optional) NEWLINE + line-break-string
(Optional) SPACE + space-string
(Optional) NOESCAPE
(Optional) path ...string

returns stringCmd -> the JSON string
read more: https://oss.redislabs.com/rejson/commands/#jsonget
*/
func (cl *redisProcessor) JsonGet(ctx context.Context, key string, args ...interface{}) *redis.StringCmd {
	return jsonGetExecute(cl, ctx, append([]interface{}{key}, args...)...)
}

/*
jsonSet

Possible args:
(Optional)
*/
func (cl *redisProcessor) JsonSet(ctx context.Context, key, path, json string, args ...interface{}) *redis.StatusCmd {
	return jsonSetExecute(cl, ctx, append([]interface{}{key, path, json}, args...)...)
}

func (cl *redisProcessor) JsonMGet(ctx context.Context, key string, args ...interface{}) *redis.StringSliceCmd {
	return jsonMGetExecute(cl, ctx, append([]interface{}{key}, args...)...)
}

func (cl *redisProcessor) JsonType(ctx context.Context, key, path string) *redis.StringCmd {
	return jsonTypeExecute(cl, ctx, key, path)
}

func (cl *redisProcessor) JsonNumIncrBy(ctx context.Context, key, path string, num int) *redis.StringCmd {
	return jsonNumIncrByExecute(cl, ctx, key, path, num)
}

func (cl *redisProcessor) JsonNumMultBy(ctx context.Context, key, path string, num int) *redis.StringCmd {
	return jsonNumMultByExecute(cl, ctx, key, path, num)
}

func (cl *redisProcessor) JsonStrAppend(ctx context.Context, key, path, appendString string) *redis.IntCmd {
	return jsonStrAppendExecute(cl, ctx, key, path, appendString)
}

func (cl *redisProcessor) JsonStrLen(ctx context.Context, key, path string) *redis.IntCmd {
	return jsonStrLenExecute(cl, ctx, key, path)
}

func (cl *redisProcessor) JsonArrAppend(ctx context.Context, key, path string, jsons ...interface{}) *redis.IntCmd {
	return jsonArrAppendExecute(cl, ctx, append([]interface{}{key, path}, jsons...)...)
}

func (cl *redisProcessor) JsonArrIndex(ctx context.Context, key, path string, jsonScalar interface{}, startAndStop ...interface{}) *redis.IntCmd {
	return jsoArrIndexExecute(cl, ctx, append([]interface{}{key, path, jsonScalar}, startAndStop...)...)
}

func (cl *redisProcessor) JsonArrInsert(ctx context.Context, key, path string, index int, jsons ...interface{}) *redis.IntCmd {
	return jsonArrInsertExecute(cl, ctx, append([]interface{}{key, path, index}, jsons...)...)
}

func (cl *redisProcessor) JsonArrLen(ctx context.Context, key, path string) *redis.IntCmd {
	return jsonArrLenExecute(cl, ctx, key, path)
}

func (cl *redisProcessor) JsonArrPop(ctx context.Context, key, path string, index int) *redis.StringCmd {
	return jsonArrPopExecute(cl, ctx, key, path, index)
}

func (cl *redisProcessor) JsonArrTrim(ctx context.Context, key, path string, start, stop int) *redis.IntCmd {
	return jsonArrTrimExecute(cl, ctx, key, path, start, stop)
}

func (cl *redisProcessor) JsonObjKeys(ctx context.Context, key, path string) *redis.StringSliceCmd {
	return jsonObjKeysExecute(cl, ctx, key, path)
}

func (cl *redisProcessor) JsonObjLen(ctx context.Context, key, path string) *redis.IntCmd {
	return jsonObjLen(cl, ctx, key, path)
}
