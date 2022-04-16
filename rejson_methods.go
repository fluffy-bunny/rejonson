package rejonson

import (
	"context"

	"github.com/go-redis/redis/v8"
)

func concatWithCmd(cmdName string, args []interface{}) []interface{} {
	res := make([]interface{}, 1)
	res[0] = cmdName
	for _, v := range args {
		if str, ok := v.(string); ok {
			if len(str) == 0 {
				continue
			}
		}
		res = append(res, v)
	}
	return res
}

func jsonDelExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, concatWithCmd("JSON.DEL", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonGetExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, concatWithCmd("JSON.GET", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonSetExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx, concatWithCmd("JSON.SET", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonMGetExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd(ctx, concatWithCmd("JSON.MGET", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonTypeExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, concatWithCmd("JSON.TYPE", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonNumIncrByExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, concatWithCmd("JSON.NUMINCRBY", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonNumMultByExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, concatWithCmd("JSON.NUMMULTBY", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonStrAppendExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, concatWithCmd("JSON.STRAPPEND", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonStrLenExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, concatWithCmd("JSON.STRLEN", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonArrAppendExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, concatWithCmd("JSON.ARRAPPEND", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsoArrIndexExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, concatWithCmd("JSON.ARRINDEX", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonArrInsertExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, concatWithCmd("JSON.ARRINSERT", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonArrLenExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, concatWithCmd("JSON.ARRLEN", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonArrPopExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, concatWithCmd("JSON.ARRPOP", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonArrTrimExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, concatWithCmd("JSON.ARRTRIM", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonObjKeysExecute(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd(ctx, concatWithCmd("JSON.OBJKEYS", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}

func jsonObjLen(c *redisProcessor, ctx context.Context, args ...interface{}) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, concatWithCmd("JSON.OBJLEN", args)...)
	_ = c.Process(context.Background(), cmd)
	return cmd
}
