package io

import (
	"github.com/lf-edge/ekuiper/internal/io/custom_redis"
	"github.com/lf-edge/ekuiper/internal/io/custom_redis/pubsub"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func init() {
	lookupSources["custom_redis"] = func() api.LookupSource { return custom_redis.GetLookupSource() }
	sinks["custom_redis"] = func() api.Sink { return custom_redis.GetSink() }
	sinks["custom_redisPub"] = func() api.Sink { return pubsub.RedisPub() }
	sinks["custom_redis2Tdb"] = func() api.Sink { return pubsub.Redis2Tdb() }
	sources["custom_redisSub"] = func() api.Source { return pubsub.RedisSub() }
}
