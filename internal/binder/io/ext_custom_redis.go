package io

import (
	"github.com/lf-edge/ekuiper/internal/io/custom_redis"
	"github.com/lf-edge/ekuiper/internal/io/custom_redis/pubsub"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/modules"
)

func init() {
	modules.RegisterSink("custom_redis", func() api.Sink { return custom_redis.GetSink() })
	modules.RegisterLookupSource("custom_redis", func() api.LookupSource { return custom_redis.GetLookupSource() })
	modules.RegisterSink("custom_redisPub", func() api.Sink { return pubsub.RedisPub() })
	modules.RegisterSink("custom_redis2Tdb", func() api.Sink { return pubsub.Redis2Tdb() })
	modules.RegisterSource("custom_redisSub", func() api.Source { return pubsub.RedisSub() })
}
