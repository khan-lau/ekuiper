package io

import (
	"github.com/lf-edge/ekuiper/internal/io/custom_kafka/pubsub"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/modules"
)

func init() {
	modules.RegisterSink("custom_kafkaPub", func() api.Sink { return pubsub.KafkaPub() })
	modules.RegisterSink("custom_kafka2Tdb", func() api.Sink { return pubsub.Kafka2Tdb() })
	modules.RegisterSource("custom_kafkaSub", func() api.Source { return pubsub.KafkaSub() })
}
