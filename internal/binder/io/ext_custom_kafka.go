package io

import (
	"github.com/lf-edge/ekuiper/internal/io/custom_kafka/pubsub"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func init() {
	sinks["custom_kafkaPub"] = func() api.Sink { return pubsub.KafkaPub() }
	sources["custom_kafkaSub"] = func() api.Source { return pubsub.KafkaSub() }
}
