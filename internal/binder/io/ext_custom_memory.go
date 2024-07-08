package io

import (
	"github.com/lf-edge/ekuiper/internal/io/custom_memory"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func init() {
	sinks["custom_memoryPub"] = func() api.Sink { return custom_memory.GetSink() }
	sources["custom_memorySub"] = func() api.Source { return custom_memory.GetSource() }
	lookupSources["custom_memory"] = func() api.LookupSource { return custom_memory.GetLookupSource() }
}
