package io

import (
	"github.com/lf-edge/ekuiper/internal/io/custom_memory"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/modules"
)

func init() {
	modules.RegisterSink("custom_memoryPub", func() api.Sink { return custom_memory.GetSink() })
	modules.RegisterLookupSource("custom_memory", func() api.LookupSource { return custom_memory.GetLookupSource() })
	modules.RegisterSource("custom_memorySub", func() api.Source { return custom_memory.GetSource() })
}
