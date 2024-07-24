package utils

import (
	"math"
	"unsafe"

	"github.com/mitchellh/mapstructure"
)

func MapToStructStrict(input, output interface{}) error {
	config := &mapstructure.DecoderConfig{
		ErrorUnused: true,
		TagName:     "json",
		Result:      output,
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(input)
}

// byteSliceToString直接将[]byte转换为string，避免额外的内存分配
func ByteSliceToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// 野鸡四舍五入法
func Round(val float64) int {
	return int(math.Floor(val + 0.5))
}

func AbsInt64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
