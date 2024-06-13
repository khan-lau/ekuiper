package pubsub

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/mitchellh/mapstructure"
)

type KafkaFormat struct {
	DevCode  string  `json:"devCode"`  // 设备代码
	Metric   string  `json:"metric"`   // 指标
	DataType string  `json:"dataType"` // 数据类型
	Value    float64 `json:"value"`    // 值
	Time     int64   `json:"time"`     // 时间戳
}

func (x *KafkaFormat) GetSchemaJson() string {
	// return a static schema
	return `{
		"DevCode": {"type": "string"},
		"Metric": {"type": "string"},
		"DataType": {"type": "string"},
		"Value": {"type": "float"},
		"Time": {"type": "string"}
	}`
}

func (x *KafkaFormat) Encode(d interface{}) ([]byte, error) {
	var builder strings.Builder

	switch dt := d.(type) {
	case map[string]interface{}:
		return x.encodeSingleMap(dt)
	case []map[string]interface{}:
		for i, item := range dt {
			encoded, err := x.encodeSingleMap(item)
			if err != nil {
				return nil, err
			}
			if i > 0 {
				builder.WriteString(",")
			}
			builder.Write(encoded)
		}
		return []byte(builder.String()), nil
	default:
		return nil, fmt.Errorf("unsupported type %T, must be a map or slice of maps", d)
	}
}

func (x *KafkaFormat) encodeSingleMap(item map[string]interface{}) ([]byte, error) {
	err := MapToStructStrict(item, x)
	if err != nil {
		return nil, err
	}
	Value_Sink := strconv.FormatFloat(x.Value, 'f', -1, 64)
	Time_Sink := strconv.FormatInt(x.Time, 10)
	return []byte(fmt.Sprintf("%s:%s@%s:%s:%s", x.DevCode, x.Metric, x.DataType, Value_Sink, Time_Sink)), nil
}

func (x *KafkaFormat) Decode(b []byte) (interface{}, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("input byte slice is empty")
	}
	strData := byteSliceToString(b)

	parts := strings.Split(strData, ",")
	resultMsgs := make([]map[string]interface{}, 0, len(parts)) // 预分配切片容量

	var decodeError error
	mapPool := sync.Pool{
		New: func() interface{} {
			return make(map[string]interface{})
		},
	}
	for _, part := range parts {
		if part == "" {
			continue // skip empty messages
		}

		rs := strings.Split(part, "@")
		if len(rs) != 2 {
			decodeError = fmt.Errorf("invalid message format: %v", part)
			continue
		}
		point := rs[0]
		pointValue := rs[1]

		lastColonIndex := strings.LastIndex(point, ":")
		devCode := point[:lastColonIndex]
		metric := point[lastColonIndex+1:]
		pvs := strings.Split(pointValue, ":")

		if len(pvs) != 3 {
			decodeError = fmt.Errorf("invalid message format: %v", part)
			continue
		}
		dataType := pvs[0]
		if dataType == "S" {
			continue
		}
		value, err := strconv.ParseFloat(pvs[1], 64)
		if err != nil {
			decodeError = fmt.Errorf("invalid message format: %v", part)
			continue
		}
		timestamp, err := strconv.ParseInt(pvs[2], 10, 64)
		if err != nil {
			decodeError = fmt.Errorf("invalid message format: %v", part)
			continue
		}

		// 时间戳的精度提升到ms
		if timestamp < 10000000000 {
			timestamp *= 1000
		}

		// // 时间戳的精度降到s
		// if timestamp > 9999999999 {
		// 	timestamp /= 1000
		// }

		rm := mapPool.Get().(map[string]interface{}) // 从Pool中获取一个map
		rm["DevCode"] = devCode
		rm["Metric"] = metric
		rm["DataType"] = dataType
		rm["Value"] = value
		rm["Time"] = timestamp

		resultMsgs = append(resultMsgs, rm)
	}
	if len(resultMsgs) == 0 && decodeError == nil {
		return nil, nil
	}
	for _, msg := range resultMsgs {
		mapPool.Put(msg)
	}
	return resultMsgs, decodeError
}

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

func GetKafkaFormat() interface{} {
	return &KafkaFormat{}
}

// byteSliceToString直接将[]byte转换为string，避免额外的内存分配
func byteSliceToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
