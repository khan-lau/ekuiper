package pubsub

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/mitchellh/mapstructure"
)

type RedisSourceMessage struct {
	Action   string  `json:"action"`   // 扩展指令
	DevCode  string  `json:"devCode"`  // 设备代码
	Metric   string  `json:"metric"`   // 指标
	DataType string  `json:"dataType"` // 数据类型
	Value    float64 `json:"value"`    // 值
	Time     int64   `json:"time"`     // 时间戳
}

func (that *RedisSourceMessage) GetSchemaJson() string {
	// return a static schema
	return `{
		"Action": {"type": "string"},
		"DevCode": {"type": "string"},
		"Metric": {"type": "string"},
		"DataType": {"type": "string"},
		"Value": {"type": "float"},
		"Time": {"type": "string"}
	}`
}

func (that *RedisSourceMessage) Encode(d interface{}) ([]byte, error) {
	var builder strings.Builder

	switch dt := d.(type) {
	case map[string]interface{}:
		return that.encodeSingleMap(dt)
	case []map[string]interface{}:
		for i, item := range dt {
			encoded, err := that.encodeSingleMap(item)
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

func (that *RedisSourceMessage) encodeSingleMap(item map[string]interface{}) ([]byte, error) {
	err := MapToStructStrict(item, that)
	if err != nil {
		return nil, err
	}
	Value_Sink := strconv.FormatFloat(that.Value, 'f', -1, 64)
	Time_Sink := strconv.FormatInt(that.Time, 10)
	return []byte(fmt.Sprintf("%s:%s@%s:%s:%s", that.DevCode, that.Metric, that.DataType, Value_Sink, Time_Sink)), nil
}

func (that *RedisSourceMessage) Decode(b []byte) (interface{}, error) {
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
		rm["Action"] = "none"
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

///////////////////////////////////////////////////////////////

type RedisSinkMessage struct {
	DevCode_Sink  string  `json:"DevCode_Sink"`
	Metric_Sink   string  `json:"Metric_Sink"`
	DataType_Sink string  `json:"DataType_Sink"`
	Value_Sink    float64 `json:"Value_Sink"`
	Time_Sink     int64   `json:"Time_Sink"`
}

func (that *RedisSinkMessage) GetSchemaJson() string {
	// return a static schema
	return `{
		"Action_Sink": {"type": "string"},"
		"DevCode_Sink": {"type": "string"},
		"Metric_Sink": {"type": "string"},
		"DataType_Sink": {"type": "string"},
		"Value_Sink": {"type": "float"},
		"Time_Sink": {"type": "string"}
	}`
}

// DTHYJK:NSFC:Q1:W001:WNAC_WdSpd@s:value:timestamp(unixmilli)
// @f;@s;@b
func (that *RedisSinkMessage) Encode(d interface{}) (string, error) {
	switch r := d.(type) {
	case map[string]interface{}:
		err := MapToStructStrict(r, that)
		if err != nil {
			return "", err
		}

		val, err := cast.ToString(that.Value_Sink, cast.CONVERT_ALL)
		if err != nil {
			return "", err
		}

		timestamp, err := cast.ToString(that.Time_Sink, cast.CONVERT_ALL)
		if err != nil {
			return "", err
		}

		result := that.DevCode_Sink + ":" + that.Metric_Sink + "@" + that.DataType_Sink + ":" + val + ":" + timestamp
		return result, nil
	default:
		return "", fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

///////////////////////////////////////////////////////////////

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

func GetRedisSourceMessage() interface{} {
	return &RedisSourceMessage{}
}

// byteSliceToString直接将[]byte转换为string，避免额外的内存分配
func byteSliceToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
