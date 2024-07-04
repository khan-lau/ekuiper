package pubsub

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/go-ini/ini"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/custom_kafka"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	thriftCommon "github.com/lf-edge/ekuiper/sdk/gen-go/common"
	kafkago "github.com/segmentio/kafka-go"
)

type kafka2Tdb struct {
	writer         *kafkago.Writer        // 用于写入kafka
	c              *sinkConf              // 用于sink配置
	kc             *kafkaConf             // 用于kafka配置
	tlsConfig      *tls.Config            // 用于tls配置
	sc             custom_kafka.SaslConf  //
	headersMap     map[string]string      //
	headerTemplate string                 // 模板
	tags           map[string]interface{} // 用于数据清洗
	cfg            *ini.File              // rule 配置
}

func (m *kafka2Tdb) Configure(props map[string]interface{}) error {
	c := &sinkConf{
		Brokers: "localhost:9092",
		Topic:   "",
	}
	if err := cast.MapToStruct(props, c); err != nil {
		return err
	}
	if len(strings.Split(c.Brokers, ",")) == 0 {
		return fmt.Errorf("brokers can not be empty")
	}
	if c.Topic == "" {
		return fmt.Errorf("topic can not be empty")
	}
	sc, err := custom_kafka.GetSaslConf(props)
	if err != nil {
		return err
	}
	if err := sc.Validate(); err != nil {
		return err
	}
	m.sc = sc
	tlsConfig, err := cert.GenTLSConfig(props, "kafka-sink")
	if err != nil {
		return err
	}
	m.tlsConfig = tlsConfig
	kc := &kafkaConf{
		RequiredACKs: -1,
		MaxAttempts:  1,
	}
	if err := cast.MapToStruct(props, kc); err != nil {
		return err
	}
	m.kc = kc
	m.c = c
	if err := m.setHeaders(); err != nil {
		return fmt.Errorf("set kafka header failed, err:%v", err)
	}
	return m.buildKafkaWriter()
}

func (m *kafka2Tdb) buildKafkaWriter() error {
	mechanism, err := m.sc.GetMechanism()
	if err != nil {
		return err
	}
	brokers := strings.Split(m.c.Brokers, ",")
	w := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  m.c.Topic,
		Balancer:               &kafkago.LeastBytes{},
		Async:                  false,
		AllowAutoTopicCreation: true,
		MaxAttempts:            m.kc.MaxAttempts,
		RequiredAcks:           kafkago.RequiredAcks(m.kc.RequiredACKs),
		BatchSize:              1,
		Transport: &kafkago.Transport{
			SASL: mechanism,
			TLS:  m.tlsConfig,
		},
	}
	m.writer = w
	return nil
}

func (m *kafka2Tdb) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Debug("Opening kafka sink")
	return nil
}

// 收到待发送的数据
//   - @param ctx 上下文
//   - @param item 待发送的数据
func (m *kafka2Tdb) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	logger.Debugf("kafka sink receive %s", item)
	var messages []kafkago.Message

	switch d := item.(type) {
	case []byte: // 如果是json字符串
		var trandatas []map[string]interface{}
		if err := json.Unmarshal(d, &trandatas); err != nil {
			logger.Error(err)
			return err
		}

		for _, msg := range trandatas {
			msgs := m.watch(ctx, msg) // 清洗数据, 因为有些情况要补记录, 所以可能会产生多条
			for _, item := range msgs {
				record := m.buildThriftRecord(ctx, item)
				if nil == record {
					logger.Debugf("msg [Value_Sink] is nil")
					continue
				}
				encode, err := m.SerializeThriftRecord(ctx, record)
				if err != nil {
					logger.Error(err)
					continue
				}

				kafkaMsg, err := m.buildMsg(ctx, item, []byte(encode))
				if err != nil {
					conf.Log.Errorf("build kafka msg failed, err:%v", err)
					return err
				}
				messages = append(messages, kafkaMsg)
			}
		}

	case []map[string]interface{}: // 如果是map数组, 这两种类型由rule.option 的 sendSingle 属性控制
		for _, msg := range d {
			msgs := m.watch(ctx, msg) // 清洗数据, 因为有些情况要补记录, 所以可能会产生多条

			for _, item := range msgs {
				record := m.buildThriftRecord(ctx, item)
				if nil == record {
					logger.Debugf("msg [Value_Sink] is nil")
					continue
				}
				encode, err := m.SerializeThriftRecord(ctx, record)
				if err != nil {
					logger.Error(err)
					continue
				}

				kafkaMsg, err := m.buildMsg(ctx, item, []byte(encode))
				if err != nil {
					conf.Log.Errorf("build kafka msg failed, err:%v", err)
					return err
				}
				messages = append(messages, kafkaMsg)
			}

		}
	case map[string]interface{}: // 如果是map
		msgs := m.watch(ctx, d) // 清洗数据, 因为有些情况要补记录, 所以可能会产生多条

		for _, item := range msgs {
			record := m.buildThriftRecord(ctx, item)
			if nil == record {
				logger.Debugf("msg [Value_Sink] is nil")
				continue
			}
			encode, err := m.SerializeThriftRecord(ctx, record)
			if err != nil {
				logger.Error(err)
				continue
			}

			kafkaMsg, err := m.buildMsg(ctx, item, []byte(encode))
			if err != nil {
				conf.Log.Errorf("build kafka msg failed, err:%v", err)
				return err
			}
			messages = append(messages, kafkaMsg)
		}
	default:
		return fmt.Errorf("unrecognized format of %s", item)
	}

	err := m.Publish(ctx, messages...)
	return err
}

// Close closes the kafka writer.
func (m *kafka2Tdb) Close(ctx api.StreamContext) error {
	return m.writer.Close()
}

// 发布消息到kafka
func (that *kafka2Tdb) Publish(ctx api.StreamContext, messages ...kafkago.Message) error {
	err := that.writer.WriteMessages(ctx, messages...) // publish 消息到kafka
	if err != nil {
		conf.Log.Errorf("kafka sink error: %v", err)
	} else {
		conf.Log.Debug("sink kafka success")
	}

	switch err := err.(type) {
	case kafkago.Error:
		if err.Temporary() {
			return fmt.Errorf(`%d: kafka sink fails to send out the data . %v`, errorx.IOErr, err)
		} else {
			return err
		}
	case kafkago.WriteErrors:
		count := 0
		for i := range messages {
			switch err := err[i].(type) {
			case nil:
				continue

			case kafkago.Error:
				if err.Temporary() {
					count++
					continue
				}
			default:
				if strings.Contains(err.Error(), "kafka.(*Client).Produce:") {
					return fmt.Errorf(`%d: kafka sink fails to send out the data . %v`, errorx.IOErr, err)
				}
			}
		}
		if count > 0 {
			return fmt.Errorf(`%d: kafka sink fails to send out the data . %v`, errorx.IOErr, err)
		} else {
			return err
		}
	case nil:
		return nil
	default:
		return fmt.Errorf(`%d: kafka sink fails to send out the data: %v`, errorx.IOErr, err)
	}
}

// 将sink收到的数据转成kafka message结构
func (m *kafka2Tdb) buildMsg(ctx api.StreamContext, item interface{}, decodedBytes []byte) (kafkago.Message, error) {
	msg := kafkago.Message{Value: decodedBytes}
	if len(m.kc.Key) > 0 {
		newKey, err := ctx.ParseTemplate(m.kc.Key, item)
		if err != nil {
			return kafkago.Message{}, fmt.Errorf("parse kafka key error: %v", err)
		}
		msg.Key = []byte(newKey)
	}
	headers, err := m.parseHeaders(ctx, item)
	if err != nil {
		return kafkago.Message{}, fmt.Errorf("parse kafka headers error: %v", err)
	}
	msg.Headers = headers
	return msg, nil
}

func (m *kafka2Tdb) setHeaders() error {
	if m.kc.Headers == nil {
		return nil
	}
	switch h := m.kc.Headers.(type) {
	case map[string]interface{}:
		kafkaHeaders := make(map[string]string)
		for key, value := range h {
			if sv, ok := value.(string); ok {
				kafkaHeaders[key] = sv
			}
		}
		m.headersMap = kafkaHeaders
		return nil
	case string:
		m.headerTemplate = h
		return nil
	default:
		return fmt.Errorf("kafka headers must be a map[string]string or a string")
	}
}

func (m *kafka2Tdb) parseHeaders(ctx api.StreamContext, data interface{}) ([]kafkago.Header, error) {
	if len(m.headersMap) > 0 {
		var kafkaHeaders []kafkago.Header
		for k, v := range m.headersMap {
			value, err := ctx.ParseTemplate(v, data)
			if err != nil {
				return nil, fmt.Errorf("parse kafka header map failed, err:%v", err)
			}
			kafkaHeaders = append(kafkaHeaders, kafkago.Header{
				Key:   k,
				Value: []byte(value),
			})
		}
		return kafkaHeaders, nil
	} else if len(m.headerTemplate) > 0 {
		headers := make(map[string]string)
		s, err := ctx.ParseTemplate(m.headerTemplate, data)
		if err != nil {
			return nil, fmt.Errorf("parse kafka header template failed, err:%v", err)
		}
		if err := json.Unmarshal([]byte(s), &headers); err != nil {
			return nil, err
		}
		var kafkaHeaders []kafkago.Header
		for key, value := range headers {
			kafkaHeaders = append(kafkaHeaders, kafkago.Header{
				Key:   key,
				Value: []byte(value),
			})
		}
		return kafkaHeaders, nil
	}
	return nil, nil
}

////////////////////////////////////////////////////////////////////////////////////////

// 构建thrift 消息结构
func (that *kafka2Tdb) buildThriftRecord(_ api.StreamContext, msg map[string]interface{}) *thriftCommon.Record {
	if val, ok := msg["Value_Sink"]; ok { // 过滤掉空值
		cell := &thriftCommon.Cell{Timestamp: msg["Time_Sink"].(int64), Value: val.(float64)}
		record := &thriftCommon.Record{}
		record.Name = msg["DevCode_Sink"].(string) + ":" + msg["Metric_Sink"].(string)
		record.Cells = []*thriftCommon.Cell{cell}
		return record
	} else {
		return nil
	}
}

// 将common.Record序列化为[]byte
func (that *kafka2Tdb) SerializeThriftRecord(ctx api.StreamContext, record *thriftCommon.Record) ([]byte, error) {
	// 创建一个 Thrift transport
	transport := thrift.NewTMemoryBuffer()

	thriftConf := &thrift.TConfiguration{
		TBinaryStrictRead:  thrift.BoolPtr(true),
		TBinaryStrictWrite: thrift.BoolPtr(true),
	}
	// 创建一个 Thrift protocol
	proto := thrift.NewTBinaryProtocolConf(transport, thriftConf)

	// 写入数据
	if err := record.Write(ctx, proto); err != nil {
		return nil, err
	}
	// 返回序列化后的数据
	return transport.Bytes(), nil
}

////////////////////////////////////////////////////////////////////////////////////////

// 从record的Action_Sink字段的配置中读取清洗条件, 并路由到对应的清洗函数
//   - @return []map[string]interface{} 清洗后的记录, 部分特殊情况可能会产生多条记录
//   - @return error                  错误信息
func (that *kafka2Tdb) watch(ctx api.StreamContext, record map[string]interface{}) []map[string]interface{} {
	records := make([]map[string]interface{}, 0, 2)
	_, ok := record["Value_Sink"]
	if !ok { // 过滤掉空值记录.
		return records
	}
	records = append(records, record)

	if actionObj, ok := record["Action_Sink"]; ok {
		if action, ok := actionObj.(string); ok {

			// 解析清洗配置
			if that.cfg == nil {
				cfg, err := ini.Load([]byte(action))
				if err != nil {
					ctx.GetLogger().Errorf("parse Action_Sink config error: %s", err)
					return records
				}
				that.cfg = cfg
			}

			if that.cfg.Section("").HasKey("proc") {
				proc := that.cfg.Section("").Key("proc").String()
				switch proc {
				case "jump": // 跳变
					{
						if that.cfg.Section("").HasKey("proc.filter") && that.cfg.Section("").Key("proc.filter").String() == "not_in" { // 跳变清洗
							return that.jumpWash(ctx, record, that.cfg)
						} else { // 跳变过滤
							return that.jumpFilter(ctx, record, that.cfg)
						}

					}
				case "inc_jump": // 递增指标跳变
					{
						if that.cfg.Section("").HasKey("proc.filter") && that.cfg.Section("").Key("proc.filter").String() == "not_in" { // 递增指标跳变清洗
							return that.incJumpWash(ctx, record, that.cfg)
						} else { // 递增指标跳变过滤
							return that.incJumpFilter(ctx, record, that.cfg)
						}
					}
				case "dead": // 死值处理
					{
						return that.deadWash(ctx, record, that.cfg)
					}
				case "out_limit": // 超限处理
					{
						return that.outLimitWash(ctx, record, that.cfg)
					}
				default:
					{
					}
				}
			}
		}
	}

	return records
}

// 累加跳变清洗并补点
//   - @return map[string]interface{} 清洗后的记录
func (that *kafka2Tdb) incJumpWash(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
	records := make([]map[string]interface{}, 0, 1)
	logger := ctx.GetLogger()
	// 如果缓存中有数据，则进行跳变清洗
	if oldVal, ok := that.tags["Value_Sink"]; ok {
		if oldTime, ok := that.tags["Time_Sink"]; ok {
			// 计算斜率
			oldValFloat := oldVal.(float64)
			oldTimeInt := oldTime.(int64)
			offset := that.tags["offset"].(float64)

			val := record["Value_Sink"].(float64)
			timestamp, _ := strconv.ParseInt(record["Time_Sink"].(string), 10, 64)
			threshold, err := strconv.ParseFloat(record["Adjust_Sink"].(string), 64)
			if nil != err {
				logger.Error(err)
			}

			if (math.Abs(val-oldValFloat) / math.Abs(float64(timestamp-oldTimeInt)/1000)) > threshold { // 如果跳变
				offset = (oldValFloat - val) + offset
				that.tags["offset"] = offset

				that.tags["Value_Sink"] = val      // 更新缓存中的值
				that.tags["Time_Sink"] = timestamp // 更新缓存中的时间戳

				return records
			} else { // 如果没有跳变, 则只需要更新缓存
				record["Value_Sink"] = val + offset

				that.tags["Value_Sink"] = val // 保留原始值到缓存, 便于计算下一次是否跳变
				that.tags["Time_Sink"] = timestamp

				records = append(records, record)
				return records
			}
		}
	} else { // 首次 记录当前值与时间戳到缓存
		that.tags["Value_Sink"] = record["Value_Sink"]
		timestamp, _ := strconv.ParseInt(record["Time_Sink"].(string), 10, 64)
		that.tags["Time_Sink"] = timestamp
		that.tags["offset"] = 0.0
	}

	return records
}

// 累加指标跳变过滤
//   - @return map[string]interface{} 过滤出来的记录
func (that *kafka2Tdb) incJumpFilter(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
	records := make([]map[string]interface{}, 0, 1)
	logger := ctx.GetLogger()

	// 如果缓存中有数据，则进行跳变清洗
	if oldVal, ok := that.tags["Value_Sink"]; ok {
		if oldTime, ok := that.tags["Time_Sink"]; ok {
			// 计算斜率
			oldValFloat := oldVal.(float64)
			oldTimeInt := oldTime.(int64)
			offset := that.tags["offset"].(float64)

			val := record["Value_Sink"].(float64)
			timestamp, _ := strconv.ParseInt(record["Time_Sink"].(string), 10, 64)
			threshold, err := strconv.ParseFloat(record["Adjust_Sink"].(string), 64)
			if nil != err {
				logger.Error(err)
			}
			if (math.Abs(val-oldValFloat) / math.Abs(float64(timestamp-oldTimeInt)/1000)) > threshold { // 如果跳变
				offset = (oldValFloat - val) + offset
				that.tags["offset"] = offset

				that.tags["Value_Sink"] = val      // 更新缓存中的值
				that.tags["Time_Sink"] = timestamp // 更新缓存中的时间戳

				records = append(records, record)
				return records
			} else { // 如果没有跳变, 则只需要更新缓存
				record["Value_Sink"] = val + offset

				that.tags["Value_Sink"] = val // 保留原始值到缓存, 便于计算下一次是否跳变
				that.tags["Time_Sink"] = timestamp

				return records
			}
		}
	} else { // 首次 记录当前值与时间戳到缓存
		that.tags["Value_Sink"] = record["Value_Sink"]
		timestamp, _ := strconv.ParseInt(record["Time_Sink"].(string), 10, 64)
		that.tags["Time_Sink"] = timestamp
		that.tags["offset"] = 0.0
	}

	return records
}

// 跳变清洗函数
//   - @return map[string]interface{} 清洗后的记录
func (that *kafka2Tdb) jumpWash(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
	records := make([]map[string]interface{}, 0, 1)
	logger := ctx.GetLogger()
	// 如果缓存中有数据，则进行跳变清洗
	if oldVal, ok := that.tags["Value_Sink"]; ok {
		if oldTime, ok := that.tags["Time_Sink"]; ok {
			// 计算斜率
			oldValFloat := oldVal.(float64)
			oldTimeInt := oldTime.(int64)

			val := record["Value_Sink"].(float64)
			timestamp, _ := strconv.ParseInt(record["Time_Sink"].(string), 10, 64)
			threshold, err := strconv.ParseFloat(record["Adjust_Sink"].(string), 64)
			if nil != err {
				logger.Error(err)
			}

			if (math.Abs(val-oldValFloat) / math.Abs(float64(timestamp-oldTimeInt)/1000)) > threshold { // 如果越限
				record["Value_Sink"] = oldValFloat // 补偿为前一个值

				that.tags["Value_Sink"] = oldValFloat // 更新缓存中的值
				that.tags["Time_Sink"] = timestamp    // 更新缓存中的时间戳
				records = append(records, record)
				return records
			} else { // 如果没有越限, 则只需要更新缓存
				that.tags["Value_Sink"] = val
				that.tags["Time_Sink"] = timestamp
			}
		}
	} else { // 否则记录当前值与时间戳到缓存
		that.tags["Value_Sink"] = record["Value_Sink"]
		timestamp, _ := strconv.ParseInt(record["Time_Sink"].(string), 10, 64)
		that.tags["Time_Sink"] = timestamp
	}
	records = append(records, record)
	return records
}

// 跳变过滤函数
//   - @return map[string]interface{} 清洗后的记录
func (that *kafka2Tdb) jumpFilter(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
	records := make([]map[string]interface{}, 0, 1)
	logger := ctx.GetLogger()

	// 如果缓存中有数据，则进行跳变清洗
	if oldVal, ok := that.tags["Value_Sink"]; ok {
		if oldTime, ok := that.tags["Time_Sink"]; ok {
			// 计算斜率
			oldValFloat := oldVal.(float64)
			oldTimeInt := oldTime.(int64)

			val := record["Value_Sink"].(float64)
			timestamp, _ := strconv.ParseInt(record["Time_Sink"].(string), 10, 64)
			threshold, err := strconv.ParseFloat(record["Adjust_Sink"].(string), 64)
			if nil != err {
				logger.Error(err)
			}
			if (math.Abs(val-oldValFloat) / math.Abs(float64(timestamp-oldTimeInt)/1000)) > threshold { // 如果越限
				that.tags["Value_Sink"] = oldValFloat // 更新缓存中的值
				that.tags["Time_Sink"] = timestamp    // 更新缓存中的时间戳
				records = append(records, record)
				return records
			} else { // 如果没有越限, 则只需要更新缓存
				that.tags["Value_Sink"] = val
				that.tags["Time_Sink"] = timestamp
			}
		}
	} else { // 否则记录当前值与时间戳到缓存
		that.tags["Value_Sink"] = record["Value_Sink"]
		timestamp, _ := strconv.ParseInt(record["Time_Sink"].(string), 10, 64)
		that.tags["Time_Sink"] = timestamp
	}
	return records
}

// 死值处理
//   - @param ctx api.StreamContext 上下文
//   - @param record map[string]interface{} 原始记录
//   - @param cfg *ini.File 规则配置项
//   - @return map[string]interface{} 处理后的记录
func (that *kafka2Tdb) deadWash(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
	logger := ctx.GetLogger()
	records := make([]map[string]interface{}, 0, 1)
	val := record["Value_Sink"].(float64)
	timestamp := record["Time_Sink"].(int64)
	flagVal, err := strconv.ParseFloat(record["Adjust_Sink"].(string), 64)
	if nil != err {
		logger.Error(err)
		return records
	}

	flag := Round(flagVal) == 1 // 0: 丢弃, 1: 输出
	if !flag {
		logger.Warnf("deadWatcch ignore value: %d, %f", timestamp, val)
		return records
	}

	records = append(records, record)
	return records
}

// 越限处理
//   - @param ctx api.StreamContext 上下文
//   - @param record map[string]interface{} 原始记录
//   - @param cfg *ini.File 规则配置项
//   - @return map[string]interface{} 处理后的记录
func (m *kafka2Tdb) outLimitWash(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
	logger := ctx.GetLogger()
	records := make([]map[string]interface{}, 0, 1)
	val := record["Value_Sink"].(float64)
	timestamp := record["Time_Sink"].(int64)
	flagVal, err := strconv.ParseFloat(record["Adjust_Sink"].(string), 64)
	if nil != err {
		logger.Error(err)
		return records
	}

	flag := Round(flagVal) == 1 // 0: 丢弃, 1: 输出
	if !flag {
		logger.Warnf("outLimitWatcch ignore value: %d, %f", timestamp, val)
		return records
	}

	records = append(records, record)
	return records
}

////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////

// 野鸡四舍五入法
func Round(val float64) int {
	return int(math.Floor(val + 0.5))
}

func Kafka2Tdb() api.Sink {
	return &kafka2Tdb{tags: make(map[string]interface{})}
}
