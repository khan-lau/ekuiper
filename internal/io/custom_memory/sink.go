// Copyright 2021-2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package custom_memory

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/go-ini/ini"
	"github.com/lf-edge/ekuiper/internal/io/custom_memory/pubsub"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type config struct {
	Topic        string   `json:"topic"`
	DataTemplate string   `json:"dataTemplate"`
	RowkindField string   `json:"rowkindField"`
	KeyField     string   `json:"keyField"`
	Fields       []string `json:"fields"`
	DataField    string   `json:"dataField"`
	ResendTopic  string   `json:"resendDestination"`
}

type sink struct {
	topic        string
	hasTransform bool
	keyField     string
	rowkindField string
	fields       []string
	dataField    string
	resendTopic  string

	tags           map[string]interface{} // 用于数据清洗
	cfg            *ini.File              // rule 配置
}

// Configure 用于配置sink的结构体实例
// 参数：
//
//	props: 一个包含配置属性的map，键为string类型，值为interface{}类型
//
// 返回值:
//
//	error: 如果配置过程中发生错误，则返回非零的错误码；否则返回nil
func (s *sink) Configure(props map[string]interface{}) error {
	cfg := &config{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return err
	}
	if strings.ContainsAny(cfg.Topic, "#+") {
		return fmt.Errorf("invalid custom_memory topic %s: wildcard found", cfg.Topic)
	}
	s.topic = cfg.Topic
	if cfg.DataTemplate != "" {
		s.hasTransform = true
	}
	s.dataField = cfg.DataField
	s.fields = cfg.Fields
	s.rowkindField = cfg.RowkindField
	s.keyField = cfg.KeyField
	if s.rowkindField != "" && s.keyField == "" {
		return fmt.Errorf("keyField is required when rowkindField is set")
	}
	s.resendTopic = cfg.ResendTopic
	if s.resendTopic == "" {
		s.resendTopic = s.topic
	}
	return nil
}

// Open 是 sink 结构体中的方法，用于打开并初始化自定义内存接收器
//
// 参数：
//
//	ctx api.StreamContext：流上下文，包含日志记录器和其他相关信息
//
// 返回值：
//
//	error：如果打开过程中发生错误，则返回非零的错误码；否则返回nil
func (s *sink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("Opening custom_memory sink: %v", s.topic)
	pubsub.CreatePub(s.topic)
	return nil
}

// Collect 是sink类型的方法，用于收集数据
//
// 参数：
//
//	ctx api.StreamContext：流上下文，用于获取日志记录器等资源
//	data interface{}：待收集的数据，可以是任意类型
//
// 返回值：
//
//	error：如果收集过程中出现错误，则返回非零的错误码；否则返回nil
func (s *sink) Collect(ctx api.StreamContext, data interface{}) error {
	ctx.GetLogger().Debugf("receive %+v", data)
	ctx.GetLogger().Infof("receive %+v", data)
	return s.collectWithTopic(ctx, data, s.topic)
}

// CollectResend 方法用于在sink结构体上调用，用于重新发送数据
// 参数：
//
//	ctx：api.StreamContext类型，上下文对象，用于获取日志记录器等资源
//	data：interface{}类型，要发送的数据，具体类型由调用者确定
//
// 返回值：
//
//	error类型，表示方法执行过程中的错误，如果执行成功则返回nil
func (s *sink) CollectResend(ctx api.StreamContext, data interface{}) error {
	ctx.GetLogger().Debugf("resend %+v", data)
	// ctx.GetLogger().Infof("resend %+v", data)
	return s.collectWithTopic(ctx, data, s.resendTopic)
}

// Close 关闭自定义内存接收器，并从pubsub中移除对应的订阅
//
// 参数：
//
//	ctx api.StreamContext：流上下文
//
// 返回值：
//
//	error：如果关闭成功则返回nil，否则返回错误
func (s *sink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("closing custom_memory sink")
	pubsub.RemovePub(s.topic)
	return nil
}


// collectWithTopic 函数根据给定的 StreamContext、数据和主题字符串，将数据收集并发布到相应的主题
//
// 参数：
//
//	s: *sink - 接收器指针，用于调用发布函数
//	ctx: api.StreamContext - 流上下文，包含模板解析和转换输出的方法
//	data: interface{} - 待收集并发布的数据
//	t: string - 主题字符串，可能包含模板表达式
//
// 返回值：
//
//	error - 如果发生错误，则返回非零的错误码；否则返回 nil
func (s *sink) collectWithTopic(ctx api.StreamContext, data interface{}, t string) error {
	topic, err := ctx.ParseTemplate(t, data)
	if err != nil {
		return err
	}

	if s.hasTransform {
		jsonBytes, _, err := ctx.TransformOutput(data)
		if err != nil {
			return err
		}
		m := make(map[string]interface{})
		err = json.Unmarshal(jsonBytes, &m)
		if err != nil {
			return fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(jsonBytes), err)
		}
		data = m
	} else {
		m, _, err := transform.TransItem(data, s.dataField, s.fields)
		if err != nil {
			return fmt.Errorf("fail to select fields %v for data %v", s.fields, data)
		}
		data = m
	}

	switch d := data.(type) {
	case []map[string]interface{}:
		for _, el := range d {
			msgs := s.watch(ctx, el) // 清洗数据, 因为有些情况要补记录, 所以可能会产生多条
			for _, item := range msgs {
				err := s.publish(ctx, topic, item)
				if err != nil {
					return fmt.Errorf("fail to publish data %v for error %v", d, err)
				}
			}
		}
	case map[string]interface{}:
		msgs := s.watch(ctx, d) // 清洗数据, 因为有些情况要补记录, 所以可能会产生多条

		for _, item := range msgs {
			err := s.publish(ctx, topic, item)
			if err != nil {
				return fmt.Errorf("fail to publish data %v for error %v", d, err)
			}
		}

	default:
		return fmt.Errorf("unrecognized format of %s", data)
	}
	return nil
}


// publish 方法将消息发布到指定的主题上
//
// 参数：
//
//	ctx：StreamContext，用于在消息传递过程中提供上下文信息
//	topic：string，消息要发布到的主题名称
//	el：map[string]interface{}，要发布的消息内容，是一个键值对映射
//
// 返回值：
//
//	error，如果发布过程中发生错误，则返回非零的错误码；否则返回nil
func (s *sink) publish(ctx api.StreamContext, topic string, el map[string]interface{}) error {
	logger := ctx.GetLogger();
	if s.rowkindField != "" {
		c, ok := el[s.rowkindField]
		var rowkind string
		if !ok {
			rowkind = ast.RowkindUpsert
		} else {
			rowkind, ok = c.(string)
			if !ok {
				return fmt.Errorf("rowkind field %s is not a string in data %v", s.rowkindField, el)
			}
			if rowkind != ast.RowkindInsert && rowkind != ast.RowkindUpdate && rowkind != ast.RowkindDelete && rowkind != ast.RowkindUpsert {
				return fmt.Errorf("invalid rowkind %s", rowkind)
			}
		}
		key, ok := el[s.keyField]
		if !ok {
			return fmt.Errorf("key field %s not found in data %v", s.keyField, el)
		}
		logger.Infof("update topic: %s, record: %+v", topic, el)
		pubsub.ProduceUpdatable(ctx, topic, el, rowkind, key)
	} else {
		logger.Infof("publish topic: %s, record: %+v", topic, el)
		pubsub.Produce(ctx, topic, el)
	}
	return nil
}



////////////////////////////////////////////////////////////////////////////////////////


// 从record的Action_Sink字段的配置中读取清洗条件, 并路由到对应的清洗函数
//   - @return []map[string]interface{} 清洗后的记录, 部分特殊情况可能会产生多条记录
//   - @return error                  错误信息
func (that *sink) watch(ctx api.StreamContext, record map[string]interface{}) []map[string]interface{} {
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
					ctx.GetLogger().Debugf("parse Action_Sink config error: %s", err)
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
func (that *sink) incJumpWash(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
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
			timestamp := int64(record["Time_Sink"].(float64))
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
		timestamp := int64(record["Time_Sink"].(float64))
		that.tags["Time_Sink"] = timestamp
		that.tags["offset"] = 0.0
	}

	return records
}

// 累加指标跳变过滤
//   - @return map[string]interface{} 过滤出来的记录
func (that *sink) incJumpFilter(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
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
			timestamp := int64(record["Time_Sink"].(float64))
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
		timestamp := int64(record["Time_Sink"].(float64))
		that.tags["Time_Sink"] = timestamp
		that.tags["offset"] = 0.0
	}

	return records
}

// 跳变清洗函数
//   - @return map[string]interface{} 清洗后的记录
func (that *sink) jumpWash(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
	records := make([]map[string]interface{}, 0, 1)
	logger := ctx.GetLogger()
	// 如果缓存中有数据，则进行跳变清洗
	if oldVal, ok := that.tags["Value_Sink"]; ok {
		if oldTime, ok := that.tags["Time_Sink"]; ok {
			// 计算斜率
			oldValFloat := oldVal.(float64)
			oldTimeInt := oldTime.(int64)

			val := record["Value_Sink"].(float64)
			timestamp := int64(record["Time_Sink"].(float64))
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
		timestamp := int64(record["Time_Sink"].(float64))
		that.tags["Time_Sink"] = timestamp
	}
	records = append(records, record)
	return records
}

// 跳变过滤函数
//   - @return map[string]interface{} 清洗后的记录
func (that *sink) jumpFilter(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
	records := make([]map[string]interface{}, 0, 1)
	logger := ctx.GetLogger()

	// 如果缓存中有数据，则进行跳变清洗
	if oldVal, ok := that.tags["Value_Sink"]; ok {
		if oldTime, ok := that.tags["Time_Sink"]; ok {
			// 计算斜率
			oldValFloat := oldVal.(float64)
			oldTimeInt := oldTime.(int64)

			val := record["Value_Sink"].(float64)
			timestamp := int64(record["Time_Sink"].(float64))
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
		timestamp := int64(record["Time_Sink"].(float64))
		that.tags["Time_Sink"] = timestamp
	}
	return records
}

// 死值处理
//   - @param ctx api.StreamContext 上下文
//   - @param record map[string]interface{} 原始记录
//   - @param cfg *ini.File 规则配置项
//   - @return map[string]interface{} 处理后的记录
func (that *sink) deadWash(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
	logger := ctx.GetLogger()
	records := make([]map[string]interface{}, 0, 1)
	val := record["Value_Sink"].(float64)
	timestamp := int64(record["Time_Sink"].(float64))
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
func (m *sink) outLimitWash(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
	logger := ctx.GetLogger()
	records := make([]map[string]interface{}, 0, 1)
	val := record["Value_Sink"].(float64)
	timestamp := int64(record["Time_Sink"].(float64))
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

