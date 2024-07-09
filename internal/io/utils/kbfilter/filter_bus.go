package kbfilter

import (
	"math"
	"strconv"

	"github.com/lf-edge/ekuiper/internal/io/utils"
	"github.com/lf-edge/ekuiper/pkg/api"
	"gopkg.in/ini.v1"
)

type SinkFilterAction struct {
	tags map[string]interface{} // 用于数据清洗
	cfg  *ini.File              // rule 配置
}

func NewSinkFilterAction() *SinkFilterAction {
	return &SinkFilterAction{tags: make(map[string]interface{}), cfg: nil}
}

// 从record的Action_Sink字段的配置中读取清洗条件, 并路由到对应的清洗函数
//   - @return []map[string]interface{} 清洗后的记录, 部分特殊情况可能会产生多条记录
//   - @return error                  错误信息
func (that *SinkFilterAction) Watch(ctx api.StreamContext, record map[string]interface{}) []map[string]interface{} {
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
func (that *SinkFilterAction) incJumpWash(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
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
func (that *SinkFilterAction) incJumpFilter(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
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
func (that *SinkFilterAction) jumpWash(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
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
func (that *SinkFilterAction) jumpFilter(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
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
func (that *SinkFilterAction) deadWash(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
	logger := ctx.GetLogger()
	records := make([]map[string]interface{}, 0, 1)
	val := record["Value_Sink"].(float64)
	timestamp := int64(record["Time_Sink"].(float64))
	flagVal, err := strconv.ParseFloat(record["Adjust_Sink"].(string), 64)
	if nil != err {
		logger.Error(err)
		return records
	}

	flag := utils.Round(flagVal) == 1 // 0: 丢弃, 1: 输出
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
func (m *SinkFilterAction) outLimitWash(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
	logger := ctx.GetLogger()
	records := make([]map[string]interface{}, 0, 1)
	val := record["Value_Sink"].(float64)
	timestamp := int64(record["Time_Sink"].(float64))
	flagVal, err := strconv.ParseFloat(record["Adjust_Sink"].(string), 64)
	if nil != err {
		logger.Error(err)
		return records
	}

	flag := utils.Round(flagVal) == 1 // 0: 丢弃, 1: 输出
	if !flag {
		logger.Warnf("outLimitWatcch ignore value: %d, %f", timestamp, val)
		return records
	}

	records = append(records, record)
	return records
}
