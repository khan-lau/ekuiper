package kbfilter

import (
	"fmt"
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
	_, ok = record["DataType_Sink"]
	if !ok { // 过滤掉无类型记录.
		return records
	}

	if actionObj, ok := record["Action_Sink"]; ok {
		if action, ok := actionObj.(string); ok {

			// 解析清洗配置
			if that.cfg == nil {
				cfg, err := ini.Load([]byte(action))
				if err != nil {
					ctx.GetLogger().Errorf("parse Action_Sink config error: %s", err)
					ctx.GetLogger().Warnf("add record to result")
					records = append(records, record)
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
				case "dead_time": // 按时间计算的死值处理
					{
						if that.cfg.Section("").HasKey("proc.count") {
							return that.deadTimeWash(ctx, record, that.cfg)
						} else {
							return make([]map[string]interface{}, 0)
						}

					}
				case "dead_count": // 按记录重复次数的死值处理
					{
						if that.cfg.Section("").HasKey("proc.count") {
							return that.deadCountWash(ctx, record, that.cfg)
						} else {
							return make([]map[string]interface{}, 0)
						}
					}
				case "out_limit": // 超限处理
					{
						return that.outLimitWash(ctx, record, that.cfg)
					}
				default:
					{
						ctx.GetLogger().Warnf("add record to result")
						records = append(records, record)
					}
				}
			} else {
				ctx.GetLogger().Warnf("add record to result")
				records = append(records, record)
			}
		} else {
			ctx.GetLogger().Warnf("add record to result")
			records = append(records, record)
		}
	} else {
		ctx.GetLogger().Warnf("add record to result")
		records = append(records, record)
	}

	return records
}

// 累加跳变清洗并补点
//   - @return map[string]interface{} 清洗后的正常记录
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
			timestamp, err := that.getTimestamp(record)
			if nil != err {
				logger.Error(err)
				return records
			}
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
		timestamp, err := that.getTimestamp(record)
		if nil != err {
			logger.Error(err)
			return records
		}
		that.tags["Time_Sink"] = timestamp
		that.tags["offset"] = 0.0
		records = append(records, record)
	}

	return records
}

// 累加指标跳变过滤
//   - @return map[string]interface{} 过滤出来的脏记录
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
			timestamp, err := that.getTimestamp(record)
			if nil != err {
				logger.Error(err)
				return records
			}
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

		timestamp, err := that.getTimestamp(record)
		if nil != err {
			logger.Error(err)
			return records
		}

		that.tags["Time_Sink"] = timestamp
		that.tags["offset"] = 0.0
		// records = append(records, record)
	}

	return records
}

// 跳变清洗函数
//   - @return map[string]interface{} 清洗后的正常记录
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
			timestamp, err := that.getTimestamp(record)
			if nil != err {
				logger.Error(err)
				return records
			}
			threshold, err := strconv.ParseFloat(record["Adjust_Sink"].(string), 64)
			if nil != err {
				logger.Error(err)
			}

			if (math.Abs(val-oldValFloat) / math.Abs(float64(timestamp-oldTimeInt)/1000)) > threshold { // 如果越限
				// record["Value_Sink"] = oldValFloat // 补偿为前一个值

				that.tags["Value_Sink"] = oldValFloat // 更新缓存中的值
				that.tags["Time_Sink"] = timestamp    // 更新缓存中的时间戳

				return records
			} else { // 如果没有越限, 则只需要更新缓存
				that.tags["Value_Sink"] = val
				that.tags["Time_Sink"] = timestamp
			}
		}
	} else { // 否则记录当前值与时间戳到缓存
		that.tags["Value_Sink"] = record["Value_Sink"]
		timestamp, err := that.getTimestamp(record)
		if nil != err {
			logger.Error(err)
			return records
		}
		that.tags["Time_Sink"] = timestamp
	}
	records = append(records, record)
	return records
}

// 跳变过滤函数
//   - @return map[string]interface{} 脏记录
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

			timestamp, err := that.getTimestamp(record)
			if nil != err {
				logger.Error(err)
				return records
			}

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
		timestamp, err := that.getTimestamp(record)
		if nil != err {
			logger.Error(err)
			return records
		}
		that.tags["Time_Sink"] = timestamp

	}
	return records
}

// 死值处理
//   - @param ctx api.StreamContext 上下文
//   - @param record map[string]interface{} 原始记录
//   - @param cfg *ini.File 规则配置项
//   - @return map[string]interface{} 处理后的记录
func (that *SinkFilterAction) deadCountWash(ctx api.StreamContext, record map[string]interface{}, cfg *ini.File) []map[string]interface{} {
	logger := ctx.GetLogger()

	records := make([]map[string]interface{}, 0, 1)
	count, err := strconv.ParseInt(cfg.Section("").Key("proc.count").String(), 10, 64)
	if nil != err {
		logger.Errorf("dead count parameter parser fault, %s", err)
		return records
	}
	if count < 1 {
		count = 1
	}

	// timestamp, err := that.getTimestamp(record)
	// if nil != err {
	// 	logger.Error(err)
	// 	return records
	// }

	flagVal, err := strconv.ParseFloat(record["Adjust_Sink"].(string), 64)
	if nil != err {
		logger.Error(err)
		return records
	}
	flag := utils.Round(flagVal) == 1 // 0: 输出非死值, 1: 输出死值
	val := record["Value_Sink"].(float64)

	if oldVal, ok := that.tags["Value_Sink"]; ok {
		oldValFloat, oldTimeInt := oldVal.(float64), that.tags["Time_Sink"].(int64)
		if math.Abs(val-oldValFloat) < 0.000001 { // 如果值没有变化
			that.tags["Time_Sink"] = oldTimeInt + 1
			if that.tags["Time_Sink"].(int64) >= count-1 { // 如果时间戳已经超过阈值
				if flag {
					// logger.Warnf("dead count filter, flag: %v, curr_ount: %d, count: %d, value: %f, timestamp: %d", flag, that.tags["Time_Sink"].(int64), count, val, timestamp)
					records = append(records, record)
				}
			} else {
				if !flag {
					// logger.Warnf("dead count filter, flag: %v, curr_ount: %d, count: %d, value: %f, timestamp: %d", flag, that.tags["Time_Sink"].(int64), count, val, timestamp)
					records = append(records, record)
				}
			}
		} else { // 如果值有变化, 重新计数
			that.tags["Value_Sink"], that.tags["Time_Sink"] = record["Value_Sink"], int64(0)
			if !flag {
				// logger.Warnf("dead count filter, flag: %v, curr_ount: %d, count: %d, value: %f, timestamp: %d", flag, that.tags["Time_Sink"].(int64), count, val, timestamp)
				records = append(records, record)
			}
		}

	} else { // 否则记录当前值与时间戳到缓存
		that.tags["Value_Sink"], that.tags["Time_Sink"] = record["Value_Sink"], int64(0)
		if !flag {
			// logger.Warnf("dead count filter, flag: %v, curr_ount: %d, count: %d, value: %f, timestamp: %d", flag, that.tags["Time_Sink"].(int64), count, val, timestamp)
			records = append(records, record)
		}
	}

	return records
}

// 死值处理
//   - @param ctx api.StreamContext 上下文
//   - @param record map[string]interface{} 原始记录
//   - @param cfg *ini.File 规则配置项
//   - @return map[string]interface{} 处理后的记录
func (that *SinkFilterAction) deadTimeWash(ctx api.StreamContext, record map[string]interface{}, cfg *ini.File) []map[string]interface{} {
	logger := ctx.GetLogger()

	records := make([]map[string]interface{}, 0, 1)
	count, err := strconv.ParseInt(cfg.Section("").Key("proc.count").String(), 10, 64)
	if nil != err {
		logger.Error(err)
		return records
	}

	if count < 1 {
		count = 1
	}

	timestamp, err := that.getTimestamp(record)
	if nil != err {
		logger.Error(err)
		return records
	}

	flagVal, err := strconv.ParseFloat(record["Adjust_Sink"].(string), 64)
	if nil != err {
		logger.Error(err)
		return records
	}

	flag := utils.Round(flagVal) == 1 // 0: 丢弃, 1: 输出
	val := record["Value_Sink"].(float64)

	if oldVal, ok := that.tags["Value_Sink"]; ok {
		if oldTime, ok := that.tags["Time_Sink"]; ok {
			oldValFloat := oldVal.(float64)
			if math.Abs(val-oldValFloat) < 0.000001 { // 如果值没有变化
				oldTimeInt := oldTime.(int64)
				if utils.AbsInt64(oldTimeInt-timestamp)/1000 >= count { // 如果时间差超过时长阈值
					if flag {
						// logger.Warnf("dead time filter, flag: %v, curr_count: %d, count: %d, value: %f, timestamp: %d", flag, utils.AbsInt64(oldTimeInt-timestamp)/1000, count, val, timestamp)
						records = append(records, record)
					}
				} else {
					if !flag {
						// logger.Warnf("dead time filter, flag: %v, curr_count: %d, count: %d, value: %f, timestamp: %d", flag, utils.AbsInt64(oldTimeInt-timestamp)/1000, count, val, timestamp)
						records = append(records, record)
					}
				}
			} else {
				that.tags["Value_Sink"], that.tags["Time_Sink"] = record["Value_Sink"], timestamp
				if !flag {
					// logger.Warnf("dead time filter, flag: %v, curr_count: %d, count: %d, value: %f, timestamp: %d", flag, 0, count, val, timestamp)
					records = append(records, record)
				}
			}
		}
	} else { // 否则记录当前值与时间戳到缓存
		that.tags["Value_Sink"], that.tags["Time_Sink"] = record["Value_Sink"], timestamp
		if !flag {
			// logger.Warnf("dead time filter, flag: %v, curr_count: %d, count: %d, value: %f, timestamp: %d", flag, 0, count, val, timestamp)
			records = append(records, record)
		}
	}

	return records
}

// 越限处理
//   - @param ctx api.StreamContext 上下文
//   - @param record map[string]interface{} 原始记录
//   - @param cfg *ini.File 规则配置项
//   - @return map[string]interface{} 处理后的记录
func (that *SinkFilterAction) outLimitWash(ctx api.StreamContext, record map[string]interface{}, _ *ini.File) []map[string]interface{} {
	logger := ctx.GetLogger()
	records := make([]map[string]interface{}, 0, 1)
	val := record["Value_Sink"].(float64)
	timestamp, err := that.getTimestamp(record)
	if nil != err {
		logger.Error(err)
		return records
	}
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

func (that *SinkFilterAction) getTimestamp(record map[string]interface{}) (int64, error) {
	timestamp := int64(0)
	switch timeObj := record["Time_Sink"].(type) {
	case int64:
		timestamp = timeObj
	case int:
		timestamp = int64(timeObj)
	case int32:
		timestamp = int64(timeObj)
	case float64:
		timestamp = int64(timeObj)
	case float32:
		timestamp = int64(timeObj)
	default:
		return -1, fmt.Errorf("Time_Sink type error")
	}
	return timestamp, nil
}