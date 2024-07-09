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
	"strings"

	"github.com/lf-edge/ekuiper/internal/io/custom_memory/pubsub"
	"github.com/lf-edge/ekuiper/internal/io/utils/kbfilter"
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

	filter *kbfilter.SinkFilterAction // 过滤器
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
	// ctx.GetLogger().Infof("receive %+v", data)
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
			msgs := s.filter.Watch(ctx, el) // 清洗数据, 因为有些情况要补记录, 所以可能会产生多条
			for _, item := range msgs {
				err := s.publish(ctx, topic, item)
				if err != nil {
					return fmt.Errorf("fail to publish data %v for error %v", d, err)
				}
			}
		}
	case map[string]interface{}:
		msgs := s.filter.Watch(ctx, d) // 清洗数据, 因为有些情况要补记录, 所以可能会产生多条

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
	logger := ctx.GetLogger()
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
