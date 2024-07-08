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

package pubsub

import (
	"regexp"
	"sync"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
)

const IdProperty = "topic"

type pubConsumers struct {
	count     int
	consumers map[string]chan api.SourceTuple // The consumer channel list [sourceId]chan
}

type subChan struct {
	regex *regexp.Regexp
	ch    chan api.SourceTuple
}

var (
	pubTopics = make(map[string]*pubConsumers)
	subExps   = make(map[string]*subChan)
	mu        = sync.RWMutex{}
)

// CreatePub 函数用于创建一个新的发布主题(topic)。
// 参数:
// topic: string类型，需要创建的发布主题名
// 功能:
//  1. 首先使用互斥锁锁定pubTopics字典的访问。
//  2. 检查pubTopics字典中是否已经存在该topic对应的消费者(pubConsumers)。
//     如果存在，则增加其计数并返回。
//  3. 如果不存在，则创建一个新的pubConsumers对象，并设置初始计数为1。
//  4. 将新的pubConsumers对象添加到pubTopics字典中。
//  5. 遍历subExps字典，找到与当前topic匹配的订阅表达式，
//     并调用addPubConsumer函数将新的pubConsumers对象与订阅表达式关联起来。
func CreatePub(topic string) {
	// 锁定互斥锁
	mu.Lock()
	defer mu.Unlock()

	// 如果pubTopics中已存在该topic对应的消费者，则增加其计数并返回
	if c, exists := pubTopics[topic]; exists {
		c.count += 1
		return
	}

	// 创建一个新的pubConsumers对象，并设置初始计数为1
	c := &pubConsumers{
		count:     1,
		consumers: make(map[string]chan api.SourceTuple),
	}

	// 将新的pubConsumers对象添加到pubTopics中
	pubTopics[topic] = c

	// 遍历subExps中的每个订阅表达式
	for sourceId, sc := range subExps {
		// 如果订阅表达式的正则表达式匹配该topic
		if sc.regex.MatchString(topic) {
			// 调用addPubConsumer函数，将新的pubConsumers对象与订阅表达式关联起来
			addPubConsumer(topic, sourceId, sc.ch)
		}
	}
}

// CreateSub 创建一个用于订阅消息的通道
//
//   - wildcard: 用于通配符匹配的字符串
//   - regex: 正则表达式对象，用于匹配发布主题，如果为nil则使用通配符匹配
//   - sourceId: 订阅者的唯一标识
//   - bufferLength: 通道的缓冲区大小
//
// 返回值:
//   - chan api.SourceTuple: 返回一个用于接收订阅消息的通道
func CreateSub(wildcard string, regex *regexp.Regexp, sourceId string, bufferLength int) chan api.SourceTuple {
	// 加锁，保证并发安全
	mu.Lock()
	defer mu.Unlock()

	// 创建一个带缓冲区的通道
	ch := make(chan api.SourceTuple, bufferLength)

	if regex != nil {
		// 如果正则表达式不为空
		// 将订阅关系保存到subExps中
		subExps[sourceId] = &subChan{
			regex: regex,
			ch:    ch,
		}

		// 遍历发布主题列表
		for topic := range pubTopics {
			// 如果主题匹配正则表达式
			if regex.MatchString(topic) {
				// 添加发布主题的消费者
				addPubConsumer(topic, sourceId, ch)
			}
		}
	} else {
		// 如果正则表达式为空
		// 直接添加通配符的消费者
		addPubConsumer(wildcard, sourceId, ch)
	}

	// 返回创建的通道
	return ch
}

// CloseSourceConsumerChannel 用于关闭指定sourceId的订阅表达式对应的通道，并从订阅表达式映射和发布主题的消费者通道中移除对应的订阅者
//
// 参数：
//   - topic - string类型，表示发布主题
//   - sourceId - string类型，表示订阅表达式的唯一标识
//
// 返回值：
//   - 无返回值
func CloseSourceConsumerChannel(topic string, sourceId string) {
	// 加锁
	mu.Lock()
	defer mu.Unlock()

	// 判断是否存在对应的订阅表达式
	if sc, exists := subExps[sourceId]; exists {
		// 关闭订阅表达式对应的通道
		close(sc.ch)
		// 从订阅表达式映射中删除该订阅表达式
		delete(subExps, sourceId)
		// 遍历所有发布主题
		for _, c := range pubTopics {
			// 从发布主题对应的消费者通道中移除订阅者
			removePubConsumer(topic, sourceId, c)
		}
	} else {
		// 如果不存在订阅表达式，则判断是否存在对应的发布主题
		if sinkConsumerChannels, exists := pubTopics[topic]; exists {
			// 从发布主题对应的消费者通道中移除订阅者
			removePubConsumer(topic, sourceId, sinkConsumerChannels)
		}
	}
}

// RemovePub 从全局的pubTopics中移除指定主题的消费者通道
//
// 参数：
//   - topic string - 要移除的消费者通道对应的主题
//
// 注意：
//   - 该函数通过互斥锁确保并发安全
func RemovePub(topic string) {
	// 加锁，确保并发安全
	mu.Lock()
	defer mu.Unlock()

	// 检查是否存在该主题的消费者通道
	if sinkConsumerChannels, exists := pubTopics[topic]; exists {
		// 消费者通道的引用计数减一
		sinkConsumerChannels.count -= 1
		// 如果消费者通道中的消费者列表为空且引用计数也为零
		if len(sinkConsumerChannels.consumers) == 0 && sinkConsumerChannels.count == 0 {
			// 从全局的pubTopics中删除该主题
			delete(pubTopics, topic)
		}
	}
}

// Produce 是一个生产消息到指定主题的函数
//
//   - ctx 是流处理上下文
//   - topic 是消息的主题名称
//   - data 是待发送的消息内容，是一个包含键值对的 map，其中键为字符串类型，值为空接口类型
func Produce(ctx api.StreamContext, topic string, data map[string]interface{}) {
	// 调用 doProduce 函数进行消息生产
	// 参数1: 上下文对象 ctx
	// 参数2: 主题 topic
	// 参数3: 使用 api.NewDefaultSourceTupleWithTime 函数创建一个带有时间戳的默认源元组
	// 其中的 data 参数为传入的消息数据
	// 其中的第二个参数为包含主题信息的 map，键为 "topic"，值为传入的 topic
	// 其中的第三个参数为当前时间戳，通过 conf.GetNow() 获取
	doProduce(ctx, topic, api.NewDefaultSourceTupleWithTime(data, map[string]interface{}{"topic": topic}, conf.GetNow()))
}

// ProduceUpdatable 函数根据给定的 StreamContext、topic、data、rowkind 和 keyval 参数，生成一个可更新的 Tuple 并进行生产
//
// 参数：
//
//	ctx api.StreamContext：流上下文对象
//	topic string：消息的主题
//	data map[string]interface{}：消息的数据
//	rowkind string：行的类型（例如：'INSERT'、'UPDATE'、'DELETE'）
//	keyval interface{}：主键值
//
// 返回值：无
func ProduceUpdatable(ctx api.StreamContext, topic string, data map[string]interface{}, rowkind string, keyval interface{}) {
	// 调用 doProduce 函数，传入 ctx、topic 和一个 UpdatableTuple 结构体指针作为参数
	doProduce(ctx, topic, &UpdatableTuple{
		// 创建 UpdatableTuple 结构体，其中 DefaultSourceTuple 字段使用 api.NewDefaultSourceTupleWithTime 函数初始化
		// data 参数作为 UpdatableTuple 的数据字段
		// map[string]interface{}{"topic": topic} 作为 UpdatableTuple 的其他元数据字段
		// conf.GetNow() 获取当前时间作为时间戳
		DefaultSourceTuple: api.NewDefaultSourceTupleWithTime(data, map[string]interface{}{"topic": topic}, conf.GetNow()),
		// Rowkind 字段设置为传入的 rowkind 参数
		Rowkind: rowkind,
		// Keyval 字段设置为传入的 keyval 参数
		Keyval: keyval,
	})
}

// doProduce 函数接收一个 StreamContext 类型的上下文、一个字符串类型的主题 topic 和一个 SourceTuple 类型的数据 data，
// 并向对应的发布主题广播该数据。
// 如果指定的主题不存在于 pubTopics 映射中，则直接返回。
// 函数首先通过 StreamContext 获取日志记录器，然后加读锁确保并发安全。
// 接着，遍历指定主题的所有消费者，通过 select 语句将数据广播给它们。
// 如果广播成功，将记录调试日志；如果规则停止，则停止等待；如果广播失败，将记录错误日志。
func doProduce(ctx api.StreamContext, topic string, data api.SourceTuple) {
	// 查找对应的发布主题
	c, exists := pubTopics[topic]
	if !exists {
		return
	}

	// 获取日志记录器
	logger := ctx.GetLogger()

	// 加读锁
	mu.RLock()
	defer mu.RUnlock()

	// 广播给所有消费者
	// broadcast to all consumers
	for name, out := range c.consumers {
		select {
		case out <- data:
			// 广播成功，记录日志
			logger.Debugf("memory source broadcast from topic %s to %s done", topic, name)
		case <-ctx.Done():
			// 规则停止，停止等待
			// rule stop so stop waiting
		default:
			// 广播失败，记录错误日志
			logger.Errorf("memory source topic %s drop message to %s", topic, name)
		}
	}
}

// ProduceError 函数用于在给定主题下生产错误消息，并将错误消息广播给所有消费者
//
// 参数：
//   - ctx    api.StreamContext：流上下文对象，用于获取日志记录器等
//   - topic  string：要生产错误消息的主题
//   - err    error：要发送的错误信息
//
// 返回值：
//   - 无返回值
func ProduceError(ctx api.StreamContext, topic string, err error) {
	// 检查是否存在该主题
	c, exists := pubTopics[topic]
	if !exists {
		return
	}

	// 获取日志记录器
	logger := ctx.GetLogger()

	// 加读锁
	mu.RLock()
	defer mu.RUnlock()

	// 广播错误给所有消费者
	// broadcast to all consumers
	for name, out := range c.consumers {
		select {
		// 将错误信息发送给消费者
		case out <- &xsql.ErrorSourceTuple{Error: err}:
			logger.Debugf("memory source broadcast error from topic %s to %s done", topic, name)
		// 如果规则停止，则停止等待
		case <-ctx.Done():
			// rule stop so stop waiting
		default:
			// 如果消费者无法接受错误消息，则记录错误信息
			logger.Errorf("memory source topic %s drop message to %s", topic, name)
		}
	}
}

// addPubConsumer 函数用于向特定主题（topic）的pubConsumers中添加一个消息消费者（channel）
// 参数：
//   - topic: string类型，指定要添加消费者的主题
//   - sourceId: string类型，指定消费者的唯一标识（sourceId）
//   - ch: chan api.SourceTuple类型，指定消费者用于接收消息的通道
//
// 返回值：
//   - 函数无返回值
func addPubConsumer(topic string, sourceId string, ch chan api.SourceTuple) {
	// 定义一个指向pubConsumers的指针sinkConsumerChannels
	var sinkConsumerChannels *pubConsumers

	// 判断pubTopics中是否已存在该topic对应的pubConsumers
	if c, exists := pubTopics[topic]; exists {
		// 如果存在，则将对应的pubConsumers赋值给sinkConsumerChannels
		sinkConsumerChannels = c
	} else {
		// 如果不存在，则创建一个新的pubConsumers实例，并将其赋值给sinkConsumerChannels
		sinkConsumerChannels = &pubConsumers{
			consumers: make(map[string]chan api.SourceTuple),
		}
		// 将sinkConsumerChannels添加到pubTopics中，以topic为键
		pubTopics[topic] = sinkConsumerChannels
	}

	// 判断sinkConsumerChannels的consumers中是否已存在该sourceId对应的channel
	if _, exists := sinkConsumerChannels.consumers[sourceId]; exists {
		// 如果存在，则打印警告日志
		conf.Log.Warnf("create memory source consumer for %s which is already exists", sourceId)
	} else {
		// 如果不存在，则将ch添加到sinkConsumerChannels的consumers中，以sourceId为键
		sinkConsumerChannels.consumers[sourceId] = ch
	}
}

// removePubConsumer 从pubConsumers中移除指定sourceId的消费者
//
// 参数：
//   - topic string - 消息的topic
//   - sourceId string - 消费者的sourceId
//   - c *pubConsumers - pubConsumers的指针，存储了topic与消费者的映射关系
//
// 返回值：
//   - 无返回值，该函数会直接修改传入的pubConsumers结构体
func removePubConsumer(topic string, sourceId string, c *pubConsumers) {
	// 如果 sourceId 在 c.consumers 中存在
	// if _, exists := c.consumers[sourceId]; exists { //
	// 从 c.consumers 中删除 sourceId
	delete(c.consumers, sourceId) // 如果sourceId不存在，则delete什么也不会做, 外围的判断不需要
	//}

	// 如果 c.consumers 为空，并且 c.count 也为 0
	if len(c.consumers) == 0 && c.count == 0 {
		// 从 pubTopics 中删除 topic
		delete(pubTopics, topic)
	}
}

// Reset For testing only
func Reset() {
	pubTopics = make(map[string]*pubConsumers)
	subExps = make(map[string]*subChan)
}
