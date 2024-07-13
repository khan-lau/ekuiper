package pubsub

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io"
	"github.com/lf-edge/ekuiper/internal/io/custom_kafka"
	"github.com/lf-edge/ekuiper/internal/io/utils/kbfilter"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
	kafkago "github.com/segmentio/kafka-go"
)

type kafkaSub struct {
	reader    *kafkago.Reader
	offset    int64
	tlsConfig *tls.Config
}

type kafkaSourceConf struct {
	Brokers     string `json:"brokers"`
	Topic       string `json:"topic"`
	GroupID     string `json:"groupID"`
	Partition   int    `json:"partition"`
	MaxAttempts int    `json:"maxAttempts"`
	MaxBytes    int    `json:"maxBytes"`
}

func (c *kafkaSourceConf) validate() error {
	if len(strings.Split(c.Brokers, ",")) == 0 {
		return fmt.Errorf("brokers can not be empty")
	}
	return nil
}

func (c *kafkaSourceConf) GetReaderConfig(topic string) kafkago.ReaderConfig {
	conf.Log.Infof("kafka source conf: %#v, topic: %s", c, topic)
	if c.GroupID == "" || c.GroupID == "groupId" {
		return kafkago.ReaderConfig{
			Brokers:     strings.Split(c.Brokers, ","),
			Topic:       topic,
			Partition:   c.Partition,
			MaxBytes:    c.MaxBytes,
			MaxAttempts: c.MaxAttempts,
		}
	} else {
		return kafkago.ReaderConfig{
			Brokers:     strings.Split(c.Brokers, ","),
			GroupID:     c.GroupID,
			Topic:       topic,
			Partition:   c.Partition,
			MaxBytes:    c.MaxBytes,
			MaxAttempts: c.MaxAttempts,
		}
	}

}

func getSourceConf(props map[string]interface{}) (*kafkaSourceConf, error) {
	c := &kafkaSourceConf{
		MaxBytes:    1e6,
		MaxAttempts: 3,
	}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return nil, fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	return c, nil
}

func (s *kafkaSub) Configure(topic string, props map[string]interface{}) error {
	if len(topic) < 1 {
		conf.Log.Error("DataSource which indicates the topic should be defined")
		return fmt.Errorf("DataSource which indicates the topic should be defined")
	}
	kConf, err := getSourceConf(props)
	if err != nil {
		conf.Log.Errorf("kafka source config error: %v", err)
		return err
	}
	if err := kConf.validate(); err != nil {
		return err
	}
	tlsConfig, err := cert.GenTLSConfig(props, "kafka-source")
	if err != nil {
		conf.Log.Errorf("kafka tls conf error: %v", err)
		return err
	}
	saslConf, err := custom_kafka.GetSaslConf(props)
	if err != nil {
		conf.Log.Errorf("kafka sasl error: %v", err)
		return err
	}
	if err := saslConf.Validate(); err != nil {
		conf.Log.Errorf("kafka validate sasl error: %v", err)
		return err
	}
	mechanism, err := saslConf.GetMechanism()
	if err != nil {
		conf.Log.Errorf("kafka sasl mechanism error: %v", err)
		return err
	}
	readerConfig := kConf.GetReaderConfig(topic)
	conf.Log.Infof("topic: %s, brokers: %v", readerConfig.Topic, readerConfig.Brokers)
	readerConfig.Dialer = &kafkago.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsConfig,
		SASLMechanism: mechanism,
	}

	conn, err := kafkago.DialContext(context.Background(), "tcp", strings.Join(readerConfig.Brokers, ","))
	if err != nil {
		conf.Log.Errorf("Connect to Kafka failed: %v", err)
		return err
	}
	defer conn.Close()

	reader := kafkago.NewReader(readerConfig)
	s.reader = reader
	if err := s.reader.SetOffset(kafkago.LastOffset); err != nil {
		return err
	}
	conf.Log.Infof("kafka source got configured.")

	return nil
}

func (s *kafkaSub) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	defer s.reader.Close()
	logger := ctx.GetLogger()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msg, err := s.reader.ReadMessage(ctx)
		if err != nil {
			logger.Errorf("Recv kafka error %v", err)
			errCh <- err
			return
		}
		s.offset = msg.Offset

		// dataList, err := ctx.DecodeIntoList(msg.Value)
		// if err != nil {
		// 	logger.Errorf("unmarshal kafka message value err: %v", err)
		// 	consumer <- &xsql.ErrorSourceTuple{
		// 		Error: fmt.Errorf("can not decompress kafka message %v.", err),
		// 	}
		// 	// errCh <- err
		// 	return
		// }
		// for _, data := range dataList {
		// 	rcvTime := conf.GetNow()
		// 	consumer <- api.NewDefaultSourceTupleWithTime(data, nil, rcvTime)
		// }

		// built-in decode
		payload := msg.Value
		kafkaFormat := kbfilter.GetCustomSourceMessage().(message.Converter)
		resultsAny, err := kafkaFormat.Decode(payload)
		if resultsAny == nil || err != nil {
			return
		}
		results := resultsAny.([]map[string]interface{}) // 一次返回多条记录, 所以此处是一个map数组

		meta := make(map[string]interface{})
		meta["key"] = msg.Key
		meta["channel"] = msg.Topic

		tuples := make([]api.SourceTuple, 0, len(results))
		for _, result := range results {
			rcvTime := conf.GetNow()
			tuples = append(tuples, api.NewDefaultSourceTupleWithTime(result, meta, rcvTime))
		}
		io.ReceiveTuples(ctx, consumer, tuples)
	}
}

func (s *kafkaSub) Close(_ api.StreamContext) error {
	return nil
}

func (s *kafkaSub) Rewind(offset interface{}) error {
	conf.Log.Infof("set kafka source offset: %v", offset)
	offsetV := s.offset //nolint:staticcheck
	switch v := offset.(type) {
	case int64:
		offsetV = v
	case int:
		offsetV = int64(v)
	case float64:
		offsetV = int64(v)
	default:
		return fmt.Errorf("%v can't be set as offset", offset)
	}
	if err := s.reader.SetOffset(offsetV); err != nil {
		conf.Log.Errorf("kafka offset error: %v", err)
		return fmt.Errorf("set kafka offset failed, err:%v", err)
	}
	return nil
}

func (s *kafkaSub) GetOffset() (interface{}, error) {
	return s.offset, nil
}
func KafkaSub() api.Source {
	return &kafkaSub{}
}
