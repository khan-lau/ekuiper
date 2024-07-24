// Copyright 2023-2024 emy120115@gmail.com
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
	"bytes"
	"compress/zlib"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/lf-edge/ekuiper/internal/compressor"
	"github.com/lf-edge/ekuiper/internal/io/utils/kbfilter"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type redis2Tdb struct {
	conf       *redisPubConfig
	conn       *redis.Client
	compressor message.Compressor

	results []string
	mux     sync.Mutex
	filter  *kbfilter.SinkFilterAction // 过滤器
}

func (r *redis2Tdb) Validate(props map[string]interface{}) error {
	cfg := &redisPubConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Db < 0 || cfg.Db > 15 {
		return fmt.Errorf("redisPub db should be in range 0-15")
	}
	if cfg.Channel == "" {
		return fmt.Errorf("redisPub sink is missing property channel")
	}
	if cfg.Compression != "" {
		r.compressor, err = compressor.GetCompressor(cfg.Compression)
		if err != nil {
			return fmt.Errorf("invalid compression method %s", cfg.Compression)
		}
	}
	if cfg.ResendChannel == "" {
		cfg.ResendChannel = cfg.Channel
	}
	r.conf = cfg
	return nil
}

func (r *redis2Tdb) Ping(_ string, props map[string]interface{}) error {
	if err := r.Configure(props); err != nil {
		return err
	}
	r.conn = redis.NewClient(&redis.Options{
		Addr:     r.conf.Address,
		Username: r.conf.Username,
		Password: r.conf.Password,
		DB:       r.conf.Db,
	})
	if err := r.conn.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("Ping Redis failed with error: %v", err)
	}
	return nil
}

func (r *redis2Tdb) Configure(props map[string]interface{}) error {
	return r.Validate(props)
}

func (r *redis2Tdb) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("redisPub sink opening")
	r.conn = redis.NewClient(&redis.Options{
		Addr:     r.conf.Address,
		Username: r.conf.Username,
		Password: r.conf.Password,
		DB:       r.conf.Db,
	})
	return nil
}

func (r *redis2Tdb) Collect(ctx api.StreamContext, item interface{}) error {
	return r.collectWithChannel(ctx, item, r.conf.Channel)
}

func (r *redis2Tdb) CollectResend(ctx api.StreamContext, item interface{}) error {
	return r.collectWithChannel(ctx, item, r.conf.ResendChannel)
}

func (r *redis2Tdb) collectWithChannel(ctx api.StreamContext, item interface{}, channel string) error {
	logger := ctx.GetLogger()
	logger.Debugf("receive %+v", item)

	switch d := item.(type) {
	case []byte:
		var trandatas []map[string]interface{}
		r.mux.Lock()
		if err := json.Unmarshal(d, &trandatas); err != nil {
			logger.Error(err)
			return err
		}

		redisMessage := kbfilter.CustomSinkMessage{}
		for _, msg := range trandatas {
			msgs := r.filter.Watch(ctx, msg) // 清洗数据, 因为有些情况要补记录, 所以可能会产生多条
			for _, item := range msgs {
				encode, err := redisMessage.Encode(item)
				if err != nil {
					logger.Error(err)
					continue
				}
				r.results = append(r.results, string(encode))
			}
		}
		r.mux.Unlock()
	case []map[string]interface{}:
		redisMessage := kbfilter.CustomSinkMessage{}
		r.mux.Lock()
		for _, msg := range d {
			msgs := r.filter.Watch(ctx, msg) // 清洗数据, 因为有些情况要补记录, 所以可能会产生多条
			for _, item := range msgs {
				encode, err := redisMessage.Encode(item)
				if err != nil {
					logger.Error(err)
					continue
				}
				r.results = append(r.results, string(encode))
			}
		}
		r.mux.Unlock()
	case map[string]interface{}:
		redisMessage := kbfilter.CustomSinkMessage{}
		r.mux.Lock()
		msgs := r.filter.Watch(ctx, d) // 清洗数据, 因为有些情况要补记录, 所以可能会产生多条
		for _, item := range msgs {
			encode, err := redisMessage.Encode(item)
			if err != nil {
				logger.Error(err)
			}
			r.results = append(r.results, string(encode))
		}
		r.mux.Unlock()
	default:
		return fmt.Errorf("unrecognized format of %s", item)
	}

	if len(r.results) > 0 {
		if err := r.PublishgRedisWithMsg(ctx, r.results, channel); err != nil {
			logger.Error(err)
			return err
		} else {
			r.results = make([]string, 0)
		}
	} else {
		logger.Error("file sink receive non byte data")
	}

	return nil
}

func (r *redis2Tdb) PublishgRedisWithMsg(ctx api.StreamContext, message []string, channel string) error {
	msgList := strings.Join(message, ",")

	logger := ctx.GetLogger()
	logger.Debugf("redisPub sink publish %s", msgList)

	buf := new(bytes.Buffer)
	writer := zlib.NewWriter(buf)
	// _, err := writer.Write([]byte("," + msgList))
	_, err := writer.Write([]byte(msgList))
	if err != nil {
		return err
	}
	err = writer.Close()
	if err != nil {
		return err
	}
	compressedData := buf.Bytes()
	err = r.conn.Publish(ctx, channel, compressedData).Err()
	if err != nil {
		return err
	}
	return nil
}

func (r *redis2Tdb) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing redisPub sink")
	if r.conn != nil {
		err := r.conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func Redis2Tdb() api.Sink {
	return &redis2Tdb{filter: kbfilter.NewSinkFilterAction()}
}
