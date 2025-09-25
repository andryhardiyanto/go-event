package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/andryhardiyanto/go-event"
	goLogger "github.com/andryhardiyanto/go-logger"

	"github.com/IBM/sarama"
)

type subscriber struct {
	configs      []config
	count        map[string]map[string]int
	broker       []string
	timeout      time.Duration
	logger       goLogger.Logger
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	KafkaVersion *sarama.KafkaVersion
}

type config struct {
	topic             event.Topic
	groupID           event.Group
	handler           event.SubscriberHandler
	RebalanceStrategy sarama.BalanceStrategy
}

func NewSubscriber(opts ...event.EventOption) event.Subscriber {
	if len(opts) == 0 {
		panic("no options provided")
	}

	cfg := &event.EventConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	if len(cfg.Kafka.Subscriber.Brokers) == 0 {
		panic("broker is nil")
	}

	if cfg.Logger == nil {
		panic("logger is nil")
	}

	if cfg.Kafka.Subscriber.Timeout == 0 {
		cfg.Kafka.Subscriber.Timeout = 5 * time.Second
	}

	return &subscriber{
		broker:       cfg.Kafka.Subscriber.Brokers,
		timeout:      cfg.Kafka.Subscriber.Timeout,
		count:        make(map[string]map[string]int),
		logger:       cfg.Logger,
		KafkaVersion: cfg.Kafka.Version,
	}
}

func (s *subscriber) Subscribe(opts ...event.SubscribeOption) {
	if len(opts) == 0 {
		panic("no options provided")
	}

	cfg := &event.SubscribeOptionConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.Topic.String() == "" {
		s.logger.Panic(context.Background(), "topic is required")
	}

	if cfg.Kafka.GroupID.String() == "" {
		s.logger.Panic(context.Background(), "group id is required")
	}

	if cfg.Handler == nil {
		s.logger.Panic(context.Background(), "handler is required")
	}

	topics, ok := s.count[cfg.Topic.String()]
	if !ok {
		topics = make(map[string]int)
		s.count[cfg.Topic.String()] = topics
	}

	if topics[cfg.Kafka.GroupID.String()] > 0 {
		s.logger.Panic(context.Background(), fmt.Sprintf("group id %s already exists for topic %s", cfg.Kafka.GroupID, cfg.Topic))
	}

	s.configs = append(s.configs, config{
		topic:             cfg.Topic,
		groupID:           cfg.Kafka.GroupID,
		handler:           cfg.Handler,
		RebalanceStrategy: cfg.Kafka.RebalanceStrategy,
	})

	topics[cfg.Kafka.GroupID.String()]++
}

func (s *subscriber) Start(parent context.Context) error {
	if len(s.configs) == 0 {
		return fmt.Errorf("no subscriptions registered")
	}

	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel

	for _, cfg := range s.configs {
		cfg := cfg // copy for closure
		s.wg.Add(1)

		go func() {
			defer s.wg.Done()

			sarCfg := sarama.NewConfig()
			if s.KafkaVersion != nil {
				sarCfg.Version = *s.KafkaVersion
			} else {
				sarCfg.Version = sarama.V3_6_1_0
			}
			if cfg.RebalanceStrategy != nil {
				sarCfg.Consumer.Group.Rebalance.Strategy = cfg.RebalanceStrategy
			} else {
				sarCfg.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategySticky()
			}
			sarCfg.Consumer.Offsets.Initial = sarama.OffsetOldest

			cg, err := sarama.NewConsumerGroup(s.broker, cfg.groupID.String(), sarCfg)
			if err != nil {
				s.logger.Error(ctx, fmt.Sprintf("create group err: %v", err))
				return
			}
			defer func() {
				if err := cg.Close(); err != nil {
					s.logger.Error(ctx, fmt.Sprintf("failed to close subscriber group for topic=%s groupID=%s: %s", cfg.topic.String(), cfg.groupID.String(), err.Error()))
				}
			}()

			handler := &consumerGroupHandler{
				handler: cfg.handler,
				topic:   cfg.topic,
				groupID: cfg.groupID,
				timeout: s.timeout,
				logger:  s.logger,
			}

			for {
				if err := cg.Consume(ctx, []string{cfg.topic.String()}, handler); err != nil {
					s.logger.Error(ctx, fmt.Sprintf("consume err: %v", err))
					select {
					case <-ctx.Done():
						return
					default:
						time.Sleep(5 * time.Second)
					}
				}
				if ctx.Err() != nil {
					return
				}
			}
		}()
	}
	return nil
}

func (s *subscriber) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	return nil
}

type consumerGroupHandler struct {
	handler event.SubscriberHandler
	topic   event.Topic
	groupID event.Group
	timeout time.Duration
	logger  goLogger.Logger
}

func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			h.logger.Info(session.Context(), fmt.Sprintf(
				"received message topic=%s groupID=%s partition=%d offset=%d key=%s",
				h.topic, h.groupID, msg.Partition, msg.Offset, string(msg.Key),
			))

			ctx, cancel := context.WithTimeout(session.Context(), h.timeout)
			err := h.handler(ctx, msg)
			cancel()

			if err != nil {
				h.logger.Error(session.Context(), fmt.Sprintf("error processing message: %v", err))
			} else {
				session.MarkMessage(msg, "")
			}
		}
	}
}
