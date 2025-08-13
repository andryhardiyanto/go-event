package event

import (
	"context"
)

type Topic string
type Group string
type SubscriberHandler func(context.Context, any) error

func (t Topic) String() string {
	return string(t)
}

func (g Group) String() string {
	return string(g)
}

type Publisher interface {
	Publish(ctx context.Context, opts ...PublishOption)
	Close() error
}
type Subscriber interface {
	Subscribe(opts ...SubscribeOption)
	Close() error
	Start(ctx context.Context) error
}

type SubscriberKafkaConfig struct {
	Topic   Topic
	GroupID Group
	Handler SubscriberHandler
}
