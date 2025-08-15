# Go Event Library

A unified Go library for event-driven messaging that supports both Apache Kafka and Amazon SQS with a consistent API interface.

## 🚀 Features

- 🔄 **Unified Interface**: Single API for both Kafka and SQS
- ⚙️ **Functional Options**: Clean configuration using functional options pattern
- 🛡️ **Type Safety**: Strong typing with custom types for topics and groups
- 📝 **Error Handling**: Comprehensive error handling and logging
- 🔧 **Auto Queue Management**: Automatic queue creation for SQS
- 📋 **FIFO Support**: Built-in support for SQS FIFO queues
- 👥 **Consumer Groups**: Kafka consumer group support
- 🔒 **Graceful Shutdown**: Proper resource cleanup and graceful shutdown
- 📦 **Message Unmarshaling**: Built-in JSON unmarshaling helpers
- 🔌 **Easy Integration**: Simple setup and integration

## 📦 Installation

```bash
go get github.com/andryhardiyanto/go-event
```

## 🏗️ Dependencies

- **Kafka**: IBM Sarama client (`github.com/IBM/sarama`)
- **SQS**: AWS SDK v2 (`github.com/aws/aws-sdk-go-v2/service/sqs`)
- **Logging**: Custom go-logger package (`github.com/andryhardiyanto/go-logger`)

## 🚀 Quick Start

### Kafka Example

#### Publisher

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/andryhardiyanto/go-event"
    "github.com/andryhardiyanto/go-event/kafka"
    "github.com/andryhardiyanto/go-logger"
    "github.com/IBM/sarama"
)

func main() {
    logger := goLogger.New()
    
    // Create Kafka publisher
    publisher, err := kafka.NewPublisher(
        event.WithLogger(logger),
        event.WithKafkaPublisherBroker("localhost:9092"),
        event.WithKafkaPublisherTimeout(10*time.Second),
        event.WithKafkaPublisherRetryMax(3),
        event.WithKafkaPublisherSuccesses(true),
        event.WithKafkaPublisherRequiredAcks(sarama.WaitForAll),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer publisher.Close()
    
    // Publish message
    publisher.Publish(context.Background(),
        event.WithSubscribeOptionTopic(event.Topic("user-events")),
        event.WithPayload(map[string]interface{}{
            "user_id": "123",
            "action":  "login",
        }),
    )
}
```

#### Subscriber

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"
    
    "github.com/andryhardiyanto/go-event"
    "github.com/andryhardiyanto/go-event/kafka"
    "github.com/andryhardiyanto/go-logger"
)

type UserEvent struct {
    UserID string `json:"user_id"`
    Action string `json:"action"`
}

func main() {
    logger := goLogger.New()
    
    // Create Kafka subscriber
    subscriber := kafka.NewSubscriber(
        event.WithLogger(logger),
        event.WithKafkaSubscriberBrokers([]string{"localhost:9092"}),
        event.WithKafkaSubscriberTimeout(30*time.Second),
    )
    
    // Subscribe to topic
    subscriber.Subscribe(
        event.WithSubscribeOptionTopic(event.Topic("user-events")),
        event.WithSubscribeOptionGroupIDKafka(event.Group("user-service")),
        event.WithSubscribeOptionHandler(func(ctx context.Context, message interface{}) error {
            var userEvent UserEvent
            if err := event.Unmarshal(message, &userEvent); err != nil {
                return err
            }
            
            log.Printf("Received: %+v", userEvent)
            return nil
        }),
    )
    
    // Start consuming
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    if err := subscriber.Start(ctx); err != nil {
        log.Fatal(err)
    }
    
    // Wait for interrupt signal
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    <-c
    
    subscriber.Close()
}
```

### SQS Example

#### Publisher

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/andryhardiyanto/go-event"
    "github.com/andryhardiyanto/go-event/sqs"
    "github.com/andryhardiyanto/go-logger"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {
    logger := goLogger.New()
    
    // Create AWS config
    cfg, err := config.LoadDefaultConfig(context.TODO())
    if err != nil {
        log.Fatal(err)
    }
    
    sqsClient := sqs.NewFromConfig(cfg)
    
    // Create SQS publisher
    publisher := sqs.NewPublisher(
        event.WithLogger(logger),
        event.WithSqsClient(sqsClient),
        event.WithSqsFIFO(true), // Enable FIFO queues
    )
    defer publisher.Close()
    
    // Publish message
    publisher.Publish(context.Background(),
        event.WithPublishTopic(event.Topic("user-events")),
        event.WithPublishPayload(map[string]interface{}{
            "user_id": "123",
            "action":  "login",
        }),
        event.WithPublisherSqsUseFifo(true),
        event.WithPublisherSqsGroupID("user-group"),
    )
}
```

#### Subscriber

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"
    
    "github.com/andryhardiyanto/go-event"
    "github.com/andryhardiyanto/go-event/sqs"
    "github.com/andryhardiyanto/go-logger"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
)

type UserEvent struct {
    UserID string `json:"user_id"`
    Action string `json:"action"`
}

func main() {
    logger := goLogger.New()
    
    // Create AWS config
    cfg, err := config.LoadDefaultConfig(context.TODO())
    if err != nil {
        log.Fatal(err)
    }
    
    sqsClient := sqs.NewFromConfig(cfg)
    
    // Create SQS subscriber
    subscriber := sqs.NewSubscriber(
        event.WithLogger(logger),
        event.WithSqsClient(sqsClient),
        event.WithSqsSubscriberTimeout(30*time.Second),
        event.WithSqsFIFO(true),
    )
    
    // Subscribe to topic
    subscriber.Subscribe(
        event.WithSubscribeOptionTopic(event.Topic("user-events")),
        event.WithSubscribeOptionHandler(func(ctx context.Context, message interface{}) error {
            var userEvent UserEvent
            if err := event.Unmarshal(message, &userEvent); err != nil {
                return err
            }
            
            log.Printf("Received: %+v", userEvent)
            return nil
        }),
    )
    
    // Start consuming
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    if err := subscriber.Start(ctx); err != nil {
        log.Fatal(err)
    }
    
    // Wait for interrupt signal
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    <-c
    
    subscriber.Close()
}
```

## ⚙️ Configuration Options

### Event Options

```go
// Logger configuration
event.WithLogger(logger)

// Kafka configuration
event.WithKafkaClient(kafkaClient)
event.WithKafkaPublisherTimeout(30 * time.Second)
event.WithKafkaSubscriberTimeout(10 * time.Second)
event.WithKafkaFIFO(true)

// SQS configuration
event.WithSqsClient(sqsClient)
event.WithSqsUseFifo(true)
event.WithSqsUseDlq(true)                    // Enable Dead Letter Queue
event.WithSqsMaxReceiveCount(3)              // Max retries before DLQ
event.WithSqsUseRedrivePermission(true)      // Enable redrive permissions
event.WithSqsQueueAttributeNameReceiveMessageWaitTimeSeconds(20)
event.WithSqsQueueAttributeNameVisibilityTimeout(60)
event.WithSqsSubscriberMaxNumberOfMessages(10)
event.WithSqsSubscriberWaitTimeSeconds(20)
```

### Publish Options

```go
// Common publish options
event.WithPublishTopic(event.Topic("my-topic"))
event.WithPublishPayload(data)

// SQS specific publish options
event.WithPublisherSqsGroupID("my-group")
```

### Subscribe Options

```go
// Common subscribe options
event.WithSubscribeOptionTopic(event.Topic("my-topic"))
event.WithSubscribeOptionHandler(handlerFunc)

// Kafka specific subscribe options
event.WithSubscribeOptionGroupIDKafka(event.Group("my-consumer-group"))
```

## 📦 Message Unmarshaling

The library provides a helper function to unmarshal messages from both Kafka and SQS:

```go
func handleMessage(ctx context.Context, message interface{}) error {
    var event MyEventStruct
    if err := event.Unmarshal(message, &event); err != nil {
        return fmt.Errorf("failed to unmarshal message: %w", err)
    }
    
    // Process the event
    return processEvent(event)
}
```

The `Unmarshal` function automatically detects whether the message is from:
- **Kafka**: `*sarama.ConsumerMessage`
- **SQS**: `types.Message`

And extracts the JSON payload accordingly.

## 🏗️ Architecture
```text
┌─────────────────────────────────────────────────────────────┐
│                    Go Event Library                        │
├─────────────────────────────────────────────────────────────┤
│                  Unified Interface                         │
│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
│  │    Publisher    │    │           Subscriber            │ │
│  │   Interface     │    │          Interface              │ │
│  └─────────────────┘    └─────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│              Implementation Layer                          │
│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
│  │  Kafka Package  │    │         SQS Package            │ │
│  │  ┌───────────┐  │    │  ┌───────────┐ ┌─────────────┐  │ │
│  │  │Publisher  │  │    │  │Publisher  │ │ Subscriber  │  │ │
│  │  └───────────┘  │    │  └───────────┘ └─────────────┘  │ │
│  │  ┌───────────┐  │    │                                 │ │
│  │  │Subscriber │  │    │                                 │ │
│  │  └───────────┘  │    │                                 │ │
│  └─────────────────┘    └─────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                External Dependencies                       │
│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
│  │  IBM Sarama     │    │        AWS SDK v2               │ │
│  │  (Kafka Client) │    │      (SQS Client)               │ │
│  └─────────────────┘    └─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## 🔧 Core Types

```go
// Basic types
type Topic string
type Group string
type SubscriberHandler func(context.Context, any) error

// Interfaces
type Publisher interface {
    Publish(ctx context.Context, opts ...PublishOption)
    Close() error
}

type Subscriber interface {
    Subscribe(opts ...SubscribeOption)
    Close() error
    Start(ctx context.Context) error
}
```

## 📋 Best Practices

1. **Always use context**: Pass context for cancellation and timeouts
2. **Graceful shutdown**: Always call `Close()` on publishers and subscribers
3. **Error handling**: Handle errors appropriately in message handlers
4. **Resource management**: Use defer statements for cleanup
5. **Configuration**: Validate configuration before starting services
6. **Logging**: Use structured logging for better observability


## Contributing

We welcome contributions! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## Support

If you encounter any issues or have questions, please open an issue on GitHub.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.