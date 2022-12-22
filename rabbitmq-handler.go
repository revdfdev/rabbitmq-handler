package rabbitmqhandler

import "github.com/wagslane/go-rabbitmq"

type RabbitMQHandler struct {
	connection *rabbitmq.Conn
}

func NewRabbitMqHandler(uri string) (*RabbitMQHandler, error) {
	connection, err := rabbitmq.NewConn(uri, func(co *rabbitmq.ConnectionOptions) {
		co.Config = rabbitmq.Config{
			Heartbeat:  10,
			FrameSize:  10512,
			ChannelMax: 1000,
			Locale:     "en_US",
		}
	})

	if err != nil {
		return nil, err
	}

	return &RabbitMQHandler{
		connection: connection,
	}, nil
}

func (r *RabbitMQHandler) Consume(
	exchangeName string,
	routingKey string,
	queueName string,
	deadLetterExchange string,
	deadLetterRoutingKey string,
	handler func(d rabbitmq.Delivery) (action rabbitmq.Action)) error {
	consumer, err := rabbitmq.NewConsumer(r.connection, handler, queueName,
		rabbitmq.WithConsumerOptionsExchangeName(exchangeName),
		rabbitmq.WithConsumerOptionsBinding(
			rabbitmq.Binding{
				RoutingKey: routingKey,
				BindingOptions: rabbitmq.BindingOptions{
					NoWait: false,
					Args: rabbitmq.Table{
						"x-dead-letter-exchange":    deadLetterExchange,
						"x-dead-letter-routing-key": deadLetterRoutingKey,
						"x-message-ttl":             "10000",
						"x-max-length":              "1000",
						"x-max-length-bytes":        "1000000",
						"x-max-priority":            "10",
					},
					Declare: true,
				},
			},
		),

		rabbitmq.WithConsumerOptionsConsumerAutoAck(false))

	if err != nil {
		return err
	}

	defer consumer.Close()

	return nil
}

func (r *RabbitMQHandler) DeadLetter(deadLetterExchange string, deadLetterRoutingKey string, handler func(d rabbitmq.Delivery) (action rabbitmq.Action)) error {
	consumer, err := rabbitmq.NewConsumer(r.connection, handler, deadLetterExchange,
		rabbitmq.WithConsumerOptionsExchangeName(deadLetterExchange),
		rabbitmq.WithConsumerOptionsBinding(
			rabbitmq.Binding{
				RoutingKey: deadLetterRoutingKey,
				BindingOptions: rabbitmq.BindingOptions{
					NoWait: false,
					Args: rabbitmq.Table{
						"x-dead-letter-exchange":    deadLetterExchange,
						"x-dead-letter-routing-key": deadLetterRoutingKey,
						"x-message-ttl":             "10000",
						"x-max-length":              "1000",
						"x-max-length-bytes":        "1000000",
						"x-max-priority":            "10",
					},
					Declare: true,
				},
			},
		),

		rabbitmq.WithConsumerOptionsConsumerAutoAck(false))

	if err != nil {
		return err
	}

	defer consumer.Close()

	return nil
}

func (r *RabbitMQHandler) Publish(exchangeName string, routingKey string, message []byte) error {
	publisher, err := rabbitmq.NewPublisher(
		r.connection,
		rabbitmq.WithPublisherOptionsExchangeName(exchangeName),
		rabbitmq.WithPublisherOptionsExchangeKind("direct"),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
	)

	if err != nil {
		return err
	}

	defer publisher.Close()

	err = publisher.Publish(
		message,
		[]string{routingKey},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsContentEncoding("utf-8"),
		rabbitmq.WithPublishOptionsExchange(exchangeName),
	)

	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQHandler) PublishWithHeaders(exchangeName string, routingKey string, message []byte, headers map[string]interface{}) error {
	publisher, err := rabbitmq.NewPublisher(
		r.connection,
		rabbitmq.WithPublisherOptionsExchangeName(exchangeName),
		rabbitmq.WithPublisherOptionsExchangeKind("direct"),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
	)

	if err != nil {
		return err
	}

	defer publisher.Close()

	err = publisher.Publish(
		message,
		[]string{routingKey},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsContentEncoding("utf-8"),
		rabbitmq.WithPublishOptionsExchange(exchangeName),
		rabbitmq.WithPublishOptionsHeaders(headers),
	)

	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQHandler) Close() error {
	return r.connection.Close()
}
