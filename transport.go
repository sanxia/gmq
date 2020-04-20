package gmq

import (
	"fmt"
	"log"
)

import (
	"github.com/sanxia/gevent"
)

type (
	rabbitMqTransport struct {
		routingKey    string
		queue         string
		message       IRabbitMessage
		serialization gevent.ISerialization
		option        RabbitMqOption
	}
)

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 初始化RabbitMqTransport
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func NewRabbitMqTransport(channelName string, option RabbitMqOption) gevent.ITransport {
	rmqTransport := &rabbitMqTransport{
		routingKey:    fmt.Sprintf("%s-%s", option.Exchange, channelName),
		queue:         fmt.Sprintf("%s", channelName),
		serialization: gevent.NewDefaultSerialization(),
		option:        option,
	}

	rmqTransport.message = NewRabbitMq(option)

	return rmqTransport
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * gevent.ITransport load impl
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqTransport) Load(callback func(*gevent.Event) error) error {
	deliverChan, err := s.message.Consume(s.routingKey, s.queue)
	if err == nil {
		for msg := range deliverChan {
			if len(msg.Body) > 0 {

				if callback != nil {
					if event := s.serialization.Deserialize(string(msg.Body)); event != nil {
						if err := callback(event); err == nil {
							if !s.option.IsAutoAck {
								msg.Ack(false)
							}
						} else {
							if !s.option.IsAutoAck {
								msg.Nack(false, true)
							}
						}
					}
				}

			}
		}
	}

	return err
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * gevent.ITransport store impl
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqTransport) Store(event *gevent.Event) error {
	if event != nil {
		if eventJson := s.serialization.Serialize(event); len(eventJson) > 0 {
			if err := s.message.Publish(s.routingKey, eventJson); err != nil {
				log.Printf("RabbitMqTransport message publish err: %#v", err)
				return err
			}
		}
	}

	return nil
}
