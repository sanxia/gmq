package gmq

import (
	"fmt"
	"log"
)

import (
	"github.com/sanxia/gevent"
	"github.com/sanxia/glib"
)

type (
	rabbitMqTransport struct {
		routingKey string
		queue      string
		message    IRabbitMessage
		option     RabbitMqOption
	}
)

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 初始化RabbitMqTransport
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func NewRabbitMqTransport(channelName string, option RabbitMqOption) gevent.ITransport {
	rmqTransport := &rabbitMqTransport{
		routingKey: fmt.Sprintf("%s-%s", option.Exchange, channelName),
		queue:      fmt.Sprintf("%s", channelName),
		option:     option,
	}

	rmqTransport.message = NewRabbitMq(option)

	return rmqTransport
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 实现gevent.ITransport接口
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqTransport) Load(callback func(*gevent.Event) error) error {
	deliverChan, err := s.message.Consume(s.routingKey, s.queue)
	if err == nil {
		for msg := range deliverChan {
			if len(msg.Body) > 0 {

				if callback != nil {
					if event := s.unpackMessage(string(msg.Body)); event != nil {
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
 * 实现gevent.ITransport接口
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqTransport) Store(event *gevent.Event) error {
	if event != nil {
		if eventJson := s.packMessage(event); len(eventJson) > 0 {
			if err := s.message.Publish(s.routingKey, eventJson); err != nil {
				log.Printf("RabbitMqTransport message publish err: %#v", err)
				return err
			}
		}
	}

	return nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 打包事件数据
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqTransport) packMessage(event *gevent.Event) string {
	eventJson, _ := glib.ToJson(event)
	return eventJson
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 解包事件数据
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqTransport) unpackMessage(body string) *gevent.Event {
	var event *gevent.Event

	if len(body) > 0 {
		glib.FromJson(body, &event)
	}

	return event
}
