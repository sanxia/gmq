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
	}
)

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 初始化RabbitMqTransport
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func NewRabbitMqTransport(channelName string, option RabbitMqOption) gevent.ITransport {
	rmqTransport := &rabbitMqTransport{
		routingKey: fmt.Sprintf("%s-%s", option.Exchange, channelName),
		queue:      fmt.Sprintf("%s", channelName),
	}

	rmqTransport.message = NewRabbitMq(option)

	return rmqTransport
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 实现gevent.ITransport接口
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqTransport) Load() *gevent.Event {
	var event *gevent.Event

	if deliverChan, err := s.message.Consume(s.routingKey, s.queue); err == nil {
		select {
		case msg := <-deliverChan:
			if string(msg.Body) != "" {
				msg.Ack(false)
				event = s.unpackMessage(string(msg.Body))
			}
		}

		/*
			for msg := range deliverChan {
				if string(msg.Body) != "" {
					msg.Ack(false)
					event = s.unpackMessage(string(msg.Body))
				}
			}
		*/
	} else {
		log.Printf("RabbitMqTransport get err: %v", err)
	}

	return event
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 实现gevent.ITransport接口
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqTransport) Store(event *gevent.Event) {
	if event != nil {
		if eventJson := s.packMessage(event); len(eventJson) > 0 {
			if err := s.message.Publish(s.routingKey, eventJson); err != nil {
				log.Printf("RabbitMqTransport put err: %v", err)
			}
		}
	}
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
