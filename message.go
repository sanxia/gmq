package gmq

import (
	amqp_api "github.com/streadway/amqp"
)

/* ================================================================================
 * Rabbitmq Message Queue Client
 * qq group: 582452342
 * email   : 2091938785@qq.com
 * author  : 美丽的地球啊 - mliu
 * ================================================================================ */

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * IRabbitMessage消息接口
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
type IRabbitMessage interface {
	Publish(routingKey, body string) error
	Consume(routingKey, queueName string, args ...string) (<-chan amqp_api.Delivery, error)
}
