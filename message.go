package gmq

import (
	amqp_api "github.com/streadway/amqp"
)

/* ================================================================================
 * Message Client
 * qq group: 582452342
 * email   : 2091938785@qq.com
 * author  : 美丽的地球啊 - mliu
 * ================================================================================ */

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * IMessage消息接口
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
type IMessage interface {
	Publish(exchange, exchangeType, routingKey, body string) error
	Consume(exchange, exchangeType, routingKey, queueName string, args ...string) (<-chan amqp_api.Delivery, error)
}
