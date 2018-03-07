package gmq

import (
	"github.com/streadway/amqp"
)

/* ================================================================================
 * Message Client
 * qq group: 582452342
 * email   : 2091938785@qq.com
 * author  : 美丽的地球啊 - mliu
 * ================================================================================ */

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 服务器选项
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
type ServerOption struct {
	Username string
	Password string
	Host     string
	Port     int
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * IMessage消息接口
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
type IMessage interface {
	Publish(exchange, exchangeType, routingKey, body string) error
	Consume(exchange, exchangeType, queueName string, args ...string) (<-chan amqp.Delivery, error)
	Close() error
}
