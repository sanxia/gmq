package gmq

import (
	"fmt"
	"strings"
)

import (
	"github.com/streadway/amqp"
)

/* ================================================================================
 * Rabbitmq Message Queue Client
 * qq group: 582452342
 * email   : 2091938785@qq.com
 * author  : 美丽的地球啊 - mliu
 * ================================================================================ */

const (
	EXCHANGE_TYPE_DIRECT string = "direct"
	EXCHANGE_TYPE_FANOUT string = "fanout"
	EXCHANGE_TYPE_TOPIC  string = "topic"
)

var (
	exchangeTypes []string
)

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * rabbitMqClient数据结构
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
type rabbitMqClient struct {
	option     RabbitMqOption
	connection *amqp.Connection
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * RabbitMqOption选项
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
type RabbitMqOption struct {
	Server    ServerOption
	Vhost     string
	IsDurable bool
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 初始化
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func init() {
	exchangeTypes = []string{EXCHANGE_TYPE_DIRECT, EXCHANGE_TYPE_FANOUT, EXCHANGE_TYPE_TOPIC}
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 初始化RabbitMq Client
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func NewRabbitMq(option RabbitMqOption) IMessage {
	client := &rabbitMqClient{
		option: option,
	}

	return client
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 判断交换类型是否正确
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func isExchangeType(exchangeType string) bool {
	isSuccess := false
	for _, _exchangeType := range exchangeTypes {
		if strings.ToLower(_exchangeType) == strings.ToLower(exchangeType) {
			isSuccess = true
			break
		}
	}

	return isSuccess
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 发送消息
 * exchangeType: direct | fanout | topic
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (c *rabbitMqClient) Publish(exchange, exchangeType, routingKey, body string) error {
	//connection to server
	err := c.openConnection()
	if err != nil {
		return err
	}
	defer c.Close()

	channel, err := c.connection.Channel()
	if err != nil {
		return err
	}

	if isSuccess := isExchangeType(exchangeType); !isSuccess {
		exchangeType = EXCHANGE_TYPE_DIRECT
	}

	err = channel.ExchangeDeclare(
		exchange,           // name
		exchangeType,       // type
		c.option.IsDurable, // durable
		false,              // auto-deleted
		false,              // internal
		false,              // noWait
		nil,                // arguments
	)
	if err != nil {
		return err
	}

	err = channel.Publish(
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(body),
			DeliveryMode: amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:     0,
		},
	)

	return err
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 接收消息
 * exchangeType: direct | fanout | topic
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (c *rabbitMqClient) Consume(exchange, exchangeType, queueName string, args ...string) (<-chan amqp.Delivery, error) {
	//connection to server
	err := c.openConnection()
	if err != nil {
		return nil, err
	}
	defer c.connection.Close()

	channel, err := c.connection.Channel()
	if err != nil {
		return nil, err
	}

	//args
	bindingKey := ""
	tag := ""

	argsCount := len(args)
	if argsCount > 0 {
		bindingKey = args[0]
	}

	if argsCount > 1 {
		tag = args[1]
	}

	if isSuccess := isExchangeType(exchangeType); !isSuccess {
		exchangeType = EXCHANGE_TYPE_DIRECT
	}

	//exchange declare
	err = channel.ExchangeDeclare(
		exchange,           // name of the exchange
		exchangeType,       // type
		c.option.IsDurable, // durable
		false,              // auto-deleted，delete when complete
		false,              // internal
		false,              // noWait
		nil,                // arguments
	)
	if err != nil {
		return nil, err
	}

	//queue declare
	queue, err := channel.QueueDeclare(
		queueName,          // name
		c.option.IsDurable, // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)

	if err != nil {
		return nil, err
	}

	//binding queue and bingingKey to exchange
	err = channel.QueueBind(
		queue.Name, // name of the queue
		bindingKey, // bindingKey routing key
		exchange,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, err
	}

	return channel.Consume(
		queue.Name, // queue
		tag,        // consumerTag,
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 连接消息服务器
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (c *rabbitMqClient) openConnection() error {
	connectionString := fmt.Sprintf("amqp://%s:%s@%s:%d%s",
		c.option.Server.Username,
		c.option.Server.Password,
		c.option.Server.Host,
		c.option.Server.Port,
		c.option.Vhost,
	)

	connection, err := amqp.Dial(connectionString)
	if err != nil {
		return err
	}

	c.connection = connection

	return nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 关闭
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (c *rabbitMqClient) Close() error {
	err := c.connection.Close()

	return err
}
