package gmq

import (
	"fmt"
	"log"
	"strings"
	"time"
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
	//连接服务器
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
		exchange,           // 交换机名称
		exchangeType,       // 交换机类型
		c.option.IsDurable, // 是否持久化
		false,              // auto-deleted
		false,              // internal
		false,              // noWait
		nil,                // arguments
	)
	if err != nil {
		return err
	}

	err = channel.Publish(
		exchange,   // 交换机名称
		routingKey, // 路由键
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(body),   //消息体内容
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
	//连接服务器
	err := c.openConnection()
	if err != nil {
		return nil, err
	}

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

	//声明交换机
	err = channel.ExchangeDeclare(
		exchange,           // 交换机名称
		exchangeType,       // 交换机类型
		c.option.IsDurable, // 是否持久化
		false,              // auto-deleted，delete when complete
		false,              // internal
		false,              // noWait
		nil,                // arguments
	)
	if err != nil {
		return nil, err
	}

	//声明队列
	queue, err := channel.QueueDeclare(
		queueName,          // 队列名称
		c.option.IsDurable, // 是否持久化
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)

	if err != nil {
		return nil, err
	}

	//队列和路由键绑定到交换机
	err = channel.QueueBind(
		queue.Name, // 队列名称
		bindingKey, // 绑定的路由键
		exchange,   // 交换机名称
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, err
	}

	//返回消息通道
	return channel.Consume(
		queue.Name, // 队列名称
		tag,        // 自定义Tag,
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
		//每隔5秒重试连接
		log.Printf("amqp.Dial error: %v", err)
		time.Sleep(5 * time.Second)

		c.openConnection()
	} else {
		c.connection = connection
	}

	return nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 关闭连接
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (c *rabbitMqClient) Close() error {
	if c.connection == nil {
		return nil
	}

	return c.connection.Close()
}
