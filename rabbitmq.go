package gmq

import (
	"fmt"
	"log"
	"strings"
	"sync"
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
	retryCount int
	mu         sync.Mutex
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * RabbitMqOption选项
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
type RabbitMqOption struct {
	Username  string
	Password  string
	Host      string
	Port      int
	Vhost     string
	IsDurable bool
	IsSync    bool
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
	if len(option.Vhost) == 0 {
		option.Vhost = "/"
	}

	client := &rabbitMqClient{
		option:     option,
		retryCount: 10,
	}

	return client
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 发送消息
 * exchangeType: direct | fanout | topic
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqClient) Publish(exchange, exchangeType, routingKey, body string) error {
	if s.option.IsSync {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	conn, err := s.openConnection()
	if err != nil {
		return err
	}

	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return err
	}

	if isSuccess := isExchangeType(exchangeType); !isSuccess {
		exchangeType = EXCHANGE_TYPE_DIRECT
	}

	err = channel.ExchangeDeclare(
		exchange,           // 交换机名称
		exchangeType,       // 交换机类型
		s.option.IsDurable, // 是否持久化
		false,              // auto-deleted
		false,              // internal
		false,              // noWait
		nil,                // arguments
	)
	if err != nil {
		return err
	}

	msg := amqp.Publishing{
		Body:         []byte(body), //消息体内容
		ContentType:  "text/plain",
		DeliveryMode: amqp.Transient, // 1=non-persistent, 2=persistent
		Priority:     0,
		Timestamp:    time.Now(),
	}

	err = channel.Publish(
		exchange,   // 交换机名称
		routingKey, // 路由键
		false,      // mandatory
		false,      // immediate
		msg,
	)

	return err
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 接收消息
 * exchangeType: direct | fanout | topic
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqClient) Consume(exchange, exchangeType, routingKey, queueName, tag string) (<-chan amqp.Delivery, error) {
	if s.option.IsSync {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	conn, err := s.openConnection()
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if isSuccess := isExchangeType(exchangeType); !isSuccess {
		exchangeType = EXCHANGE_TYPE_DIRECT
	}

	//声明交换机
	err = channel.ExchangeDeclare(
		exchange,           // 交换机名称
		exchangeType,       // 交换机类型
		s.option.IsDurable, // 是否持久化
		false,              // auto-deleted，delete when complete
		false,              // internal
		false,              // noWait
		nil,                // arguments
	)
	if err != nil {
		return nil, err
	}

	//声明队列
	_, err = channel.QueueDeclare(
		queueName,          // 队列名称
		s.option.IsDurable, // 是否持久化
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
		queueName,  // 队列名称
		routingKey, // 绑定的路由键
		exchange,   // 交换机名称
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, err
	}

	//返回消息通道
	return channel.Consume(
		queueName, // 队列名称
		tag,       // 自定义Tag,
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 连接消息服务器
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqClient) openConnection() (*amqp.Connection, error) {
	connection, err := s.connectionServer()

	if err != nil {
		retriedCount := 0
		for retriedCount < s.retryCount {
			retriedCount += 1

			//重连
			log.Printf("reConnection %d amqp.Dial error: %v", retriedCount, err)

			time.Sleep(3 * time.Second)

			connection, err = s.connectionServer()
			if err == nil {
				break
			}
		}
	}

	return connection, err
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 连接消息服务器
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqClient) connectionServer() (*amqp.Connection, error) {
	connectionString := fmt.Sprintf("amqp://%s:%s@%s:%d%s",
		s.option.Username,
		s.option.Password,
		s.option.Host,
		s.option.Port,
		s.option.Vhost,
	)

	return amqp.Dial(connectionString)
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
