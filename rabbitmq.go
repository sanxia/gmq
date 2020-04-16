package gmq

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

import (
	amqp_api "github.com/streadway/amqp"
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
	conn       *amqp_api.Connection
	errChan    chan error
	retryCount int
	mu         sync.Mutex
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * RabbitMqOption选项
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
type RabbitMqOption struct {
	Username     string
	Password     string
	Ip           string
	Port         int
	VirtualHost  string
	Exchange     string
	ExchangeType string
	IsAutoAck    bool
	IsPersistent bool
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
func NewRabbitMq(option RabbitMqOption) IRabbitMessage {
	if len(option.Ip) == 0 {
		option.Ip = "127.0.0.1"
	}

	if option.Port <= 0 {
		option.Port = 5672
	}

	if len(option.VirtualHost) == 0 {
		option.VirtualHost = "/"
	}

	if len(option.Exchange) == 0 {
		option.Exchange = "__gmq_default__"
	}

	if isSuccess := isExchangeType(option.ExchangeType); !isSuccess {
		option.ExchangeType = EXCHANGE_TYPE_DIRECT
	}

	client := &rabbitMqClient{
		option:     option,
		errChan:    make(chan error, 0),
		retryCount: 0,
	}

	if err := client.openConnection(); err != nil {
		panic("rabbitmq conn err")
	}

	go client.monitorConnection()

	return client
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 发送消息
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqClient) Publish(routingKey, body string) error {
	channel, err := s.conn.Channel()
	if err != nil {
		s.errorHandler(err)
		return err
	}

	err = channel.ExchangeDeclare(
		s.option.Exchange,     // 交换机名称
		s.option.ExchangeType, // 交换机类型
		s.option.IsPersistent, // 是否持久化
		false,                 // auto-deleted
		false,                 // internal
		false,                 // noWait
		nil,                   // arguments
	)
	if err != nil {
		s.errorHandler(err)
		return err
	}

	msg := amqp_api.Publishing{
		Body:        []byte(body), //消息体内容
		ContentType: "text/plain",
		Priority:    0,
		Timestamp:   time.Now(),
	}

	// DeliveryMode: amqp_api.Persistent
	// 1=non-persistent(Transient), 2=persistent(Persistent)
	if s.option.IsPersistent {
		msg.DeliveryMode = amqp_api.Persistent
	} else {
		msg.DeliveryMode = amqp_api.Transient
	}

	err = channel.Publish(
		s.option.Exchange, // 交换机名称
		routingKey,        // 路由键
		false,             // mandatory
		false,             // immediate
		msg,
	)

	if err != nil {
		s.errorHandler(err)
	}

	return err
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 接收消息
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqClient) Consume(routingKey, queueName string, args ...string) (<-chan amqp_api.Delivery, error) {
	channel, err := s.conn.Channel()
	if err != nil {
		s.errorHandler(err)
		return nil, err
	}

	//声明交换机
	err = channel.ExchangeDeclare(
		s.option.Exchange,     // 交换机名称
		s.option.ExchangeType, // 交换机类型
		s.option.IsPersistent, // 是否持久化
		false,                 // auto-deleted，delete when complete
		false,                 // internal
		false,                 // noWait
		nil,                   // arguments
	)
	if err != nil {
		s.errorHandler(err)
		return nil, err
	}

	//声明队列
	_, err = channel.QueueDeclare(
		queueName,             // 队列名称
		s.option.IsPersistent, // 是否持久化
		false,                 // autoDelete when unused
		false,                 // exclusive
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		s.errorHandler(err)
		return nil, err
	}

	//队列和路由键绑定到交换机
	err = channel.QueueBind(
		queueName,         // queue anme
		routingKey,        // binding route key
		s.option.Exchange, // exchange name
		false,             // noWait
		nil,               // arguments
	)
	if err != nil {
		s.errorHandler(err)
		return nil, err
	}

	tag := ""
	if len(args) > 0 {
		tag = args[0]
	}

	//返回消息通道
	return channel.Consume(
		queueName,          // queue anme
		tag,                // tag,
		s.option.IsAutoAck, // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 打开连接
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqClient) openConnection() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errResult error
	if connection, err := s.connectionServer(); err == nil {
		s.conn = connection
	} else {
		retriedCount := 0

		if s.retryCount > 0 {
			for retriedCount < s.retryCount {
				log.Printf("reconnection retried:%d, error: %v", retriedCount, err)

				if connection, err = s.connectionServer(); err == nil {
					s.conn = connection
					break
				}

				retriedCount += 1
				time.Sleep(5 * time.Second)
			}
			errResult = fmt.Errorf("%s", "connection error")
		} else {
			for {
				log.Printf("reconnection retried:%d, error: %v", retriedCount, err)

				if connection, err = s.connectionServer(); err == nil {
					s.conn = connection
					break
				}

				retriedCount += 1
				time.Sleep(5 * time.Second)
			}
		}
	}

	return errResult
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 连接消息服务器
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqClient) connectionServer() (*amqp_api.Connection, error) {
	connectionString := fmt.Sprintf("amqp://%s:%s@%s:%d%s",
		s.option.Username,
		s.option.Password,
		s.option.Ip,
		s.option.Port,
		s.option.VirtualHost,
	)

	return amqp_api.Dial(connectionString)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 监视连接状态
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqClient) monitorConnection() {
	for {
		select {
		case err := <-s.errChan:
			if err != nil {
				log.Printf("monitor connection err: %#v", err)

				if err := s.openConnection(); err == nil {
					log.Printf("monitor reconnection OK")
				}
			}
		}
	}
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 错误处理器
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *rabbitMqClient) errorHandler(err error) {
	if opErr, isOk := err.(*net.OpError); isOk {
		if strings.ToUpper(opErr.Net) == "TCP" {
			s.errChan <- err
		}
	}
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 判断交换类型是否正确
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func isExchangeType(exchangeType string) bool {
	isSuccess := false

	if len(exchangeType) > 0 {
		for _, _exchangeType := range exchangeTypes {
			if strings.ToLower(_exchangeType) == strings.ToLower(exchangeType) {
				isSuccess = true
				break
			}
		}
	}

	return isSuccess
}
