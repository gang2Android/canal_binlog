package mq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type RabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	// 队列名称
	QueueName string
	// 交换机
	Exchange string
	// key
	Key string
	// 链接信息
	Mqurl string
}

func (r RabbitMQ) failOnError(err error, str string) {
	if err != nil {
		fmt.Print(str + err.Error())
	}
}

func NewRabbitMQ(mqUrl, queueName string, exchange string, key string) *RabbitMQ {
	rabbitmq := &RabbitMQ{
		QueueName: queueName,
		Exchange:  exchange,
		Key:       key,
		Mqurl:     mqUrl,
	}
	var err error
	// 创建rabbitmq链接
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnError(err, "创建链接错误")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnError(err, "获取channel失败")
	return rabbitmq
}

func NewRabbitMQPubSub(mqUrl, exchangeName string) *RabbitMQ {
	// 指定交换机名称， 而队列名称则为空即可
	rabbitmq := NewRabbitMQ(mqUrl, "", exchangeName, "")
	var err error
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnError(err, "failed to connect rabbitmq")
	// 获取channel
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnError(err, "failed to open a channel")
	return rabbitmq
}

// PublishPub 生产消息
func (r *RabbitMQ) PublishPub(message string) {
	// 尝试创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		// 交换机类型为广播类型
		"fanout",
		true,
		false,
		false,
		false,
		nil)
	r.failOnError(err, "failed to declare an exchange")
	// 发送消息
	err = r.channel.Publish(
		r.Exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	r.failOnError(err, "failed to send a message")
}

// ReceiveSub 消费消息
func (r *RabbitMQ) ReceiveSub(keyStr string) {
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"fanout",
		true,
		false,
		// YES表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		false,
		nil)
	r.failOnError(err, "failed to declare an exchange")
	// 创建队列
	q, err := r.channel.QueueDeclare(
		"", // 随机生产队列名称
		false,
		false,
		true,
		false,
		nil)
	// 绑定队列到exchange中
	r.failOnError(err, "failed to declare a queue")
	err = r.channel.QueueBind(
		q.Name,
		// 在Pub/Sub模式下，这里的key要为空
		"",
		r.Exchange,
		false,
		nil)

	// 消费消息
	message, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil)
	forever := make(chan bool)
	go func() {
		for d := range message {
			log.Printf(keyStr+"Receive a message: %s", d.Body)
		}
	}()
	log.Println("退出请按： Ctrl + c")
	<-forever
}
