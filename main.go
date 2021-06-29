package main

import (
	"canal_binlog/db_c"
	"canal_binlog/mq"
	"canal_binlog/pub"
)

func main() {
	// 搭建mq
	// docker pull rabbitmq:management
	// docker run -d -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin -p 15672:15672 -p 5672:5672 --name rabbitmq rabbitmq:management
	pub.Sub = mq.NewRabbitMQPubSub("amqp://admin:admin@127.0.0.1:5672/", "pro")
	go func() {
		pub.Sub.ReceiveSub("key")
	}()

	db_c.SetDbConfig(db_c.DbConfig{
		Addr:      "127.0.0.1:3306",
		User:      "root",
		Pwd:       "root",
		DB:        "pro",
		Table:     "list",
		Row:       "stock",
		BinLog:    "mysql-bin.002311",
		BinLogPos: 27688549,
	})
	db_c.Run()
}
