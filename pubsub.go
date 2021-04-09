package goatee

import (
	"log"
	"strconv"
	"sync"
	"time"
	"github.com/aas-spec/mlog"
	"github.com/gomodule/redigo/redis"
)

type Message struct {
	Type    string
	Channel string
	Data    []byte
}

type Data struct {
	Channel   string `json:"channel"`
	Payload   string `json:"payload"`
	CreatedAt string `json:"created_at"`
}

type RedisClient struct {
	conn redis.Conn
	redis.PubSubConn
	sync.Mutex
}

type Client interface {
	Receive() (message Message)
}

func NewRedisClient(config RedisConfig) (*RedisClient, error) {
	curHost :=  config.Host + ":" + strconv.Itoa(config.Port)
	conn, err := redis.Dial("tcp", curHost)
	if err != nil {
		log.Printf("Error dialing redis pubsub: %s", err)
		return nil, err
	}

	pubsub, _ := redis.Dial("tcp", curHost)
	client := RedisClient{conn, redis.PubSubConn{pubsub}, sync.Mutex{}}

	mlog.LLog(9, "Subscribed to Redis on: ", curHost)

	go func() {
		for {
			time.Sleep(200 * time.Millisecond)
			client.Lock()
			client.conn.Flush()
			client.Unlock()
		}
	}()

	go client.PubsubHub()

	h.rclient = &client
	h.rconn = conn

	return &client, nil
}

func (client *RedisClient) Receive() Message {
	switch message := client.PubSubConn.Receive().(type) {
	case redis.Message:
		return Message{"message", message.Channel, message.Data}
	case redis.Subscription:
		return Message{message.Kind, message.Channel, []byte(strconv.Itoa(message.Count))}
	}
	return Message{}
}

func (client *RedisClient) PubsubHub() {
	data := Data{}
	for {
		message := client.Receive()
		if message.Type == "message" {
			mlog.LPrintf(5, "Receive data: %s %s", message.Channel, string(message.Data) )
			data.Channel = message.Channel
			data.Payload = string(message.Data);
			h.broadcast <- &data
			if DEBUG {
				log.Printf("Received: %s", message)
			}
		}
	}
}
