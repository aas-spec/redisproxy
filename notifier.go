package goatee

import (
	"log"
	"net/http"

	"github.com/aas-spec/mlog"
	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/websocket"
)

// type AuthFunc func(req *http.Request) bool
type RedisConfig struct {
	Host     string
	Port     int
	Password string
	Db       int
	Channel  string
}

type sockethub struct {
	// registered connections
	connections map[*connection]bool

	// inbound messages from connections
	broadcast chan *Data

	// register requests from connection
	register chan *connection

	// unregister request from connection
	unregister chan *connection

	// copy of the redis client
	rclient *RedisClient

	// Канал для подписки, к которому добавляется id  /channel/id
	channel string
	// copy of the redis connection
	rconn redis.Conn

	Auth func(req *http.Request) string
}

var h = sockethub{
	broadcast:   make(chan *Data),
	register:    make(chan *connection),
	unregister:  make(chan *connection),
	connections: make(map[*connection]bool),
}

func (h *sockethub) WsHandler(w http.ResponseWriter, r *http.Request) {
	var authId string

	if h.Auth != nil {
		authId = h.Auth(r)
	}
	// Проверяю, если не пустой id
	if authId != "" {
		ws, err := websocket.Upgrade(w, r, nil, 1024, 1024)

		if _, ok := err.(websocket.HandshakeError); ok {
			http.Error(w, "Not a websocket handshake", 400)
			return
		} else if err != nil {
			log.Printf("WsHandler error: %s", err.Error())
			return
		}

		c := &connection{sid: authId, send: make(chan *Data), ws: ws}
		h.register <- c

		go c.writer()
		go c.reader()
	} else {
		mlog.Print("Invalid Query")
		http.Error(w, "Invalid Query", 401)
	}
}

func (h *sockethub) Run() {
	for {
		select {
		case c := <-h.register:
			h.connections[c] = true
			h.rclient.Subscribe(h.channel + "/" + c.sid)
			mlog.LPrintf(6, "Connected: %s", c.sid)
		case c := <-h.unregister:
			if _, ok := h.connections[c]; ok {
				mlog.LPrintf(6, "Disconnected: %s", c.sid)
				h.rclient.Unsubscribe("/queue/" + c.sid)
				delete(h.connections, c)
				close(c.send)
			}
		case m := <-h.broadcast:
			for c := range h.connections {
				select {
				case c.send <- m:
					mlog.LPrintf(7, "broadcasting: %s", m.Payload)
				default:
					close(c.send)
					delete(h.connections, c)
				}
			}
		}
	}
}

func (h *sockethub) RegisterAuthFunc(AuthFunc func(req *http.Request) string) {
	aut := AuthFunc
	h.Auth = aut
}

func (h *sockethub) StartServer(config RedisConfig, webHost string) {
	//	conf := LoadConfig("config")
	client, err := NewRedisClient(config)
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()
	h.rclient = client
	h.channel = config.Channel
	go h.Run()

	http.HandleFunc("/", h.WsHandler)
	mlog.Printf("Starting server on: %s", webHost)

	err = http.ListenAndServe(webHost, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func CreateServer() sockethub {
	return h
}
