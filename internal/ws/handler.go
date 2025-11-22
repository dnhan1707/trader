/*
This is the HTP handler allow users to connect
*/

package ws

import (
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
)

func NewHandler(hub *Hub, stockSubs chan string, indexSubs chan string) fiber.Handler {
	return websocket.New(func(c *websocket.Conn) {
		client := &Client{hub: hub, conn: c, send: make(chan []byte, 256)}
		client.hub.Register <- client

		go client.WritePump()
		// Pass both channels to ReadPump
		client.ReadPump(stockSubs, indexSubs)
	})
}
