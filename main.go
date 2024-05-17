package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rabbitmq/amqp091-go"
)

var (
	db      *sql.DB
	channel *amqp091.Channel
	ctx     context.Context
	conn    *amqp091.Connection
)

func initMySQL() {
	var err error
	dsn := "root@tcp(localhost:3306)/cobamq"
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to ping MySQL: %v", err)
	}
}

func initRabbitMQ() {
	var err error
	conn, err = amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	channel, err = conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}

	// Declare exchange
	err = channel.ExchangeDeclare(
		"notification", // name of the exchange
		"direct",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare exchange: %v", err)
	}

	ctx = context.Background()
}

func main() {
	initMySQL()
	initRabbitMQ()
	defer db.Close()
	defer conn.Close()
	defer channel.Close()

	router := gin.Default()
	router.POST("/send", sendHandler)
	router.GET("/messages", getMessagesHandler)
	router.Run(":8080")
}

type RequestData struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func sendHandler(c *gin.Context) {
	var data RequestData
	if err := c.ShouldBindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	query := "INSERT INTO messages (name) VALUES (?)"
	_, err := db.Exec(query, data.Name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save to database"})
		return
	}

	message := amqp091.Publishing{
		Headers: amqp091.Table{
			"sample": "value",
		},
		Body: []byte("Hello " + data.Name),
	}
	err = channel.PublishWithContext(ctx,
		"notification", // exchange
		"email",        // routing key
		false,          // mandatory
		false,          // immediate
		message,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to publish message"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func getMessagesHandler(c *gin.Context) {
	rows, err := db.Query("SELECT id, name FROM messages")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch from database"})
		return
	}
	defer rows.Close()

	var messages []RequestData
	for rows.Next() {
		var msg RequestData
		if err := rows.Scan(&msg.ID, &msg.Name); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to scan database row"})
			return
		}
		messages = append(messages, msg)
	}

	for _, msg := range messages {
		fmt.Printf("ID: %d, Name: %s\n", msg.ID, msg.Name)
	}

	c.JSON(http.StatusOK, messages)
}
