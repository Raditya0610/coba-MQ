package main

import (
	"context"
	"database/sql"
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
)

func initMySQL() {
	var err error
	// Ganti dengan detail koneksi MySQL Anda
	dsn := "user:@tcp(localhost:3306)/dbname"
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
	connection, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	channel, err = connection.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}

	ctx = context.Background()
}

func main() {
	initMySQL()
	initRabbitMQ()

	router := gin.Default()
	router.POST("/send", sendHandler)
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

	// Simpan data ke MySQL
	query := "INSERT INTO messages (id, name) VALUES (?, ?)"
	_, err := db.Exec(query, data.ID, data.Name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save to database"})
		return
	}

	// Kirim pesan ke RabbitMQ
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
