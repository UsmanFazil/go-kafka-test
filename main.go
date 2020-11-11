package main 

import (
	"time"
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
	"github.com/gin-gonic/gin"
)

var writer *kafka.Writer


func pinger(ctx *gin.Context){
	
	val := []byte("new msg is here")
	err := Push(writer ,context.Background(), val, val)
	ctx.JSON(200, gin.H{
		"message": "ping",
		"err": err,
	})
}

// Producer function (push message in kafka topic)
func Push(w *kafka.Writer,parent context.Context, key []byte, value []byte)(error){
	message := kafka.Message{
		Key: key,
		Value: value,
		Time: time.Now(),
	}

	return w.WriteMessages(parent,message)
}

// function to set kafka writer configuration
func Configure(brokersLink []string, clientId string, topic string)(w *kafka.Writer, err error){
	
	dialer := &kafka.Dialer{
		Timeout: 10 *time.Second,
		ClientID: clientId,
	}

	config := kafka.WriterConfig{
		Brokers: brokersLink,
		Topic: topic,
		Dialer: dialer,
		WriteTimeout:     10 * time.Second,
		ReadTimeout:      10 * time.Second,
		CompressionCodec: snappy.NewCompressionCodec(), 
	}

	w = kafka.NewWriter(config)
	writer = w 
	return w, nil

}

// consumer function 
func consumerfunc(){
	// to consume messages
	topic := "testTopic"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		fmt.Println("failed to dial leader:", err)
	}
	conn.SetReadDeadline(time.Now().Add(10*time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message
	for {
		_, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b))
	}

	if err := batch.Close(); err != nil {
		fmt.Println("failed to close batch:", err)
	}

	if err := conn.Close(); err != nil {
		fmt.Println("failed to close connection:", err)
	}
}

func main (){
	brokers := []string{"localhost:9092"}
	Configure(brokers,"x","testTopic")
	r := gin.Default()
	r.GET("/ping", pinger)
	r.Run() 

}