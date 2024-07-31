package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"messaggio_demo/config"
	"messaggio_demo/driver"
	"messaggio_demo/model"
	"messaggio_demo/repository"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

// создать Хэндлеры
// сохранить в БД
// Записать в кафка и прочесть

func LoadConfig() (*config.Config, error) {
	cfg := &config.Config{}
	data, err := ioutil.ReadFile(config.ConfigFile)
	if err != nil {
		return nil, errors.Wrap(err, "read failed")
	}

	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "error unmarshalling")
	}

	return cfg, nil
}

func MessageHandler(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid Method", http.StatusMethodNotAllowed)
		return
	}
	message := new(model.Message)

	if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
		http.Error(w, "error decoding", http.StatusBadRequest)
		return
	}
	repository.InsertMsg(db, *message)

	msgInBytes, err := json.Marshal(message)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = PushOrderToQue("msg", msgInBytes)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"success": true,
		"msg":     "Order for " + message.ID + " placed successfully!",
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func ConnectProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	return sarama.NewSyncProducer(brokers, config)
}

func ConnectConsumer(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	return sarama.NewConsumer(brokers, config)
}

func PushOrderToQue(topic string, message []byte) error {
	brokers := []string{"localhost:9092"}

	producer, err := ConnectProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	// Create new message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	// Send message
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	log.Printf("Order is stored in topic(%s)/partition(%d)/offset(%d)\n",
		topic,
		partition,
		offset,
	)
	return nil
}

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config file: %v", err)
	}

	db, err := driver.GetDB(cfg)
	if err != nil {
		log.Fatalf("Error connecting DB: %v", err)
	}

	topic := "msg"
	msgCnt := 0

	worker, err := ConnectConsumer([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	fmt.Println("Consumer started")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCnt++
				fmt.Printf("Received order Count %d: | Topic(%s) | Message(%s)\n", msgCnt, string(msg.Topic), string(msg.Value))
				order := string(msg.Value)
				fmt.Printf("Brewing coffee for order: %s\n", order)
			case <-sigchan:
				fmt.Println("Interrupt detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
	fmt.Println("Processed", msgCnt, "messages")
	if err := worker.Close(); err != nil {
		panic(err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/messages", func(w http.ResponseWriter, r *http.Request) {
		MessageHandler(w, r, db)
	}).Methods("POST")

	log.Println("Server starting on: 8000")
	log.Fatal(http.ListenAndServe(":8000", r))
}
