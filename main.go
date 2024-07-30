package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"messaggio_demo/config"
	"messaggio_demo/driver"
	"net/http"
	"sync"

	"github.com/IBM/sarama"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

// создать Хэндлеры
// сохранить в БД
// Записать в кафка и прочесть
// docker run --name demo -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=messaggio -d postgres:latest
type db struct {
	db driver.DB
}

type Message struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

type MessageStates struct {
	ID        string `db:id`
	Processed bool   `db:"processed"`
}

var (
	responseChannels = make(map[string]chan *sarama.ConsumerMessage)
	mu               sync.Mutex
)

func NewKafka() {
	producer, err := sarama.NewSyncProducer([]string{kafka: 9092}, nil)
	if err != nil {
		log.Fatalf("Failed to create producer:%v", err)
	}
	defer producer.Close()

	consumer, err := sarama.NewConsumer([]string{"kafka:9092"}, nil)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	defer consumer.Close()

	partConsumer, err := consumer.ConsumePartition("pong", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failde to consume partition: %v", err)
	}
	defer partConsumer.Close()
	// чтение сообщения из Kafka
	go func() {
		for {
			select {
			case msg, ok := <-partConsumer.Messages():
				if !ok {
					log.Println("Channel closed, exiting gotoutine")
					return
				}
				responseID := string(msg.Key)
				mu.Lock()
				ch, exists := responseChannels[responseID]
				if exists {
					ch <- msg
					delete(responseChannels, responseID)
				}
				mu.Unlock()
			}
		}
	}()
}

func LoadConfig() (*config.Config, error) {
	cfg := &config.Config{}
	data, err := ioutil.ReadFile(config.ConfigFile)
	if err != nil {
		return nil, errors.Wrap(err, "read failed")
	}

	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, errors.Wrap(err, "error unmarshalling")
	}

	return cfg, nil
}

func MessageHandler(w http.ResponseWriter, r *http.Request) {
	var message Message

	if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
		http.Error(w, "error decoding", http.StatusBadRequest)
	}
	CheckMessage(message)

}
func (db *db) CheckMessage(msg Message) bool {
	row := db.QueryRow("select * from users where text=$2", msg.Name)
	err := row.Scan(&msg.ID, &msg.Name, &msg.Value)
	if err != nil {
		return false
	}

}

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config file: %v", err)
	}

	db, err := driver.GetDB(cfg)
	if err != nil {
		log.Fatalf("Error connecting DB ")
	}

	r := mux.NewRouter()
	r.HandleFunc("/messages", MessageHandler).Methods("POST")
	log.Println("Server starting on: 8000")
	log.Fatal(http.ListenAndServe(":8000", r))
}
