package config

const ConfigFile = "./config/" + "config.yaml"

type Config struct {
	User     string `yaml:"user"`
	DBname   string `yaml:"dbname"`
	Sslmode  string `yaml:"sslmode"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
}
