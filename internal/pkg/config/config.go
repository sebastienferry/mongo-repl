package config

import (
	"os"
	"regexp"

	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"gopkg.in/yaml.v2"
)

type FullReplConfig struct {
	// The batch size for replication
	BatchSize int `yaml:"batch"`
	// Update on duplicate key
	UpdateOnDuplicate bool `yaml:"update_on_duplicate"`
}

type IncrReplConfig struct {
	// The state of the replication
	State struct {
		Database   string `yaml:"db"`
		Collection string `yaml:"collection"`
	} `yaml:"state"`
}

type ReplConfig struct {
	// The address of the MongoDB server
	Source string `json:"Source" yaml:"source"`
	// The address of the MongoDB server
	Target string `json:"Target" yaml:"target"`

	// The list of databases to replicate
	Databases []string `yaml:"databases"`

	// Collection whitelist/blacklist
	Filters    map[string][]string `yaml:"filters"`
	FiltersIn  map[string]bool     `yaml:"-"`
	FiltersOut map[string]bool     `yaml:"-"`

	// The replication configuration
	Full FullReplConfig `yaml:"full"`
	Incr IncrReplConfig `yaml:"incr"`
}

type AppConfig struct {
	// Application logging configuration
	Logging struct {
		Level string `yaml:"level"`
	} `yaml:"logging"`

	// The replication configuration
	Repl ReplConfig `yaml:"repl"`
}

// NewConfig returns a new Config struct
func NewConfig() *AppConfig {
	return &AppConfig{}
}

var Current *AppConfig

func init() {
	Current = NewConfig()
	Current.LoadConfig()
}

// LoadConfig loads the configuration from a file
func (c *AppConfig) LoadConfig() error {

	// Fetch the environment variable
	configFilePath := os.Getenv("CONFIG_FILE_PATH")
	log.Debug("CONFIG_FILE_PATH: ", configFilePath)

	// Open the configuration file
	f, err := os.Open(configFilePath)
	if err != nil {
		log.Fatal("Error opening configuration file: ", err)
	}
	defer f.Close()

	// Decode the configuration file
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(c)
	if err != nil {
		log.Fatal("Error decoding configuration file: ", err)
	}

	// Override the log level if set in the environment
	if os.Getenv("LOG_LEVEL") != "" {
		c.Logging.Level = os.Getenv("LOG_LEVEL")
	}

	// Override the source and target if set in the environment
	if os.Getenv("SOURCE") != "" {
		c.Repl.Source = os.Getenv("SOURCE")
	}
	if os.Getenv("TARGET") != "" {
		c.Repl.Target = os.Getenv("TARGET")
	}

	// Initialize the filters
	c.Repl.FiltersIn = make(map[string]bool)
	for _, filter := range c.Repl.Filters["in"] {
		c.Repl.FiltersIn[filter] = true
	}

	c.Repl.FiltersOut = make(map[string]bool)
	for _, filter := range c.Repl.Filters["out"] {
		c.Repl.FiltersOut[filter] = true
	}

	return nil
}

func (c *AppConfig) LogConfig() {
	log.Info("MongoDB configuration:")
	log.Info("- Source: ", obfuscateCrendentials(c.Repl.Source))
	log.Info("- Target: ", obfuscateCrendentials(c.Repl.Target))
	log.Info("Databases to replicate:")
	for _, db := range c.Repl.Databases {
		log.Info("- ", db)
	}
}

// Considering the following structure for MongoDB connection string:
// "mongodb://<username>:<password>@<host>:<port>"
// The following function will replaces the username and password with "****"
func obfuscateCrendentials(mongoConnectionString string) string {
	// Find the username and password
	regexp := regexp.MustCompile(`mongodb:\/\/(.*):(.*)@`)
	matches := regexp.FindStringSubmatch(mongoConnectionString)
	if len(matches) == 3 {
		// Replace the username and password with "****"
		return regexp.ReplaceAllString(mongoConnectionString, "mongodb://****:****@")
	}
	return mongoConnectionString
}
