package config

import (
	"flag"
	"os"
	"regexp"

	"github.com/sebastienferry/mongo-repl/internal/pkg/log"
	"gopkg.in/yaml.v2"
)

var configFileArg = flag.String("c", "", "configuration file path")

func init() {
	flag.Parse()
}

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

	// The replication id
	Id string `yaml:"id"`
	// The address of the MongoDB server
	Source string `json:"source" yaml:"source"`
	// The address of the MongoDB server
	Target string `json:"target" yaml:"target"`

	// Features flags
	Features        []string        `yaml:"features"`
	FeaturesEnabled map[string]bool `yaml:"-"`

	// The list of databases to replicate
	Databases   []string        `yaml:"databases"`
	DatabasesIn map[string]bool `yaml:"-"`

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

var Current *AppConfig = NewConfig()

// LoadConfig loads the configuration from a file
func (c *AppConfig) LoadConfig() error {

	// Fetch the environment variable
	configFilePath := os.Getenv("CONFIG_FILE_PATH")
	log.Info("configuration file path: ", configFilePath)
	if configFilePath == "" {
		configFilePath = *configFileArg
	}

	// Open the configuration file
	f, err := os.Open(configFilePath)
	if err != nil {
		log.Fatal("error opening configuration file: ", err)
	}
	defer f.Close()

	// Decode the configuration file
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(c)
	if err != nil {
		log.Fatal("error decoding configuration file: ", err)
	}

	// Override the log level if set in the environment
	if os.Getenv("LOG_LEVEL") != "" {
		c.Logging.Level = os.Getenv("lOG_LEVEL")
	}

	// Ensure the ID is set
	if c.Repl.Id == "" {
		c.Repl.Id = "default"
	}

	// Override the source and target if set in the environment
	if os.Getenv("SOURCE") != "" {
		c.Repl.Source = os.Getenv("SOURCE")
	}
	if os.Getenv("TARGET") != "" {
		c.Repl.Target = os.Getenv("TARGET")
	}

	// Features
	c.Repl.FeaturesEnabled = make(map[string]bool)
	for _, feature := range c.Repl.Features {
		c.Repl.FeaturesEnabled[feature] = true
	}

	// Databases to replicate
	c.Repl.DatabasesIn = make(map[string]bool)
	for _, db := range c.Repl.Databases {
		c.Repl.DatabasesIn[db] = true
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
	log.Info("mongo configuration:")
	log.Info("- source: ", ObfuscateCrendentials(c.Repl.Source))
	log.Info("- target: ", ObfuscateCrendentials(c.Repl.Target))
	log.Info("databases to replicate:")
	for _, db := range c.Repl.Databases {
		log.Info("- ", db)
	}
}

// Considering the following structure for MongoDB connection string:
// "mongodb://<username>:<password>@<host>:<port>"
// The following function will replaces the username and password with "****"
func ObfuscateCrendentials(mongoConnectionString string) string {
	// Find the username and password
	regexp := regexp.MustCompile(`mongodb:\/\/(.*):(.*)@`)
	matches := regexp.FindStringSubmatch(mongoConnectionString)
	if len(matches) == 3 {
		// Replace the username and password with "****"
		return regexp.ReplaceAllString(mongoConnectionString, "mongodb://****:****@")
	}
	return mongoConnectionString
}
