package consumer

import (
	"fmt"
	"strings"

	"github.com/nsqio/go-nsq"
)

type Consumer struct {
	client      *nsq.Consumer
	config      *nsq.Config
	nsqds       []string
	nsqlookupds []string
	channel     string
	topic       string
	level       nsq.LogLevel
	err         error
}

func NewConsumer(topic, channel string) *Consumer {
	return &Consumer{
		config:  nsq.NewConfig(),
		level:   nsq.LogLevelInfo,
		channel: channel,
		topic:   topic,
	}
}

func (c *Consumer) SetMap(options map[string]interface{}) {
	for k, v := range options {
		c.Set(k, v)
	}
}

func (c *Consumer) Set(option string, value interface{}) {
	switch option {
	case "topic":
		if s, ok := value.(string); ok {
			c.topic = s
		} else {
			c.err = fmt.Errorf("%q: expected string", option)
			return
		}
	case "channel":
		if s, ok := value.(string); ok {
			c.channel = s
		} else {
			c.err = fmt.Errorf("%q: expected string", option)
			return
		}
	case "nsqd":
		if s, ok := value.(string); ok {
			c.nsqds = []string{s}
		} else {
			c.err = fmt.Errorf("%q: expected string", option)
			return
		}
	case "nsqlookupd":
		if s, ok := value.(string); ok {
			c.nsqlookupds = []string{s}
		} else {
			c.err = fmt.Errorf("%q: expected string", option)
			return
		}
	case "nsqds":
		if s, err := split(value); err == nil {
			c.nsqds = s
		} else {
			c.err = fmt.Errorf("%q: %v", option, err)
			return
		}
	case "nsqlookupds":
		if s, err := split(value); err == nil {
			c.nsqlookupds = s
		} else {
			c.err = fmt.Errorf("%q: %v", option, err)
			return
		}
	default:
		if err := c.config.Set(option, value); err != nil {
			c.err = err
		}
	}
}

func (c *Consumer) Start(handler nsq.Handler) error {
	if c.err != nil {
		return c.err
	}

	client, err := nsq.NewConsumer(c.topic, c.channel, c.config)
	if err != nil {
		return err
	}
	c.client = client

	client.AddConcurrentHandlers(handler, 1)

	return c.connect()
}

func (c *Consumer) Stop() error {
	c.client.Stop()
	<-c.client.StopChan
	return nil
}

func (c *Consumer) connect() error {
	if len(c.nsqds) == 0 && len(c.nsqlookupds) == 0 {
		return fmt.Errorf(`nsqd/nsqlookupd address REQUIRED`)
	}

	if len(c.nsqds) > 0 {
		err := c.client.ConnectToNSQDs(c.nsqds)
		if err != nil {
			return err
		}
	}

	if len(c.nsqlookupds) > 0 {
		err := c.client.ConnectToNSQLookupds(c.nsqlookupds)
		if err != nil {
			return err
		}
	}

	return nil
}

func split(value interface{}) ([]string, error) {
	switch value.(type) {
	case []string:
		return value.([]string), nil
	case string:
		return strings.FieldsFunc(value.(string), func(r rune) bool {
			switch r {
			case ',', ' ':
				return true
			}
			return false
		}), nil
	default:
		return nil, fmt.Errorf("expected string or slice of strings")
	}
}
