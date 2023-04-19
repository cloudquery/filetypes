package json

import "github.com/apache/arrow/go/v12/arrow/memory"

type Option func(*Client)

type Client struct {
	mem memory.Allocator
}

func NewClient(options ...Option) (*Client, error) {
	c := &Client{
		mem: memory.DefaultAllocator,
	}
	for _, option := range options {
		option(c)
	}

	return c, nil
}
