package parquet

import "github.com/apache/arrow/go/v12/arrow/memory"

type Options func(*Client)

// Client is a parquet client.
type Client struct {
	spec Spec
	mem  memory.Allocator
}

func NewClient(options ...Options) (*Client, error) {
	c := &Client{
		mem: memory.DefaultAllocator,
	}
	for _, option := range options {
		option(c)
	}

	return c, nil
}

func WithSpec(spec Spec) Options {
	return func(c *Client) {
		c.spec = spec
	}
}
