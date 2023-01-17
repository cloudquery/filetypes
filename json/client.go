package json

import "io"

type FileOption func(*Client)

// Client is a csv client.
type Client struct {
	Writer io.Writer
	Reader io.Reader
}

func NewClient(options ...FileOption) (*Client, error) {
	c := &Client{}
	for _, option := range options {
		option(c)
	}

	return c, nil
}

func WithWriter(w io.Writer) FileOption {
	return func(c *Client) {
		c.Writer = w
	}
}

func WithReader(r io.Reader) FileOption {
	return func(c *Client) {
		c.Reader = r
	}
}
