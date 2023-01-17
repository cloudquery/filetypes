package csv

import (
	"io"
)

type FileOption func(*Client)

// Client is a csv client.
type Client struct {
	Writer io.Writer
	Reader io.Reader
	// fileFormat     string
	IncludeHeaders bool
	Delimiter      rune
}

func NewClient(options ...FileOption) (*Client, error) {
	c := &Client{}
	for _, option := range options {
		option(c)
	}
	c.defaults()

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

func WithHeader(include bool) FileOption {
	return func(c *Client) {
		c.IncludeHeaders = include
	}
}

func WithDelimiter(delimiter rune) FileOption {
	return func(c *Client) {
		c.Delimiter = delimiter
	}
}

func (cl *Client) defaults() {
	if cl.Delimiter == 0 {
		cl.Delimiter = ','
	}
}
