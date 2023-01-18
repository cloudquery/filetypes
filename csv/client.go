package csv

type Options func(*Client)

// Client is a csv client.
type Client struct {
	IncludeHeaders bool
	Delimiter      rune
}

func NewClient(options ...Options) (*Client, error) {
	c := &Client{
		Delimiter: ',',
	}
	for _, option := range options {
		option(c)
	}

	return c, nil
}

func WithHeader() Options {
	return func(c *Client) {
		c.IncludeHeaders = true
	}
}

func WithDelimiter(delimiter rune) Options {
	return func(c *Client) {
		c.Delimiter = delimiter
	}
}
