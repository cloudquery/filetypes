package csv

type Options func(*Client)

// Client is a csv client.
type Client struct {
	// fileFormat     string
	IncludeHeaders bool
	Delimiter      rune
}

func NewClient(options ...Options) (*Client, error) {
	c := &Client{}
	for _, option := range options {
		option(c)
	}
	if c.Delimiter == 0 {
		c.Delimiter = ','
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
