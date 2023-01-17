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
	c.defaults()

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

func (cl *Client) defaults() {
	if cl.Delimiter == 0 {
		cl.Delimiter = ','
	}
}
