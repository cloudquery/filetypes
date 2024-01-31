package parquet

type Options func(*Client)

// Client is a parquet client.
type Client struct {
	spec ParquetSpec
}

func NewClient(options ...Options) (*Client, error) {
	c := &Client{}
	for _, option := range options {
		option(c)
	}

	return c, nil
}

func WithSpec(spec ParquetSpec) Options {
	return func(c *Client) {
		c.spec = spec
	}
}
