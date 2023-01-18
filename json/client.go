package json

type Option func(*Client)

type Client struct{}

func NewClient(options ...Option) (*Client, error) {
	c := &Client{}
	for _, option := range options {
		option(c)
	}

	return c, nil
}
