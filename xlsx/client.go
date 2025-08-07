package xlsx

type Options func(*Client)

// Client is a csv client.
type Client struct {
}

func NewClient(options ...Options) (*Client, error) {
	c := &Client{}

	for _, option := range options {
		option(c)
	}

	return c, nil
}
