# Contributing to CloudQuery

:+1::tada: First off, thanks for taking the time to contribute! :tada::+1:

This is the repository for CloudQuery FileTypes utility module. If you are looking for CloudQuery CLI and plugins take a look at our [monorepo](https://github.com/cloudquery/cloudquery)

## Links

- [Homepage](https://cloudquery.io)
- [Documentation](https://docs.cloudquery.io)
- [CLI and Plugins Monorepo](https://github.com/cloudquery/cloudquery)
- [Discord](https://cloudquery.io/discord)

## Development

### Tests

Run `make test` to run all unit-tests

To update snapshot testing run:

```
UPDATE_SNAPSHOTS=true make test
```

### Lint

We use `golangci-lint` as our linter. Configuration available in [./golangci.yml] to run lint `make lint`
