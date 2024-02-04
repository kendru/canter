# Canter: A triplestore inspired by Datomic

Canter aims to be a simple-to-use general-purpose database for managing data
that changes over time. It is heavily inspired by
[Datomic](https://www.datomic.com/), but it is currently focused on single-node
operation and does not implement sharding or any distributed features.

## Goals

- Provide an embedded triplestore as a Go library
- Provide a SPARQL endpoint for querying data externally
- Focus on performance over flexibility. For example, all predicates/attributes
must be declared with a specific type ahead of time.
- Allow time travel queries

## Developing

### Dependencies

- direnv
- git
- go

We provide a script in `scripts/dev-check` that verifies your local environment
is configured to work on Canter. This script will fail with a helpful error
message if any dependency is not met or is not correctly configured.

### Testing

Unit tests should follow the common Go pattern of living in the same package as
the application code that they test and should have a `_test.go` suffix. Unit
tests may be run at any time with the `unit-tests` script. These tests should
run quickly and not rely on any external dependencies.

Integration tests are kept under `tests/integration` and may be run with the
`integration-tests` script. These tests rely on the database and other Docker
dependencies expressed in `docker-compose.yml`. Setup and tear-down should
ensure that the tests run against the app in a fresh state.

### Git Config

We recommend the use of the provided git hooks in the `githooks` directory.
These can be installed by copying them into your `.git/hooks` directory. E.g.:
`cp githooks/* .git/hooks`.

### 🔧 General Configuration

Direnv will use the `.envrc` file in the root of the repo to set up a few handy
environment variables and will augment your `PATH` with the `script` directory,
so if your terminal is within this repo, you can run any script command
unqualified.

We use a tools file at `tools/tools.go` to force Go modules to download several
tools that are used in development. This avoids requiring global installations
of development tools. This file is never compiled because it depends on a
"tools" build tag.

### 📖 More Info

Oh, you want to read _more_ 😍? Then check out the `docs` directory.
