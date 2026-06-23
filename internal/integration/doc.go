// Package integration holds build-tagged live tests that exercise the sink cores
// against real services (Kafka, Redis, Postgres, …) brought up by compose.yaml.
//
// The tests are gated behind `//go:build integration`, so the default
// `go test ./...` stays offline; run them with `make integration` (or
// scripts/integration.sh), which starts the stack, runs `go test -tags
// integration ./...`, and tears it down. Service addresses come from EVM_TEST_*
// env vars, defaulting to the compose.yaml port mappings on localhost.
package integration
