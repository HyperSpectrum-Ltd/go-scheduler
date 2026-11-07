# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Backoff calculation delegates to `go-domainkit/port.BackoffPolicy`
  (`RetryPolicy.BackoffPolicy()`, `MaxRetries → MaxAttempts+1`).
- Added `ContractLock`, mapping the scheduler lock (`Acquire/Release/Extend`)
  to `go-domainkit/port.ExtendableLock` while keeping existing names.
- Go version unified to 1.25.0.

### Added

- Minimal CI workflow (`go vet` + `go test -race`).
