package app

import "log/slog"

// Build provenance, overwritten at link time with -ldflags -X (see the
// Dockerfile and Makefile). The defaults mark an unstamped local build.
//
// These live in app rather than a package of their own because app is the only
// consumer and they encode no decision. runtime/debug.ReadBuildInfo is not the
// source: .dockerignore excludes .git, so a container build would report no VCS
// information at all.
var (
	// version is a release tag or short commit describing the build.
	version = "dev"
	// commit is the full git SHA the binary was built from.
	commit = "none"
	// buildDate is the RFC3339 build timestamp.
	buildDate = "unknown"
)

// buildInfo renders provenance as one structured log group.
func buildInfo() slog.Value {
	return slog.GroupValue(
		slog.String("version", version),
		slog.String("commit", commit),
		slog.String("date", buildDate),
	)
}
