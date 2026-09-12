// Package migrations embeds the goose SQL migrations so the binary can apply
// them at boot (POSTGRES_MIGRATE=true) without shipping loose files, following
// the same pattern as identity and expense.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
