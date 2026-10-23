package migrations

import "embed"

//go:embed 001_initial.sql
var Files embed.FS
