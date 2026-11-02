module github.com/Dzarlax-AI/evening-news/go-web

go 1.26.10

require github.com/jackc/pgx/v5 v5.11.0

require golang.org/x/sys v0.49.0 // indirect

require (
	github.com/Dzarlax-AI/evening-news/go-tools v0.0.0
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/crypto v0.58.0
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/text v0.43.0 // indirect
)

replace github.com/Dzarlax-AI/evening-news/go-tools => ../go-tools
