module github.com/Dzarlax-AI/evening-news/go-background

go 1.25.6

require (
	github.com/Dzarlax-AI/evening-news/go-ingestion v0.0.0
	github.com/Dzarlax-AI/evening-news/go-processing v0.0.0
	github.com/Dzarlax-AI/evening-news/go-scheduler v0.0.0
	github.com/Dzarlax-AI/evening-news/go-worker v0.0.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.11.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace github.com/Dzarlax-AI/evening-news/go-ingestion => ../go-ingestion

replace github.com/Dzarlax-AI/evening-news/go-processing => ../go-processing

replace github.com/Dzarlax-AI/evening-news/go-scheduler => ../go-scheduler

replace github.com/Dzarlax-AI/evening-news/go-worker => ../go-worker
