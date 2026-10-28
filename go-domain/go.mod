module github.com/Dzarlax-AI/evening-news/go-domain

go 1.26.10

require github.com/jackc/pgx/v5 v5.11.0

require (
	codeberg.org/readeck/go-readability/v2 v2.1.3 // indirect
	github.com/PuerkitoBio/goquery v1.10.3 // indirect
	github.com/andybalholm/cascadia v1.3.3 // indirect
	github.com/go-shiori/dom v0.0.0-20230515143342-73569d674e1c // indirect
	github.com/gogs/chardet v0.0.0-20211120154057-b7413eaefb8f // indirect
	github.com/itlightning/dateparse v0.2.1 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/mmcdole/gofeed v1.3.0 // indirect
	github.com/mmcdole/goxpp v1.1.1-0.20240225020742-a0c311522b23 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
)

require (
	github.com/Dzarlax-AI/evening-news/go v0.0.0
	github.com/Dzarlax-AI/evening-news/go-editorial v0.0.0
	github.com/andybalholm/brotli v1.2.0
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/net v0.62.0
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0
	golang.org/x/text v0.43.0
)

replace github.com/Dzarlax-AI/evening-news/go-editorial => ../go-editorial

replace github.com/Dzarlax-AI/evening-news/go => ../go
