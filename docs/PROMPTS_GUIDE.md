# AI prompt ownership

Article analysis and Russian editorial digest generation use the native Go runtime. The article analysis prompt lives in `go-domain/analysis`, and the editorial prompt, evidence selection and rendering live in `go-editorial/editorial`. The digest model receives prepared article summaries as evidence; it does not receive original article bodies or source URLs. See [Go digest delivery](go-digest-delivery.md) for the workflow and delivery contract.

The private Python domain still uses `AIClient._call_summary_llm` for the category-analysis backend operation and the native runtime's text transport. Python retains extraction-related prompt helpers in `news_aggregator/services/prompts.py`. The former Python category-summary generator, digest builder and digest generation method have been removed; historical `DailySummary` records and readers remain available.

The editorial CLI remains a diagnostic tool. Normal digest jobs use the long-lived native child and do not start the CLI for each edition.
