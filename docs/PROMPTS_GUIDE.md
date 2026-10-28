# AI prompt ownership

Article analysis and Russian editorial digest generation use the native Go runtime. The article analysis prompt lives in `go-domain/analysis`, and the editorial prompt, evidence selection and rendering live in `go-editorial/editorial`. The digest model receives prepared article summaries as evidence; it does not receive original article bodies or source URLs. See [Go digest delivery](go-digest-delivery.md) for the workflow and delivery contract.

The private Go domain owns category-analysis prompts in `go-domain/webbackend` and Page Monitor selector-analysis prompts in `go-domain/selectors`. Historical `DailySummary` records and readers remain available.

Normal digest jobs invoke the editorial library in the long-lived domain process; they do not start a child process for each edition.
