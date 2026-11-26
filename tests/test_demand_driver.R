# ============================================================
# Demand-driver prompt test
#
# Runs ONLY the analyst research step of consumer_demand_pipeline.R
# on a few hand-picked topics (no Google Trends discovery, no editor,
# no publishing) and prints each report to the log.
#
# Cases:
#   ps5_pro      - the miss: PS5 Pro sell-outs; does it reach the
#                  publisher of the launch driving demand?
#   bearista_cup - control: viral merch; should stay immaterial
#                  without inventing a driver chain.
#   acotar       - control: the book is the driver; should not add
#                  padding companies.
#
# Run from GitHub Actions: "Test demand-driver prompt" (manual).
# ============================================================

suppressPackageStartupMessages({
  library(dplyr); library(purrr); library(tibble); library(stringr)
  library(jsonlite); library(lubridate); library(readr)
})

stopifnot(nzchar(Sys.getenv("OPENAI_API_KEY")))

# Load function definitions only (the pipeline itself is not executed).
exprs <- parse("consumer_demand_pipeline.R", keep.source = FALSE)
for (e in exprs) {
  if (is.call(e) && identical(e[[1]], as.name("<-")) &&
      is.call(e[[3]]) && identical(e[[3]][[1]], as.name("function"))) {
    eval(e, envir = globalenv())
  }
}

HIGH_GROWTH_THRESHOLD <- 200
OPENAI_API_KEY <- Sys.getenv("OPENAI_API_KEY")
OPENAI_RESEARCH_MODEL <- "gpt-5"

q <- function(query, value, breakout = FALSE) {
  sprintf('- Query: "%s" | Bucket: rising | Google Trends value: %s | Breakout: %s | High growth: Yes | Candidate source: seed expansion',
          query, value, if (breakout) "Yes" else "No")
}

cases <- list(
  list(
    id = "ps5_pro", title = "ps5 pro",
    seeds = "sold out everywhere; out of stock; back in stock",
    evidence = c(q("ps5 pro sold out", "Breakout", TRUE),
                 q("ps5 pro sold out everywhere", "+450%"),
                 q("ps5 pro restock", "+300%"),
                 q("ps5 pro back in stock", "+250%"),
                 q("where to buy ps5 pro", "+200%"))
  ),
  list(
    id = "bearista_cup", title = "starbucks bearista cup",
    seeds = "sold out everywhere; waitlist",
    evidence = c(q("starbucks bearista cup sold out", "Breakout", TRUE),
                 q("bearista cup restock", "+350%"),
                 q("starbucks bear cup", "+250%"))
  ),
  list(
    id = "acotar", title = "acotar",
    seeds = "sold out everywhere; out of stock",
    evidence = c(q("acotar sold out", "Breakout", TRUE),
                 q("acotar book 6 sold out", "+400%"),
                 q("acotar special edition restock", "+250%"))
  )
)

only <- trimws(strsplit(Sys.getenv("TEST_CASES", ""), ",")[[1]])
if (length(only) > 0 && nzchar(only[1])) cases <- keep(cases, ~ .x$id %in% only)

dir.create("demand_driver_test", showWarnings = FALSE)

# Start all jobs, then wait for each (they run in parallel at OpenAI).
jobs <- map(cases, function(cs) {
  prompt <- build_parent_research_prompt(
    parent_title = cs$title,
    n_queries = length(cs$evidence),
    breakout_count = sum(grepl("Breakout: Yes", cs$evidence)),
    high_growth_count = length(cs$evidence),
    max_growth_value = 450,
    origin_seed_count = length(strsplit(cs$seeds, ";")[[1]]),
    origin_seeds = cs$seeds,
    evidence_text = paste(cs$evidence, collapse = "\n")
  )
  job <- start_web_research(prompt)
  message("Started ", cs$id, ": ", job$id)
  list(case = cs, id = job$id)
})

for (j in jobs) {
  cs <- j$case
  res <- tryCatch(wait_for_openai_response(j$id), error = function(e) {
    message("FAILED ", cs$id, ": ", conditionMessage(e)); NULL
  })
  if (is.null(res)) next
  txt <- extract_response_text(res)
  writeLines(txt, file.path("demand_driver_test", paste0(cs$id, ".md")), useBytes = TRUE)
  cat("\n\n##################### REPORT:", cs$id, "#####################\n")
  cat(txt, "\n")
  cat("##################### END:", cs$id, "#####################\n")
}
