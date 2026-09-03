#Google Trends scarcity-signal pipeline using Windows curl.exe
# ------------------------------------------------------------
# Purpose:
# 1) Search Google Trends for a seed phrase such as "sold out everywhere"
# 2) Collect interest over time, related queries, and related topics
# 3) Repeat the seed search several times
# 4) Deduplicate and rank recurring queries/topics
# 5) Expand discovered seeds one level deeper
# 6) Build a candidate table for later research/trading/article workflows
# 7) Export all outputs to CSV
#
# Important:
# - This uses unofficial Google Trends web endpoints.
# - Google may change them or temporarily rate-limit frequent requests.
# - All web requests go through Windows curl.exe.
# - Start with a small number of runs and expansions.

# ============================================================
# 0. Packages
# ============================================================

library(httr2)
library(dplyr)
library(tidyr)
library(purrr)
library(tibble)
library(lubridate)
library(jsonlite)
library(stringr)
library(readr)

# ============================================================
# 1. Configuration
# ============================================================

SEED_QUERIES <- c(
  "sold out everywhere")

# Backward-compatible display value used in manifests.
SEED_Q <- paste(SEED_QUERIES, collapse = " | ")

GEO <- "US"
TIME_WINDOW <- "now 7-d"
CATEGORY <- 0
PROPERTY <- ""
LANGUAGE <- "en-US"

# Google Trends uses minutes west of UTC.
# Chile in July is UTC-4, so 240 is appropriate.
TZ_OFFSET <- 240

LOCAL_TZ <- "America/Santiago"

# Start conservatively to reduce the chance of HTTP 429 errors.
N_RUNS <- 1
SLEEP_BETWEEN_RUNS <- 45

MAX_SEEDS_TO_EXPAND <- 5

N_EXPANSION_RUNS <- 1
SLEEP_BETWEEN_EXPANSION_RUNS <- 35

EXPANSION_SLEEP_MIN <- 40
EXPANSION_SLEEP_MAX <- 70

# ============================================================
# Project paths
# ============================================================

# On GitHub Actions, GITHUB_WORKSPACE points to the checked-out
# repository. Locally, we use the current working directory.
GITHUB_WORKSPACE <- Sys.getenv("GITHUB_WORKSPACE")

if (nzchar(GITHUB_WORKSPACE)) {
  PROJECT_ROOT <- normalizePath(
    GITHUB_WORKSPACE,
    winslash = "/",
    mustWork = TRUE
  )
} else {
  PROJECT_ROOT <- normalizePath(
    getwd(),
    winslash = "/",
    mustWork = TRUE
  )
}

# Main output directory for pipeline artifacts.
OUTPUT_DIR <- PROJECT_ROOT

# Quarto research directory used by the public website.
RESEARCH_DIR <- file.path(
  PROJECT_ROOT,
  "research"
)

# Make sure the research directory exists.
dir.create(
  RESEARCH_DIR,
  recursive = TRUE,
  showWarnings = FALSE
)

# Safety check: this script should run from the Quarto project repository.
QUARTO_CONFIG <- file.path(
  PROJECT_ROOT,
  "_quarto.yml"
)

if (!file.exists(QUARTO_CONFIG)) {
  stop(
    paste0(
      "Could not find _quarto.yml in PROJECT_ROOT:\n",
      PROJECT_ROOT,
      "\n\n",
      "Run the pipeline from the consumer-demand-intelligence repository."
    )
  )
}

message("Project root: ", PROJECT_ROOT)
message("Research directory: ", RESEARCH_DIR)


# ============================================================
# Weekly publication configuration
# ============================================================

ISSUE_DATE <- Sys.Date()
ISO_YEAR <- lubridate::isoyear(ISSUE_DATE)
ISO_WEEK <- lubridate::isoweek(ISSUE_DATE)
ISSUE_ID <- sprintf("%d-W%02d", ISO_YEAR, ISO_WEEK)
ISSUE_TITLE <- paste("Weekly Market Intelligence", ISSUE_ID)
MAX_WEEKLY_PARENT_REPORTS <- 7
MAX_AUTOMATIC_REPORTS <- 5
MAX_EXPLORATORY_REPORTS <- 2
MIN_DISTINCT_QUERIES_FOR_EXPLORATORY <- 3
HIGH_GROWTH_THRESHOLD <- 200
MIN_ENTITY_CONFIDENCE_FOR_RESEARCH <- "medium"
MIN_COHERENCE_SCORE_FOR_RESEARCH <- 50
MIN_REPORT_CHARACTERS <- 1000
MIN_REPORT_SOURCES <- 3
MAX_WEEKLY_BRIEFING_STORIES <- 5
MAX_REPORT_CHARACTERS_FOR_EDITOR <- 30000
WEEKLY_OUTPUT_DIR <- file.path(OUTPUT_DIR, paste0("weekly_issue_", ISSUE_ID))
dir.create(WEEKLY_OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

# ============================================================
# 2. General helpers
# ============================================================

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0) y else x
}

empty_timeseries_tbl <- function() {
  tibble(
    ts_utc = as.POSIXct(character(), tz = "UTC"),
    ts_chile = as.POSIXct(character(), tz = LOCAL_TZ),
    date_text = character(),
    query = character(),
    value = double(),
    value_raw = character()
  )
}

empty_queries_tbl <- function() {
  tibble(
    bucket = character(),
    position = integer(),
    query = character(),
    value_raw = character(),
    extracted_value = double(),
    is_breakout = logical(),
    seed_keyword = character(),
    link = character()
  )
}

empty_topics_tbl <- function() {
  tibble(
    bucket = character(),
    position = integer(),
    id = character(),
    title = character(),
    type = character(),
    value_raw = character(),
    extracted_value = double(),
    is_breakout = logical(),
    seed_keyword = character(),
    link = character()
  )
}

parse_related_value <- function(x) {
  x <- as.character(x)
  
  case_when(
    is.na(x) ~ NA_real_,
    str_to_lower(x) == "breakout" ~ 5000,
    TRUE ~ suppressWarnings(
      as.numeric(
        str_remove_all(x, "[,%+]")
      )
    )
  )
}


as_numeric_scalar <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(NA_real_)
  }
  
  x <- unlist(x, recursive = TRUE, use.names = FALSE)
  
  if (length(x) == 0) {
    return(NA_real_)
  }
  
  suppressWarnings(as.numeric(x[[1]]))
}

# ============================================================
# 3. curl.exe request helper
# ============================================================

curl_exe_get <- function(
    url,
    cookie_file,
    retries = 3,
    sleep_s = 3,
    timeout_s = 90
) {
  
  output_file <- tempfile(fileext = ".txt")
  header_file <- tempfile(fileext = ".txt")
  
  on.exit(
    unlink(c(output_file, header_file), force = TRUE),
    add = TRUE
  )
  
  last_status <- NA_integer_
  
  for (attempt in seq_len(retries)) {
    
    args <- c(
      "--silent",
      "--show-error",
      "--location",
      "--compressed",
      
      "--user-agent",
      shQuote(
        paste(
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
          "AppleWebKit/537.36 (KHTML, like Gecko)",
          "Chrome/145.0.0.0 Safari/537.36"
        )
      ),
      
      "--header",
      shQuote("Accept: application/json,text/plain,*/*"),
      
      "--header",
      shQuote("Accept-Language: en-US,en;q=0.9"),
      
      "--header",
      shQuote("Referer: https://trends.google.com/"),
      
      "--cookie-jar",
      shQuote(cookie_file),
      
      "--cookie",
      shQuote(cookie_file),
      
      "--connect-timeout",
      "20",
      
      "--max-time",
      as.character(timeout_s),
      
      "--retry",
      "2",
      
      "--retry-delay",
      "2",
      
      "--dump-header",
      shQuote(header_file),
      
      "--output",
      shQuote(output_file),
      
      "--write-out",
      "%{http_code}",
      
      shQuote(url)
    )
    
    curl_result <- tryCatch(
      system2(
        "curl.exe",
        args = args,
        stdout = TRUE,
        stderr = TRUE
      ),
      error = function(e) e
    )
    
    if (!inherits(curl_result, "error")) {
      status_text <- tail(curl_result, 1)
      last_status <- suppressWarnings(as.integer(status_text))
    }
    
    response_exists <- file.exists(output_file) &&
      !is.na(file.info(output_file)$size) &&
      file.info(output_file)$size > 0
    
    if (
      response_exists &&
      !is.na(last_status) &&
      last_status >= 200 &&
      last_status < 300
    ) {
      
      response_text <- paste(
        readLines(
          output_file,
          warn = FALSE,
          encoding = "UTF-8"
        ),
        collapse = "\n"
      )
      
      return(response_text)
    }
    
    if (attempt < retries) {
      Sys.sleep(sleep_s * attempt)
    }
  }
  
  response_preview <- ""
  
  if (file.exists(output_file)) {
    response_preview <- paste(
      readLines(
        output_file,
        warn = FALSE,
        encoding = "UTF-8",
        n = 20
      ),
      collapse = "\n"
    )
  }
  
  stop(
    "Google Trends request failed.\n",
    "HTTP status: ", last_status, "\n",
    "Response preview:\n",
    substr(response_preview, 1, 1000)
  )
}

# ============================================================
# 4. Google Trends URL and JSON helpers
# ============================================================

parse_google_trends_json <- function(text) {
  
  clean_text <- sub(
    "^\\)\\]\\}',?\\s*",
    "",
    text
  )
  
  tryCatch(
    jsonlite::fromJSON(
      clean_text,
      simplifyVector = FALSE
    ),
    error = function(e) {
      stop(
        "Google returned content that could not be parsed as JSON.\n",
        "Original error: ", conditionMessage(e), "\n\n",
        "First 1000 characters:\n",
        substr(text, 1, 1000)
      )
    }
  )
}

build_url <- function(base_url, query) {
  request(base_url) |>
    req_url_query(!!!query) |>
    (\(x) x$url)()
}

start_google_trends_session <- function(cookie_file) {
  
  homepage_url <- "https://trends.google.com/trends/"
  
  invisible(
    curl_exe_get(
      url = homepage_url,
      cookie_file = cookie_file,
      retries = 3
    )
  )
  
  TRUE
}

google_trends_explore <- function(
    keyword,
    geo = GEO,
    time = TIME_WINDOW,
    category = CATEGORY,
    property = PROPERTY,
    hl = LANGUAGE,
    tz = TZ_OFFSET,
    cookie_file
) {
  
  explore_request <- list(
    comparisonItem = list(
      list(
        keyword = keyword,
        geo = geo,
        time = time
      )
    ),
    category = category,
    property = property
  )
  
  explore_json <- jsonlite::toJSON(
    explore_request,
    auto_unbox = TRUE,
    null = "null"
  )
  
  explore_url <- build_url(
    "https://trends.google.com/trends/api/explore",
    list(
      hl = hl,
      tz = tz,
      req = explore_json
    )
  )
  
  response_text <- curl_exe_get(
    url = explore_url,
    cookie_file = cookie_file
  )
  
  parse_google_trends_json(response_text)
}

find_widget <- function(widgets, widget_id) {
  
  matches <- keep(
    widgets,
    function(widget) {
      identical(
        toupper(widget$id %||% ""),
        toupper(widget_id)
      )
    }
  )
  
  if (length(matches) == 0) {
    return(NULL)
  }
  
  matches[[1]]
}

google_trends_widget <- function(
    widget,
    endpoint,
    cookie_file,
    hl = LANGUAGE,
    tz = TZ_OFFSET
) {
  
  if (is.null(widget$token)) {
    stop("The selected widget does not contain a token.")
  }
  
  if (is.null(widget$request)) {
    stop("The selected widget does not contain a request object.")
  }
  
  widget_request_json <- jsonlite::toJSON(
    widget$request,
    auto_unbox = TRUE,
    null = "null"
  )
  
  widget_url <- build_url(
    paste0(
      "https://trends.google.com/trends/api/widgetdata/",
      endpoint
    ),
    list(
      hl = hl,
      tz = tz,
      req = widget_request_json,
      token = widget$token
    )
  )
  
  response_text <- curl_exe_get(
    url = widget_url,
    cookie_file = cookie_file
  )
  
  parse_google_trends_json(response_text)
}

# ============================================================
# 5. Parsers
# ============================================================

extract_ranked_queries <- function(response, search_term) {
  
  if (
    is.null(response) ||
    is.null(response$default) ||
    is.null(response$default$rankedList) ||
    length(response$default$rankedList) == 0
  ) {
    return(empty_queries_tbl())
  }
  
  ranked_lists <- response$default$rankedList
  
  result <- map_dfr(
    seq_along(ranked_lists),
    function(list_number) {
      
      ranked_list <- ranked_lists[[list_number]]
      rows <- ranked_list$rankedKeyword %||% list()
      
      if (length(rows) == 0) {
        return(empty_queries_tbl())
      }
      
      bucket <- case_when(
        list_number == 1 ~ "top",
        list_number == 2 ~ "rising",
        TRUE ~ paste0("group_", list_number)
      )
      
      map_dfr(
        seq_along(rows),
        function(position_number) {
          
          row <- rows[[position_number]]
          
          value_raw <- row$formattedValue %||%
            row$value %||%
            NA_character_
          
          tibble(
            bucket = bucket,
            position = as.integer(position_number),
            query = as.character(
              row$query %||%
                row$topic$title %||%
                NA_character_
            ),
            value_raw = as.character(value_raw),
            extracted_value = parse_related_value(value_raw),
            is_breakout = str_detect(
              str_to_lower(as.character(value_raw)),
              "breakout"
            ),
            seed_keyword = search_term,
            link = as.character(
              row$link %||% NA_character_
            )
          )
        }
      )
    }
  )
  
  if (nrow(result) == 0) {
    return(empty_queries_tbl())
  }
  
  result
}

extract_ranked_topics <- function(response, search_term) {
  
  if (
    is.null(response) ||
    is.null(response$default) ||
    is.null(response$default$rankedList) ||
    length(response$default$rankedList) == 0
  ) {
    return(empty_topics_tbl())
  }
  
  ranked_lists <- response$default$rankedList
  
  result <- map_dfr(
    seq_along(ranked_lists),
    function(list_number) {
      
      ranked_list <- ranked_lists[[list_number]]
      rows <- ranked_list$rankedKeyword %||% list()
      
      if (length(rows) == 0) {
        return(empty_topics_tbl())
      }
      
      bucket <- case_when(
        list_number == 1 ~ "top",
        list_number == 2 ~ "rising",
        TRUE ~ paste0("group_", list_number)
      )
      
      map_dfr(
        seq_along(rows),
        function(position_number) {
          
          row <- rows[[position_number]]
          
          value_raw <- row$formattedValue %||%
            row$value %||%
            NA_character_
          
          tibble(
            bucket = bucket,
            position = as.integer(position_number),
            id = as.character(
              row$topic$mid %||% NA_character_
            ),
            title = as.character(
              row$topic$title %||%
                row$query %||%
                NA_character_
            ),
            type = as.character(
              row$topic$type %||%
                NA_character_
            ),
            value_raw = as.character(value_raw),
            extracted_value = parse_related_value(value_raw),
            is_breakout = str_detect(
              str_to_lower(as.character(value_raw)),
              "breakout"
            ),
            seed_keyword = search_term,
            link = as.character(
              row$link %||% NA_character_
            )
          )
        }
      )
    }
  )
  
  if (nrow(result) == 0) {
    return(empty_topics_tbl())
  }
  
  result
}

# ============================================================
# 6. Reusable one-search function
# ============================================================

get_google_trend <- function(
    search_term,
    cookie_file,
    geo = GEO,
    time = TIME_WINDOW,
    category = CATEGORY,
    property = PROPERTY,
    hl = LANGUAGE,
    tz = TZ_OFFSET,
    collect_timeseries = TRUE
) {
  
  message("Searching Google Trends for: ", search_term)
  
  explore_response <- google_trends_explore(
    keyword = search_term,
    geo = geo,
    time = time,
    category = category,
    property = property,
    hl = hl,
    tz = tz,
    cookie_file = cookie_file
  )
  
  widgets <- explore_response$widgets %||% list()
  
  timeseries_widget <- find_widget(
    widgets,
    "TIMESERIES"
  )
  
  queries_widget <- find_widget(
    widgets,
    "RELATED_QUERIES"
  )
  
  topics_widget <- find_widget(
    widgets,
    "RELATED_TOPICS"
  )
  
  # Time series
  if (isTRUE(collect_timeseries) && !is.null(timeseries_widget)) {
    
    timeseries_response <- google_trends_widget(
      widget = timeseries_widget,
      endpoint = "multiline",
      cookie_file = cookie_file,
      hl = hl,
      tz = tz
    )
    
    timeline_data <- timeseries_response$default$timelineData %||% list()
    
    ts_result <- map_dfr(
      timeline_data,
      function(row) {
        
        values <- row$value %||% list(NA_real_)
        formatted_values <- row$formattedValue %||%
          list(NA_character_)
        
        tibble(
          timestamp = as_numeric_scalar(
            row$time %||% NA_real_
          ),
          date_text = row$formattedTime %||%
            row$formattedAxisTime %||%
            NA_character_,
          value = suppressWarnings(
            as.numeric(values[[1]] %||% NA_real_)
          ),
          value_raw = as.character(
            formatted_values[[1]] %||% NA_character_
          )
        )
      }
    ) |>
      mutate(
        ts_utc = as.POSIXct(
          timestamp,
          origin = "1970-01-01",
          tz = "UTC"
        ),
        ts_chile = with_tz(ts_utc, LOCAL_TZ),
        query = search_term
      ) |>
      select(
        ts_utc,
        ts_chile,
        date_text,
        query,
        value,
        value_raw
      )
    
  } else {
    ts_result <- empty_timeseries_tbl()
  }
  
  # Related queries
  if (!is.null(queries_widget)) {
    
    queries_response <- google_trends_widget(
      widget = queries_widget,
      endpoint = "relatedsearches",
      cookie_file = cookie_file,
      hl = hl,
      tz = tz
    )
    
    rq_result <- extract_ranked_queries(
      response = queries_response,
      search_term = search_term
    )
    
  } else {
    rq_result <- empty_queries_tbl()
  }
  
  # Related topics
  if (!is.null(topics_widget)) {
    
    topics_response <- google_trends_widget(
      widget = topics_widget,
      endpoint = "relatedsearches",
      cookie_file = cookie_file,
      hl = hl,
      tz = tz
    )
    
    rt_result <- extract_ranked_topics(
      response = topics_response,
      search_term = search_term
    )
    
  } else {
    rt_result <- empty_topics_tbl()
  }
  
  list(
    timeseries = ts_result,
    related_queries = rq_result,
    related_topics = rt_result
  )
}

# ============================================================
# 7. Start session and test connection
# ============================================================

cookie_file <- file.path(
  tempdir(),
  "google_trends_cookies.txt"
)

start_google_trends_session(cookie_file)

message("Google Trends session started.")

# Optional test:
# test_result <- get_google_trend(
#   search_term = SEED_Q,
#   cookie_file = cookie_file
# )
# glimpse(test_result$timeseries)
# glimpse(test_result$related_queries)
# glimpse(test_result$related_topics)

# ============================================================
# 8. Repeat all discovery-seed searches
# ============================================================

seed_results <- list()
seed_run_index <- 0L

for (seed_q in SEED_QUERIES) {
  for (i in seq_len(N_RUNS)) {
    seed_run_index <- seed_run_index + 1L
    message("Seed: ", seed_q, " | run ", i, " of ", N_RUNS)
    fetched_at <- Sys.time()
    
    result_i <- tryCatch(
      get_google_trend(search_term = seed_q, cookie_file = cookie_file, collect_timeseries = FALSE),
      error = function(e) {
        message("Seed run failed for ", seed_q, ": ", conditionMessage(e))
        NULL
      }
    )
    
    if (!is.null(result_i)) {
      result_i$timeseries <- result_i$timeseries |>
        mutate(run = i, fetched_at = fetched_at, origin_seed = seed_q)
      result_i$related_queries <- result_i$related_queries |>
        mutate(run = i, fetched_at = fetched_at, origin_seed = seed_q)
      result_i$related_topics <- result_i$related_topics |>
        mutate(run = i, fetched_at = fetched_at, origin_seed = seed_q)
    }
    
    seed_results[[seed_run_index]] <- result_i
    if (i < N_RUNS) Sys.sleep(SLEEP_BETWEEN_RUNS)
  }
}

# ============================================================
# 9. Combine seed runs
# ============================================================

harvest_timeseries <- map_dfr(
  seed_results,
  ~ if (is.null(.x)) {
    empty_timeseries_tbl()
  } else {
    .x$timeseries
  }
)

harvest_related_queries <- map_dfr(
  seed_results,
  ~ if (is.null(.x)) {
    empty_queries_tbl()
  } else {
    .x$related_queries
  }
)

harvest_related_topics <- map_dfr(
  seed_results,
  ~ if (is.null(.x)) {
    empty_topics_tbl()
  } else {
    .x$related_topics
  }
)

message("Seed harvest complete.")
message("Time-series rows: ", nrow(harvest_timeseries))
message("Related-query rows: ", nrow(harvest_related_queries))
message("Related-topic rows: ", nrow(harvest_related_topics))

# ------------------------------------------------------------
# Stop safely if Google Trends returned no discovery data
# ------------------------------------------------------------

if (
  nrow(harvest_related_queries) == 0 &&
  nrow(harvest_related_topics) == 0
) {
  
  stop(
    paste0(
      "\nGoogle Trends returned no usable discovery data.\n",
      "The most likely cause is temporary HTTP 429 rate limiting.\n",
      "The pipeline is stopping before candidate construction ",
      "to avoid producing misleading empty results.\n"
    )
  )
}

# ============================================================
# 10. Build expansion seeds with cross-seed confirmation
# ============================================================


topic_seeds <- harvest_related_topics |>
  filter(!is.na(title), title != "") |>
  group_by(id, title, type) |>
  summarise(
    frequency = n(),
    origin_seed_count = n_distinct(origin_seed),
    origin_seeds = paste(sort(unique(origin_seed)), collapse = " | "),
    .groups = "drop"
  ) |>
  mutate(
    seed_kind = "topic",
    search_value = if_else(!is.na(id) & id != "", id, title)
  )

query_seeds <- harvest_related_queries |>
  filter(!is.na(query), query != "") |>
  group_by(query) |>
  summarise(
    frequency = n(),
    origin_seed_count = n_distinct(origin_seed),
    origin_seeds = paste(sort(unique(origin_seed)), collapse = " | "),
    .groups = "drop"
  ) |>
  transmute(
    id = NA_character_, title = query, type = "search_query",
    frequency, origin_seed_count, origin_seeds,
    seed_kind = "query", search_value = query
  )

normalize_query_text <- function(x) {
  x |>
    stringr::str_to_lower() |>
    stringr::str_replace_all("[^a-z0-9\\s]", " ") |>
    stringr::str_replace_all("\\s+", " ") |>
    stringr::str_trim()
}

expanded_seeds <- bind_rows(topic_seeds, query_seeds) |>
  distinct(search_value, .keep_all = TRUE) |>
  mutate(
    seed_text = normalize_query_text(title),
    scarcity_relevant = stringr::str_detect(
      seed_text,
      paste(
        "sold out",
        "out of stock",
        "shortage",
        "restock",
        "recall",
        "contamination",
        "shutdown",
        "closing",
        "supply",
        "production",
        "delivery delay",
        "price increase",
        "inventory",
        "where to buy",
        "export restriction",
        sep = "|"
      )
    ),
    expansion_score =
      40 * pmin(origin_seed_count / 2, 1) +
      30 * pmin(frequency / 5, 1) +
      30 * if_else(scarcity_relevant, 1, 0)
  ) |>
  arrange(desc(expansion_score), desc(origin_seed_count), desc(frequency))

seeds_to_expand <- expanded_seeds |>
  filter(expansion_score >= 30) |>
  slice_head(n = MAX_SEEDS_TO_EXPAND)

message("Unique seeds found: ", nrow(expanded_seeds))
message("Seeds selected for expansion: ", nrow(seeds_to_expand))

# ============================================================
# 11. Expand each seed one level deeper
# ============================================================

expanded_results <- vector("list", nrow(seeds_to_expand))

if (nrow(seeds_to_expand) > 0) {
  for (i in seq_len(nrow(seeds_to_expand))) {
    parent_title <- seeds_to_expand$title[i]
    parent_id <- seeds_to_expand$id[i]
    parent_type <- seeds_to_expand$type[i]
    parent_kind <- seeds_to_expand$seed_kind[i]
    search_value <- seeds_to_expand$search_value[i]
    parent_origin_seed_count <- seeds_to_expand$origin_seed_count[i]
    parent_origin_seeds <- seeds_to_expand$origin_seeds[i]
    
    message("Expanding seed ", i, " of ", nrow(seeds_to_expand), ": ", parent_title)
    seed_expansion_runs <- vector("list", N_EXPANSION_RUNS)
    
    for (j in seq_len(N_EXPANSION_RUNS)) {
      fetched_at <- Sys.time()
      result_j <- tryCatch(
        get_google_trend(
          search_term = search_value,
          cookie_file = cookie_file,
          collect_timeseries = FALSE
        ),
        error = function(e) {
          message("Expansion failed for ", parent_title, ": ", conditionMessage(e))
          NULL
        }
      )
      
      if (!is.null(result_j)) {
        common_fields <- list(
          parent_id = parent_id,
          parent_title = parent_title,
          parent_type = parent_type,
          parent_kind = parent_kind,
          parent_search_value = search_value,
          parent_origin_seed_count = parent_origin_seed_count,
          parent_origin_seeds = parent_origin_seeds,
          expansion_run = j,
          expansion_fetched_at = fetched_at
        )
        result_j$related_queries <- result_j$related_queries |> mutate(!!!common_fields)
        result_j$related_topics <- result_j$related_topics |> mutate(!!!common_fields)
      }
      
      seed_expansion_runs[[j]] <- result_j
      if (j < N_EXPANSION_RUNS) Sys.sleep(SLEEP_BETWEEN_EXPANSION_RUNS)
    }
    
    expanded_results[[i]] <- seed_expansion_runs
    if (i < nrow(seeds_to_expand)) {
      Sys.sleep(sample(EXPANSION_SLEEP_MIN:EXPANSION_SLEEP_MAX, 1))
    }
  }
}

expanded_rq <- map_dfr(expanded_results, function(seed_runs) {
  if (is.null(seed_runs)) return(tibble())
  map_dfr(seed_runs, ~ if (is.null(.x)) tibble() else .x$related_queries)
})

expanded_rt <- map_dfr(expanded_results, function(seed_runs) {
  if (is.null(seed_runs)) return(tibble())
  map_dfr(seed_runs, ~ if (is.null(.x)) tibble() else .x$related_topics)
})

message("Expansion complete.")
message("Expanded related-query rows: ", nrow(expanded_rq))
message("Expanded related-topic rows: ", nrow(expanded_rt))


# ============================================================
# Preserve initial Google Trends discoveries
# ============================================================

initial_query_candidates <- harvest_related_queries |>
  filter(
    !is.na(query),
    query != ""
  ) |>
  mutate(
    parent_title = query,
    parent_id = NA_character_,
    parent_type = "search_query",
    parent_kind = "initial_query",
    parent_search_value = query,
    parent_origin_seed_count = 1,
    parent_origin_seeds = origin_seed,
    expansion_run = NA_integer_,
    expansion_fetched_at = fetched_at,
    candidate_source = "initial_discovery"
  ) |>
  distinct(
    parent_title,
    query,
    .keep_all = TRUE
  )


# ============================================================
# 12. Query normalization, candidate scoring, and entity checks
# ============================================================


# ------------------------------------------------------------
# 12A. Query normalization helper
# ------------------------------------------------------------

normalize_query_text <- function(x) {
  x |>
    stringr::str_to_lower() |>
    stringr::str_replace_all(
      "[^a-z0-9\\s]",
      " "
    ) |>
    stringr::str_replace_all(
      "\\s+",
      " "
    ) |>
    stringr::str_trim()
}


# ------------------------------------------------------------
# 12B. Entity-confidence helper
# ------------------------------------------------------------

infer_entity_confidence <- function(
    parent_title,
    supporting_queries
) {
  
  parent_norm <- normalize_query_text(
    parent_title
  )
  
  queries_norm <- normalize_query_text(
    supporting_queries
  )
  
  # Remove missing / empty query values
  queries_norm <- queries_norm[
    !is.na(queries_norm) &
      nzchar(queries_norm)
  ]
  
  if (
    is.na(parent_norm) ||
    !nzchar(parent_norm) ||
    length(queries_norm) == 0
  ) {
    return("low")
  }
  
  
  # ----------------------------------------------------------
  # Special protection for Best Buy
  # ----------------------------------------------------------
  
  if (identical(parent_norm, "best buy")) {
    
    explicit_company <- any(
      stringr::str_detect(
        queries_norm,
        paste0(
          "\\bbest buy\\b.*",
          "(store|stores|retailer|electronics|geek squad)|",
          "(store|stores|retailer|electronics|geek squad).*",
          "\\bbest buy\\b"
        )
      ),
      na.rm = TRUE
    )
    
    return(
      if (explicit_company) {
        "high"
      } else {
        "low"
      }
    )
  }
  
  
  # ----------------------------------------------------------
  # General parent-topic matching
  # ----------------------------------------------------------
  
  parent_matches <- stringr::str_detect(
    queries_norm,
    stringr::fixed(
      parent_norm
    )
  )
  
  explicit_share <- mean(
    parent_matches,
    na.rm = TRUE
  )
  
  if (
    is.nan(explicit_share) ||
    !is.finite(explicit_share)
  ) {
    explicit_share <- 0
  }
  
  dplyr::case_when(
    explicit_share >= 0.50 ~ "high",
    explicit_share >= 0.20 ~ "medium",
    TRUE ~ "medium"
  )
}


# ------------------------------------------------------------
# 12C. Safe maximum helper
# ------------------------------------------------------------

safe_max <- function(x) {
  
  x <- suppressWarnings(
    as.numeric(x)
  )
  
  x <- x[
    is.finite(x)
  ]
  
  if (length(x) == 0) {
    return(NA_real_)
  }
  
  max(
    x,
    na.rm = TRUE
  )
}


# ============================================================
# 12D. Preserve INITIAL Google Trends discoveries
#
# IMPORTANT:
# A valid query discovered from "sold out everywhere"
# must survive even if expanding that query returns nothing.
# ============================================================

initial_query_candidates <- harvest_related_queries |>
  filter(
    !is.na(query),
    query != ""
  ) |>
  mutate(
    parent_title = query,
    
    parent_id = NA_character_,
    
    parent_type = "search_query",
    
    parent_kind = "initial_query",
    
    parent_search_value = query,
    
    # We currently use only "sold out everywhere"
    # as the originating discovery seed.
    parent_origin_seed_count = 1,
    
    parent_origin_seeds = origin_seed,
    
    expansion_run = NA_integer_,
    
    expansion_fetched_at = fetched_at,
    
    candidate_source = "initial_discovery"
  ) |>
  distinct(
    parent_title,
    query,
    .keep_all = TRUE
  )


# ============================================================
# 12E. Preserve EXPANDED Google Trends discoveries
# ============================================================

if (nrow(expanded_rq) > 0) {
  
  expanded_query_candidates <- expanded_rq |>
    mutate(
      candidate_source = "expanded"
    )
  
} else {
  
  # Correctly structured empty table.
  # This lets bind_rows() work even if expansion produced
  # zero related queries.
  
  expanded_query_candidates <- tibble(
    bucket = character(),
    position = integer(),
    query = character(),
    value_raw = character(),
    extracted_value = double(),
    is_breakout = logical(),
    seed_keyword = character(),
    link = character(),
    
    parent_id = character(),
    parent_title = character(),
    parent_type = character(),
    parent_kind = character(),
    parent_search_value = character(),
    
    parent_origin_seed_count = double(),
    parent_origin_seeds = character(),
    
    expansion_run = integer(),
    expansion_fetched_at = as.POSIXct(
      character()
    ),
    
    candidate_source = character()
  )
}


# ============================================================
# 12F. Combine initial + expanded discoveries
# ============================================================

combined_candidate_queries <- bind_rows(
  expanded_query_candidates,
  initial_query_candidates
)


message(
  "Initial candidate-query rows: ",
  nrow(initial_query_candidates)
)

message(
  "Expanded candidate-query rows: ",
  nrow(expanded_query_candidates)
)

message(
  "Combined candidate-query rows: ",
  nrow(combined_candidate_queries)
)


# ============================================================
# 12G. Build query-level trend candidates
# ============================================================

if (nrow(combined_candidate_queries) > 0) {
  
  trend_candidates <- combined_candidate_queries |>
    
    filter(
      !is.na(query),
      query != ""
    ) |>
    
    mutate(
      normalized_query =
        normalize_query_text(
          query
        )
    ) |>
    
    # Remove duplicate observations of the same query
    # under the same parent.
    distinct(
      parent_title,
      normalized_query,
      .keep_all = TRUE
    ) |>
    
    mutate(
      
      # ------------------------------------------------------
      # High-growth signal
      # ------------------------------------------------------
      
      high_growth =
        !is_breakout &
        !is.na(extracted_value) &
        extracted_value >=
        HIGH_GROWTH_THRESHOLD,
      
      
      # ------------------------------------------------------
      # Google Trends signal score
      #
      # Breakout:
      # strongest possible signal.
      #
      # Rising:
      # numeric value represents relative growth.
      #
      # Top:
      # value is NOT a percentage-growth measure,
      # therefore it gets a modest fixed score rather than
      # being interpreted as +100%.
      # ------------------------------------------------------
      
      growth_score = case_when(
        
        is_breakout ~ 100,
        
        bucket == "top" ~ 35,
        
        is.na(extracted_value) ~ 0,
        
        TRUE ~ pmin(
          log1p(extracted_value) /
            log1p(5000) *
            100,
          100
        )
      ),
      
      
      # ------------------------------------------------------
      # Scarcity / consumer-demand relevance
      # ------------------------------------------------------
      
      scarcity_match =
        stringr::str_detect(
          normalized_query,
          paste(
            "sold out",
            "out of stock",
            "restock",
            "back in stock",
            "where to buy",
            "hard to find",
            "shortage",
            "supply disruption",
            "delivery delay",
            "price increase",
            "recall",
            sep = "|"
          )
        ),
      
      
      scarcity_score =
        dplyr::if_else(
          scarcity_match,
          100,
          35
        ),
      
      
      # ------------------------------------------------------
      # Candidate-source score
      #
      # Expanded signals receive more weight because they
      # survived an additional Google Trends investigation.
      #
      # Initial discoveries are still valid and MUST NOT be
      # discarded simply because expansion returned nothing.
      # ------------------------------------------------------
      
      source_score = case_when(
        
        candidate_source ==
          "expanded" ~ 100,
        
        candidate_source ==
          "initial_discovery" ~ 70,
        
        TRUE ~ 50
      ),
      
      
      # ------------------------------------------------------
      # Query-level discovery score
      # ------------------------------------------------------
      
      query_discovery_score =
        
        0.45 * growth_score +
        
        0.20 *
        dplyr::if_else(
          is_breakout,
          100,
          0
        ) +
        
        0.20 *
        scarcity_score +
        
        0.15 *
        source_score
    ) |>
    
    arrange(
      desc(
        query_discovery_score
      )
    )
  
} else {
  
  # Correctly structured empty candidate table
  
  trend_candidates <- tibble(
    parent_title = character(),
    query = character(),
    normalized_query = character(),
    bucket = character(),
    value_raw = character(),
    extracted_value = double(),
    is_breakout = logical(),
    high_growth = logical(),
    scarcity_match = logical(),
    candidate_source = character(),
    
    growth_score = double(),
    scarcity_score = double(),
    source_score = double(),
    
    query_discovery_score = double(),
    
    parent_origin_seed_count = double(),
    parent_origin_seeds = character()
  )
}


message(
  "Final trend-candidate rows: ",
  nrow(trend_candidates)
)


# ============================================================
# 12H. Build parent-level candidates
# ============================================================

parent_candidates <- trend_candidates |>
  
  group_by(
    parent_title
  ) |>
  
  summarise(
    
    # --------------------------------------------------------
    # Query evidence
    # --------------------------------------------------------
    
    n_queries =
      n_distinct(
        normalized_query
      ),
    
    breakout_count =
      sum(
        is_breakout,
        na.rm = TRUE
      ),
    
    high_growth_count =
      sum(
        high_growth,
        na.rm = TRUE
      ),
    
    scarcity_query_count =
      sum(
        scarcity_match,
        na.rm = TRUE
      ),
    
    initial_discovery_count =
      sum(
        candidate_source ==
          "initial_discovery",
        na.rm = TRUE
      ),
    
    expanded_discovery_count =
      sum(
        candidate_source ==
          "expanded",
        na.rm = TRUE
      ),
    
    
    # --------------------------------------------------------
    # Aggregate signal strength
    # --------------------------------------------------------
    
    breakout_share =
      mean(
        is_breakout,
        na.rm = TRUE
      ),
    
    avg_discovery_score =
      mean(
        query_discovery_score,
        na.rm = TRUE
      ),
    
    max_discovery_score =
      safe_max(
        query_discovery_score
      ),
    
    median_discovery_score =
      median(
        query_discovery_score,
        na.rm = TRUE
      ),
    
    max_growth_value =
      safe_max(
        extracted_value[
          !is_breakout &
            bucket != "top"
        ]
      ),
    
    
    # --------------------------------------------------------
    # Origin information
    # --------------------------------------------------------
    
    origin_seed_count =
      safe_max(
        parent_origin_seed_count
      ),
    
    origin_seeds =
      first(
        parent_origin_seeds
      ),
    
    
    # --------------------------------------------------------
    # Store supporting evidence
    # --------------------------------------------------------
    
    supporting_queries =
      list(
        query
      ),
    
    supporting_values =
      list(
        value_raw
      ),
    
    supporting_sources =
      list(
        candidate_source
      ),
    
    .groups = "drop"
  ) |>
  
  rowwise() |>
  
  mutate(
    
    # --------------------------------------------------------
    # Entity confidence
    # --------------------------------------------------------
    
    entity_confidence =
      infer_entity_confidence(
        parent_title,
        unlist(
          supporting_queries
        )
      ),
    
    
    # --------------------------------------------------------
    # Component scores
    # --------------------------------------------------------
    
    query_count_score =
      pmin(
        100,
        100 *
          log1p(n_queries) /
          log1p(20)
      ),
    
    breakout_score =
      pmin(
        100,
        breakout_count * 35
      ),
    
    high_growth_score =
      pmin(
        100,
        high_growth_count * 20
      ),
    
    scarcity_parent_score =
      pmin(
        100,
        scarcity_query_count * 40
      ),
    
    cross_seed_score =
      pmin(
        100,
        origin_seed_count / 3 * 100
      ),
    
    
    # --------------------------------------------------------
    # Parent discovery score
    # --------------------------------------------------------
    
    discovery_score =
      
      0.25 *
      query_count_score +
      
      0.15 *
      breakout_score +
      
      0.15 *
      high_growth_score +
      
      0.15 *
      scarcity_parent_score +
      
      0.10 *
      pmin(
        avg_discovery_score,
        100
      ) +
      
      0.10 *
      pmin(
        max_discovery_score,
        100
      ) +
      
      0.10 *
      cross_seed_score,
    
    
    # --------------------------------------------------------
    # Discovery-priority label
    # --------------------------------------------------------
    
    discovery_priority = case_when(
      
      discovery_score >= 75 ~
        "high",
      
      discovery_score >= 50 ~
        "medium",
      
      discovery_score >= 30 ~
        "exploratory",
      
      TRUE ~
        "low"
    ),
    
    
    # --------------------------------------------------------
    # Research queue decision
    #
    # IMPORTANT CHANGE:
    #
    # A direct scarcity query discovered from
    # "sold out everywhere" can now receive exploratory
    # research even if it has:
    #
    # - only one supporting query;
    # - no Breakout;
    # - no successful expansion.
    #
    # This is what preserves signals such as:
    #
    # "why are needohs sold out everywhere"
    # --------------------------------------------------------
    
    queue_status = case_when(
      
      entity_confidence == "low" ~
        "reject",
      
      discovery_score >= 50 ~
        "automatic",
      
      scarcity_query_count >= 1 &
        initial_discovery_count >= 1 ~
        "exploratory",
      
      n_queries >=
        MIN_DISTINCT_QUERIES_FOR_EXPLORATORY ~
        "exploratory",
      
      high_growth_count >= 2 ~
        "exploratory",
      
      breakout_count >= 1 ~
        "exploratory",
      
      origin_seed_count >= 2 ~
        "exploratory",
      
      TRUE ~
        "reject"
    ),
    
    
    # --------------------------------------------------------
    # Human-readable explanation
    # --------------------------------------------------------
    
    queue_reason = case_when(
      
      entity_confidence == "low" ~
        paste(
          "Rejected because the entity match",
          "is ambiguous or likely incorrect."
        ),
      
      queue_status == "automatic" ~
        paste(
          "Automatically selected by",
          "discovery strength."
        ),
      
      queue_status == "exploratory" &
        scarcity_query_count >= 1 &
        initial_discovery_count >= 1 ~
        paste(
          "Exploratory research because Google Trends",
          "directly surfaced a scarcity-related query from",
          "the 'sold out everywhere' discovery seed."
        ),
      
      queue_status == "exploratory" &
        n_queries >=
        MIN_DISTINCT_QUERIES_FOR_EXPLORATORY ~
        paste0(
          "Exploratory research because it has ",
          n_queries,
          " distinct supporting queries."
        ),
      
      queue_status == "exploratory" &
        high_growth_count >= 2 ~
        paste0(
          "Exploratory research because it has ",
          high_growth_count,
          " high-growth queries."
        ),
      
      queue_status == "exploratory" &
        breakout_count >= 1 ~
        paste(
          "Exploratory research because it",
          "includes a Breakout query."
        ),
      
      queue_status == "exploratory" &
        origin_seed_count >= 2 ~
        paste(
          "Exploratory research because it was",
          "confirmed across multiple discovery seeds."
        ),
      
      TRUE ~
        paste(
          "Rejected because the discovery",
          "evidence is too weak."
        )
    )
  ) |>
  
  ungroup() |>
  
  arrange(
    desc(
      discovery_score
    )
  )


# ============================================================
# 12I. Build parent evidence text
# ============================================================

parent_evidence <- trend_candidates |>
  
  arrange(
    parent_title,
    desc(is_breakout),
    desc(high_growth),
    desc(query_discovery_score)
  ) |>
  
  group_by(
    parent_title
  ) |>
  
  summarise(
    
    evidence_text = paste0(
      
      '- Query: "',
      query,
      
      '" | Bucket: ',
      bucket,
      
      " | Google Trends value: ",
      value_raw,
      
      " | Breakout: ",
      if_else(
        is_breakout,
        "Yes",
        "No"
      ),
      
      " | High growth: ",
      if_else(
        high_growth,
        "Yes",
        "No"
      ),
      
      " | Candidate source: ",
      candidate_source,
      
      " | Scarcity match: ",
      if_else(
        scarcity_match,
        "Yes",
        "No"
      ),
      
      " | Query discovery score: ",
      round(
        query_discovery_score,
        1
      ),
      
      collapse = "\n"
    ),
    
    .groups = "drop"
  )


# ============================================================
# 12J. Attach evidence to parent candidates
# ============================================================

parent_candidates <- parent_candidates |>
  
  left_join(
    parent_evidence,
    by = "parent_title"
  )


# ============================================================
# 12K. Build automatic + exploratory queues
# ============================================================

automatic_queue <- parent_candidates |>
  
  filter(
    queue_status ==
      "automatic"
  ) |>
  
  arrange(
    desc(
      discovery_score
    )
  ) |>
  
  slice_head(
    n = MAX_AUTOMATIC_REPORTS
  )


exploratory_queue <- parent_candidates |>
  
  filter(
    queue_status ==
      "exploratory"
  ) |>
  
  arrange(
    desc(
      discovery_score
    )
  ) |>
  
  slice_head(
    n = MAX_EXPLORATORY_REPORTS
  )


# ============================================================
# 12L. Pre-coherence research queue
# ============================================================

research_queue_pre_coherence <- bind_rows(
  automatic_queue,
  exploratory_queue
) |>
  
  arrange(
    
    factor(
      queue_status,
      levels = c(
        "automatic",
        "exploratory"
      )
    ),
    
    desc(
      discovery_score
    )
  )


# The actual coherence classifier runs later.
research_queue <- research_queue_pre_coherence


# ============================================================
# 12M. Save full parent-candidate audit
# ============================================================

readr::write_csv(
  
  parent_candidates |>
    select(
      -supporting_queries,
      -supporting_values,
      -supporting_sources
    ),
  
  file.path(
    WEEKLY_OUTPUT_DIR,
    "parent_candidate_audit.csv"
  )
)


# ============================================================
# 12N. Console diagnostics
# ============================================================

message(
  "Parent candidates: ",
  nrow(
    parent_candidates
  )
)

message(
  "Automatic candidates: ",
  nrow(
    automatic_queue
  )
)

message(
  "Exploratory candidates: ",
  nrow(
    exploratory_queue
  )
)

message(
  "Pre-coherence research queue: ",
  nrow(
    research_queue_pre_coherence
  )
)


print(
  parent_candidates |>
    select(
      parent_title,
      discovery_score,
      discovery_priority,
      queue_status,
      queue_reason,
      entity_confidence,
      n_queries,
      breakout_count,
      high_growth_count,
      scarcity_query_count,
      initial_discovery_count,
      expanded_discovery_count
    ),
  n = Inf
)
# ============================================================
# 13. Deep Research prompt
# ============================================================

# ============================================================
# Parent research prompt
# ============================================================

# ============================================================
# Parent research prompt
# ============================================================

build_parent_research_prompt <- function(
    parent_title,
    n_queries,
    breakout_count,
    high_growth_count,
    max_growth_value,
    origin_seed_count,
    origin_seeds,
    evidence_text
) {
  
  paste0(
    
    "You are conducting independent consumer-demand, market-intelligence, and investment research on a topic identified through Google Trends.\n\n",
    
    
    # --------------------------------------------------------
    # Interpretation
    # --------------------------------------------------------
    
    "IMPORTANT INTERPRETATION RULE\n",
    
    "Google Trends activity is only a discovery signal. It is not proof of sales growth, scarcity, financial impact, or investment relevance. Do not let query count, Breakout status, or search growth determine your final investment conclusion.\n\n",
    
    
    # --------------------------------------------------------
    # Discovery context
    # --------------------------------------------------------
    
    "DISCOVERY CONTEXT\n",
    
    "Parent topic: ", parent_title, "\n",
    
    "Distinct supporting queries: ", n_queries, "\n",
    
    "Breakout queries: ", breakout_count, "\n",
    
    "High-growth queries (>= ",
    HIGH_GROWTH_THRESHOLD,
    "%): ",
    high_growth_count,
    "\n",
    
    "Highest observed non-Breakout growth: ",
    ifelse(
      is.finite(max_growth_value),
      paste0(max_growth_value, "%"),
      "Not available"
    ),
    "\n",
    
    "Independent originating seed families: ",
    origin_seed_count,
    "\n",
    
    "Originating seeds: ",
    origin_seeds,
    "\n\n",
    
    "SUPPORTING GOOGLE TRENDS EVIDENCE\n",
    
    evidence_text,
    
    "\n\n",
    
    
    # --------------------------------------------------------
    # Research process
    # --------------------------------------------------------
    
    "RESEARCH PROCESS REQUIREMENT\n",
    
    "Conduct a multi-source web investigation before reaching conclusions.\n",
    
    "Do not answer primarily from prior model knowledge.\n",
    
    "For every important factual claim, search for current supporting evidence.\n",
    
    "Use multiple independent sources when possible and prioritize primary sources.\n",
    
    "Open and inspect relevant source pages rather than relying only on search-result snippets.\n\n",
    
    
    # --------------------------------------------------------
    # Scope
    # --------------------------------------------------------
    
    "CRITICAL SCOPE RULE\n",
    
    "The supplied Google Trends queries define the scope of the investigation.\n",
    
    "Do not introduce a current event, product, company development, news story, or investment thesis merely because it relates to the parent topic.\n",
    
    "A real-world development is a valid trend explanation ONLY when at least one supplied Google Trends query can reasonably be connected to it.\n",
    
    "If none of the supplied queries can be connected to a verified development, classify the cluster as misassigned or unexplained and recommend rejection.\n",
    
    "Do not rescue a weak Google Trends cluster by finding unrelated current news about the same person, company, brand, retailer, or topic.\n\n",
    
    
    # --------------------------------------------------------
    # Research discipline
    # --------------------------------------------------------
    
    "RESEARCH DISCIPLINE\n",
    
    "- The objective is to determine whether the topic deserves investor attention, not to justify researching it.\n",
    
    "- Do not try to make every topic look important.\n",
    
    "- A conclusion that the topic is interesting but not investable is a successful research outcome.\n",
    
    "- If financial impact is immaterial, public-company exposure is weak, or no investment thesis exists, state that explicitly.\n",
    
    "- Do not populate tables merely to make the report appear comprehensive.\n",
    
    "- A short accurate table is preferred over a long speculative table.\n\n",
    
    
    # --------------------------------------------------------
    # Research rules
    # --------------------------------------------------------
    
    "RESEARCH RULES\n",
    
    "1. Disambiguate the parent term. Determine whether it refers to a company, brand, product, generic phrase, person, event, or several unrelated entities.\n",
    
    "2. Group the supplied queries into distinct subthemes. Do not merge unrelated developments merely because they share a retailer, company, or brand.\n",
    
    "3. For each meaningful subtheme, establish whether a verified real-world development explains the search activity.\n",
    
    "4. Clearly separate verified facts, reasonable inferences, and unresolved speculation.\n",
    
    "5. Prefer evidence published or updated within the past 30 days. Use older evidence only for ownership, history, capital structure, or context.\n",
    
    "6. Distinguish the date an event happened from the date an article was published or updated.\n",
    
    "7. Verify every ticker, exchange, legal entity, ownership relationship, brand-owner relationship, and material investment relationship using authoritative sources whenever available.\n",
    
    "8. Explain the financial transmission mechanism through revenue, units, pricing, margins, inventory, fulfillment, customer traffic, market share, capital spending, credit quality, investment income, fair value, or valuation.\n",
    
    "9. Compare the likely scale of the event with the relevant company, business segment, investment portfolio, or product category. A popular product can still be financially negligible.\n",
    
    "10. Classify persistence as one-day curiosity, short viral cycle, seasonal, recurring, or structural.\n",
    
    "11. Identify catalysts and explicit falsification criteria.\n",
    
    "12. Do not force an investment thesis. A negligible, unquantifiable, or non-investable conclusion is acceptable.\n\n",
    
    
    # --------------------------------------------------------
    # Ownership and look-through
    # --------------------------------------------------------
    
    "STRICT OWNERSHIP AND LOOK-THROUGH RULES\n",
    
    "- Do NOT stop the ownership analysis merely because the immediate brand owner or manufacturer is private.\n",
    
    "- First identify the product, brand, operating company, legal owner, and ultimate corporate parent using authoritative sources.\n",
    
    "- If any relevant operating company is private, perform a second-stage look-through ownership and capital-provider analysis.\n",
    
    "- Specifically investigate whether the private company is owned, controlled, financed, or materially invested in by a publicly traded parent company, holding company, business development company (BDC), private-equity vehicle with a listed parent, investment company, publicly traded strategic investor, or other listed entity with meaningful economic exposure.\n",
    
    "- Search SEC filings, annual reports, quarterly reports, portfolio schedules, investor-relations materials, acquisition announcements, financing disclosures, and management commentary for evidence of these relationships.\n",
    
    "- For investment companies and BDCs, determine whether the private company appears in the listed company's investment portfolio and, when disclosed, identify cost basis, fair value, debt exposure, preferred/common equity exposure, percentage ownership, portfolio concentration, realized/unrealized appreciation, and changes in valuation over time.\n",
    
    "- Distinguish legal ownership from economic exposure. A public company does not need to own 100% of the private operating company to have a potentially meaningful financial interest.\n",
    
    "- If the private company is a portfolio company of a listed investment vehicle, treat this as a potentially DIRECT economic linkage and investigate its materiality rather than dismissing it as private-company-only exposure.\n",
    
    "- Look for management commentary explicitly connecting the consumer trend, product demand, production expansion, revenue growth, earnings, or valuation of the private company to the listed investor's financial results or NAV.\n",
    
    "- Every ownership, investment, financing, or portfolio-company relationship must be verified using authoritative sources whenever available.\n",
    
    "- If ownership or economic exposure cannot be verified with high confidence, write: 'Ownership/economic exposure could not be verified.'\n",
    
    "- Do not assign the same brand to multiple owners without explaining the corporate structure.\n\n",
    
    
    # --------------------------------------------------------
    # Public-company exposure
    # --------------------------------------------------------
    
    "STRICT PUBLIC-COMPANY EXPOSURE RULES\n",
    
    "- Do NOT include a public company merely because it operates in the same industry, sells similar products, or could theoretically benefit.\n",
    
    "- Include a public company only when there is a clear, evidence-backed economic transmission mechanism from the verified consumer trend or event to that company.\n",
    
    "- Evaluate BOTH operating exposure and investment/ownership exposure.\n",
    
    "- Valid operating exposure includes: ownership of the affected brand; manufacturing; primary supplier relationship; primary distribution; meaningful retailer exposure; licensing; or explicit operational exposure.\n",
    
    "- Valid investment exposure includes: a publicly traded parent; listed holding company; BDC; investment company; listed strategic investor; or other public entity with a material debt or equity investment in the affected private company.\n",
    
    "- For a listed investment vehicle or BDC, investigate whether changes in the private company's financial performance can affect fair value, NAV, unrealized appreciation/depreciation, investment income, realized gains, portfolio concentration, or shareholder returns.\n",
    
    "- Give particular attention when the affected private company is one of the listed investor's largest portfolio holdings or when management has explicitly attributed valuation changes to the product or trend being investigated.\n",
    
    "- Distinguish exposure types as: direct operating, indirect operating, direct investment, indirect investment, or incidental.\n",
    
    "- Invalid reasons include: selling similar products; generic retail availability; competing in the same category; theoretical benefit; broad industry membership; or being a peer company without an identifiable economic transmission mechanism.\n",
    
    "- Do not inflate the exposure table. A single strongly verified investment connection is more valuable than several weak retailer or competitor connections.\n\n",
    
    
    # --------------------------------------------------------
    # Financial reasoning
    # --------------------------------------------------------
    
    "FINANCIAL REASONING RULES\n",
    
    "- When the relevant company is a private portfolio company of a listed investment vehicle, compare the size of that investment with the listed vehicle's NAV, total investment portfolio, net assets, and earnings when data are available.\n",
    
    "- Investigate whether recent changes in the private company's fair value, unrealized appreciation, or portfolio carrying value have already reflected the consumer trend.\n",
    
    "- Distinguish between an already-recognized valuation effect and evidence suggesting further incremental upside or downside.\n",
    
    "- If management has explicitly linked product demand to portfolio-company financial performance or valuation, treat that statement as high-value evidence and cite it prominently.\n",
    
    "- Separate verified facts from illustrative assumptions.\n",
    
    "- Never present estimated revenue, margins, earnings impact, unit sales, market share, or valuation effects as factual unless explicitly reported by a reliable source.\n",
    
    "- Whenever making an assumption or scenario calculation, label it 'Illustrative scenario'.\n",
    
    "- Avoid false precision.\n",
    
    "- If sufficient information is unavailable, state that the financial impact cannot be quantified reliably.\n\n",
    
    
    # --------------------------------------------------------
    # Instrument-specific transmission
    # --------------------------------------------------------
    
    "FAIR-VALUE/NAV VS. INCOME TRANSMISSION CHECK\n",
    
    "For every material public-company exposure identified through an investment, ownership stake, financing relationship, or portfolio holding, distinguish between FAIR-VALUE/NAV sensitivity and INCOME/CASH-FLOW sensitivity.\n\n",
    
    "1. Identify the exact investment instrument whenever evidence permits: common equity, preferred equity, debt, convertible security, or another instrument. Do not treat these instruments as economically equivalent.\n",
    
    "2. FAIR-VALUE / NAV SENSITIVITY: Determine whether improved or deteriorating performance at the underlying company could change the fair value of the public company's investment and therefore affect NAV, book value, or reported portfolio value. Quantify the position relative to the public company's portfolio, NAV, assets, or another appropriate denominator when reliable data permit.\n",
    
    "3. INCOME / CASH-FLOW SENSITIVITY: Separately determine whether improved or deteriorating performance at the underlying company could change the cash income received by the public company, including interest, dividends, distributions, fees, or other contractual payments.\n",
    
    "4. For DEBT, distinguish contractual interest income and credit/recovery risk from changes in the borrower's enterprise value. Stronger sales or profits do not automatically increase interest income.\n",
    
    "5. For PREFERRED EQUITY, determine when possible whether dividends are fixed, variable, participating, cumulative, PIK, or otherwise performance-sensitive, and whether the security has conversion, redemption, liquidation-preference, or participation rights. Do not assume that stronger underlying-company performance automatically increases current investment income.\n",
    
    "6. For COMMON EQUITY, distinguish appreciation in the value of the ownership stake from dividends or cash distributions actually received.\n",
    
    "7. For CONVERTIBLE or HYBRID securities, identify which features create debt-like versus equity-like sensitivity when reliable terms are available.\n",
    
    "8. If contractual terms cannot be verified, explicitly state that the income transmission mechanism is unknown or unverified. Do not infer contractual economics from the security label alone.\n",
    
    "9. Never write that stronger sales, profits, demand, or valuation at a portfolio company automatically increase the public investor's earnings, dividends, or cash flow unless the investment terms or reported financial results support that conclusion.\n\n",
    
    
    # --------------------------------------------------------
    # Mandatory public-market look-through
    # --------------------------------------------------------
    
    "PUBLIC-MARKET LOOK-THROUGH CHECK (MANDATORY)\n",
    
    "Before assigning the public-company exposure and financial-materiality scores, answer these questions internally:\n",
    
    "1. Is the immediate operating company private?\n",
    
    "2. If yes, who owns, finances, or has significant equity or debt exposure to it?\n",
    
    "3. Is any such investor, parent, BDC, investment company, holding company, or strategic investor publicly traded?\n",
    
    "4. Is the private company financially meaningful to that public investor? Examine portfolio concentration, fair value, cost basis, NAV contribution, or other disclosed measures when available.\n",
    
    "5. Has the public investor disclosed changes in the private company's valuation, financial performance, investment carrying value, unrealized appreciation/depreciation, or investment income?\n",
    
    "6. Has management explicitly connected the underlying product demand or consumer trend to the private company's performance, production expansion, valuation, or financial results?\n",
    
    "7. Has the economic benefit already been substantially recognized in the public investor's reported fair value or NAV, or does current evidence plausibly indicate incremental upside or downside not yet reflected?\n",
    
    "8. Which part of the public-company exposure is sensitive to FAIR VALUE/NAV and which part is sensitive to CURRENT INCOME/CASH FLOW?\n\n",
    
    "Do not finalize the public-company exposure score, financial-materiality score, or overall investment score until this look-through check is complete.\n\n",
    
    
    # --------------------------------------------------------
    # Investment score
    # --------------------------------------------------------
    
    "INDEPENDENT INVESTMENT SCORE\n",
    
    "Score the topic only AFTER completing the research using these components:\n",
    
    "- Verified event: 0-20\n",
    "- Financial materiality: 0-25\n",
    "- Public-company exposure: 0-15\n",
    "- Evidence quality: 0-15\n",
    "- Persistence: 0-10\n",
    "- Catalyst clarity: 0-10\n",
    "- Informational value: 0-5\n",
    
    "Total: 0-100.\n",
    
    "Priority classification: RED = 80-100; ORANGE = 65-79; YELLOW = 50-64; WHITE = below 50.\n\n",
    
    
    # --------------------------------------------------------
    # Required report
    # --------------------------------------------------------
    
    "REQUIRED REPORT STRUCTURE\n\n",
    
    "## 0. Weekly-editor snapshot\n",
    
    "Provide exactly: topic; one-sentence verified development; principal subtheme; primary trend driver; strongest verified evidence; strongest verified public-company connection or 'None'; financial materiality; evidence quality; persistence classification; investment score; priority classification; recommended status (publish, monitor, reject); three reasons supporting the classification; principal reason not to prioritize it.\n\n",
    
    "## 1. Topic disambiguation\n",
    
    "Explain valid, ambiguous, unrelated, or misassigned meanings. Identify and reject false entity matches.\n\n",
    
    "## 2. Subtheme table\n",
    
    "For each subtheme list supporting queries, highest growth, Breakout status, validity, and explanation. Keep unrelated subthemes separate.\n\n",
    
    "## 3. Verified development by subtheme\n",
    
    "State what actually happened, when it happened, and the evidence. Clearly label unresolved claims.\n\n",
    
    "## 4. Demand, supply, and persistence\n",
    
    "Assess verified demand evidence, inventory, stockouts, restocks, production or distribution constraints, and likely duration. Do not treat Google Trends growth itself as proof of demand or scarcity.\n\n",
    
    "## 5. Ownership, capital-provider, and public-company map\n",
    
    "For every verified subtheme construct the full economic chain where applicable:\n",
    
    "product/event -> brand -> operating company -> legal owner -> ultimate parent -> major investor/BDC/holding company -> publicly traded economic beneficiary.\n\n",
    
    "For each entity report: legal entity name; private/public status; ownership or investment relationship; percentage ownership when disclosed; debt/equity instrument when disclosed; cost basis; fair value; portfolio significance; ticker and exchange; relationship confidence; and authoritative evidence.\n\n",
    
    "If the immediate company is private, explicitly state whether you performed a look-through search for public investors or portfolio owners and report the result.\n\n",
    
    "If a publicly traded investment company owns or finances the private company, investigate the scale of that exposure before concluding that public-company exposure is immaterial.\n\n",
    
    "## 6. Public-company exposure table\n",
    
    "Include only companies that pass the STRICT PUBLIC-COMPANY EXPOSURE RULES above.\n",
    
    "Columns: Company | legal entity | ticker | exchange | exposure type | investment instrument | transmission mechanism | portfolio significance | direct/indirect | confidence | financial materiality | evidence | principal risk.\n\n",
    
    "For investment-company or BDC exposure, explicitly discuss possible effects on NAV, fair value, unrealized appreciation/depreciation, investment income, and realized gains where applicable.\n\n",
    
    "## 7. Financial relevance\n",
    
    "For every material public-company connection, report separately:\n",
    
    "- Investment instrument\n",
    
    "- Position fair value or carrying value, if available\n",
    
    "- Position as a percentage of portfolio, NAV, assets, or another appropriate denominator, if calculable\n",
    
    "- FAIR-VALUE/NAV transmission mechanism\n",
    
    "- INCOME/CASH-FLOW transmission mechanism\n",
    
    "- Whether income is fixed, variable, participating, contractual, performance-sensitive, or unknown\n",
    
    "- Evidence supporting the instrument terms\n",
    
    "- What remains unknown or unverified\n",
    
    "- Whether the consumer trend appears already reflected in current fair-value marks\n",
    
    "- Overall financial materiality\n\n",
    
    "Separate verified facts from illustrative assumptions. Label any scenario calculation 'Illustrative scenario'. If the impact cannot be quantified reliably, say so.\n\n",
    
    "## 8. Investment score\n",
    
    "Show every component score, explain each component, calculate the total, and assign RED/ORANGE/YELLOW/WHITE. If the event is interesting but not investable, state that explicitly.\n\n",
    
    "## 9. Bull case, bear case, and falsification criteria\n",
    
    "Provide a realistic bull case, bear case, and explicit falsification criteria. Do not invent bullish narratives. If no realistic bull case exists, say so. If the bear case is substantially stronger than the bull case, state that explicitly.\n\n",
    
    "## 10. Catalysts and monitoring plan\n",
    
    "Provide known catalysts, a 7-day monitoring plan, a 30-day monitoring plan, and events that would invalidate the thesis. Avoid generic monitoring suggestions; every item must relate directly to the verified event.\n\n",
    
    
    # --------------------------------------------------------
    # Internal audit + source check
    # --------------------------------------------------------
    
    "## 11. Internal consistency and source-claim audit (MANDATORY)\n",
    
    "Before returning the report, perform a complete internal audit and correct any inconsistency.\n\n",
    
    "Check all of the following:\n",
    
    "- dates and chronology;\n",
    "- ownership consistency;\n",
    "- direct/indirect exposure consistency;\n",
    "- investment-instrument classification;\n",
    "- fair-value/NAV versus income/cash-flow transmission consistency;\n",
    "- materiality consistency;\n",
    "- facts versus assumptions;\n",
    "- placeholder text;\n",
    "- contradictions between the weekly-editor snapshot and detailed sections;\n",
    "- duplicate citations;\n",
    "- preference for authoritative sources.\n\n",
    
    "SOURCE-CLAIM CONSISTENCY CHECK\n",
    
    "Before finalizing the report, audit every important citation.\n",
    
    "For each citation verify that:\n",
    
    "1. The cited source is actually the source described.\n",
    
    "2. The source directly supports the adjacent claim.\n",
    
    "3. Company names, URLs, tickers, dates, ownership relationships, investment instruments, and financial figures match the cited source.\n",
    
    "4. An unrelated source was not accidentally attached to the claim.\n",
    
    "5. A secondary source is not being used when a directly available primary filing, company disclosure, regulatory source, or official record would materially improve reliability.\n\n",
    
    "If a citation does not directly support the claim, replace the citation, rewrite the claim to match the evidence, or mark the claim as unverified.\n",
    
    "Do not retain a citation merely because it is topically related.\n",
    
    "Correct all inconsistencies before returning the report.\n\n",
    
    
    # --------------------------------------------------------
    # Report length
    # --------------------------------------------------------
    
    "## Report length\n",
    
    "- Investment score below 50: target approximately 1,000-1,500 words.\n",
    
    "- Investment score 50-70: target approximately 2,000-2,500 words.\n",
    
    "- Investment score above 70: produce the full comprehensive report.\n",
    
    "Do not make low-relevance topics artificially long simply because information is available.\n\n",
    
    
    # --------------------------------------------------------
    # Verdict
    # --------------------------------------------------------
    
    "## 12. Final verdict\n",
    
    "State exactly one status: publish, monitor, or reject. Also state the priority color and one-sentence rationale. If financial impact is immaterial, no meaningful public-company exposure exists, or no investable thesis exists, state this explicitly. A conclusion that says 'This is not an investment opportunity' is a valid and successful outcome.\n\n",
    
    
    # --------------------------------------------------------
    # Sources
    # --------------------------------------------------------
    
    "## 13. Full sources\n",
    
    "For every principal source provide title, publisher, publication date, event date when different, source quality, claim supported, and full official URL. Preserve full URLs and remove duplicate citations.\n\n",
    
    "Do not provide personalized financial advice or unconditional buy/sell recommendations."
  )
}
# NOTE:
# research_prompt is intentionally built AFTER the coherence gate and
# canonical-story deduplication. At this point research_queue still contains
# candidate-level rows and may contain duplicate representations of one story.

print(
  research_queue |>
    select(parent_title, discovery_score, discovery_priority, queue_status,
           queue_reason, entity_confidence, n_queries, breakout_count,
           high_growth_count, origin_seed_count)
)

# ============================================================
# 14. OpenAI request helpers
# ============================================================

# ============================================================
# OpenAI configuration
# ============================================================

OPENAI_API_KEY <- Sys.getenv("OPENAI_API_KEY")

if (!nzchar(OPENAI_API_KEY)) {
  stop(
    paste0(
      "OPENAI_API_KEY is not configured.\n",
      "Set it as an environment variable before running the pipeline.\n",
      "Locally, you can store it in .Renviron.\n",
      "On GitHub Actions, it will be provided through GitHub Secrets."
    )
  )
}

OPENAI_RESEARCH_MODEL <- "gpt-5"
OPENAI_EDITOR_MODEL <- "gpt-5"
OPENAI_COHERENCE_MODEL <- "gpt-5-mini"

openai_curl_request <- function(
    url,
    method = "GET",
    body = NULL,
    api_key = OPENAI_API_KEY,
    timeout_s = 180,
    retries = 3
) {
  output_file <- tempfile(fileext = ".json")
  body_file <- tempfile(fileext = ".json")
  on.exit(unlink(c(output_file, body_file), force = TRUE), add = TRUE)
  
  if (!is.null(body)) {
    jsonlite::write_json(
      body, body_file,
      auto_unbox = TRUE,
      null = "null",
      pretty = FALSE
    )
  }
  
  last_status <- NA_integer_
  
  for (attempt in seq_len(retries)) {
    args <- c(
      "--silent", "--show-error", "--location", "--request", method,
      "--header", shQuote(paste0("Authorization: Bearer ", api_key)),
      "--header", shQuote("Content-Type: application/json"),
      "--connect-timeout", "30",
      "--max-time", as.character(timeout_s),
      "--retry", "2",
      "--retry-delay", "3",
      "--output", shQuote(output_file),
      "--write-out", "%{http_code}"
    )
    
    if (!is.null(body)) {
      args <- c(args, "--data-binary", paste0("@", shQuote(body_file)))
    }
    
    args <- c(args, shQuote(url))
    
    curl_result <- tryCatch(
      system2("curl.exe", args = args, stdout = TRUE, stderr = TRUE),
      error = function(e) e
    )
    
    if (!inherits(curl_result, "error")) {
      last_status <- suppressWarnings(as.integer(tail(curl_result, 1)))
    }
    
    response_exists <- file.exists(output_file) &&
      !is.na(file.info(output_file)$size) &&
      file.info(output_file)$size > 0
    
    if (
      response_exists &&
      !is.na(last_status) &&
      last_status >= 200 &&
      last_status < 300
    ) {
      response_text <- paste(
        readLines(output_file, warn = FALSE, encoding = "UTF-8"),
        collapse = "\n"
      )
      
      return(
        jsonlite::fromJSON(
          response_text,
          simplifyVector = FALSE
        )
      )
    }
    
    if (attempt < retries) Sys.sleep(3 * attempt)
  }
  
  error_preview <- if (file.exists(output_file)) {
    paste(
      readLines(output_file, warn = FALSE, encoding = "UTF-8"),
      collapse = "\n"
    )
  } else {
    ""
  }
  
  stop(
    "OpenAI request failed. HTTP status: ",
    last_status,
    "\nResponse:\n",
    substr(error_preview, 1, 2000)
  )
}

start_web_research <- function(
    prompt,
    model = OPENAI_RESEARCH_MODEL
) {
  request_body <- list(
    model = model,
    background = TRUE,
    tools = list(
      list(
        type = "web_search_preview",
        search_context_size = "high"
      )
    ),
    input = prompt
  )
  
  openai_curl_request(
    url = "https://api.openai.com/v1/responses",
    method = "POST",
    body = request_body,
    timeout_s = 180
  )
}

get_openai_response <- function(response_id) {
  openai_curl_request(
    paste0("https://api.openai.com/v1/responses/", response_id),
    method = "GET",
    timeout_s = 180
  )
}

wait_for_openai_response <- function(
    response_id,
    poll_every_s = 30,
    max_polls = 120,
    max_consecutive_failures = 8
) {
  terminal <- c("completed", "failed", "cancelled", "incomplete")
  consecutive_failures <- 0L
  
  for (poll_number in seq_len(max_polls)) {
    response <- tryCatch(
      get_openai_response(response_id),
      error = function(e) {
        message("Temporary polling failure: ", conditionMessage(e))
        NULL
      }
    )
    
    if (is.null(response)) {
      consecutive_failures <- consecutive_failures + 1L
      
      if (consecutive_failures >= max_consecutive_failures) {
        stop(
          "Polling stopped after ", consecutive_failures,
          " consecutive network failures.\n",
          "The background response may still be running.\n",
          "Response ID: ", response_id
        )
      }
      
      Sys.sleep(min(120, poll_every_s * consecutive_failures))
      next
    }
    
    consecutive_failures <- 0L
    status <- response$status %||% "unknown"
    
    message(
      "Research status: ", status,
      " | Check ", poll_number,
      " of ", max_polls
    )
    
    if (status %in% terminal) {
      if (status != "completed") {
        reason <- response$error$message %||%
          response$incomplete_details$reason %||%
          "No detailed error returned."
        
        stop(
          "Research ended with status: ", status,
          "\nReason: ", reason,
          "\nResponse ID: ", response_id
        )
      }
      return(response)
    }
    
    Sys.sleep(poll_every_s)
  }
  
  stop(
    "Research did not complete within polling limit.\n",
    "Response ID: ", response_id
  )
}

extract_response_text <- function(response) {
  text_parts <- purrr::map(
    response$output %||% list(),
    function(item) {
      if (!identical(item$type, "message")) return(character())
      
      purrr::map_chr(
        item$content %||% list(),
        function(content) {
          if (content$type %in% c("output_text", "text")) {
            as.character(content$text %||% "")
          } else {
            ""
          }
        }
      )
    }
  ) |>
    unlist(recursive = TRUE, use.names = FALSE)
  
  paste(text_parts[nzchar(text_parts)], collapse = "\n\n")
}

extract_url_citations <- function(response) {
  rows <- list()
  
  for (item in response$output %||% list()) {
    if (!identical(item$type, "message")) next
    
    for (content in item$content %||% list()) {
      for (annotation in content$annotations %||% list()) {
        citation <- annotation$url_citation %||% annotation
        url <- citation$url %||% NA_character_
        
        if (!is.na(url)) {
          rows[[length(rows) + 1]] <- tibble(
            title = as.character(citation$title %||% NA_character_),
            url = as.character(url),
            start_index = suppressWarnings(
              as.integer(citation$start_index %||% NA_integer_)
            ),
            end_index = suppressWarnings(
              as.integer(citation$end_index %||% NA_integer_)
            )
          )
        }
      }
    }
  }
  
  if (!length(rows)) {
    return(
      tibble(
        title = character(),
        url = character(),
        start_index = integer(),
        end_index = integer()
      )
    )
  }
  
  bind_rows(rows) |>
    distinct(title, url, .keep_all = TRUE)
}

build_coherence_prompt <- function(
    parent_title,
    evidence_text
) {
  
  paste0(
    "You are a strict pre-research classifier for a weekly consumer-demand and market-intelligence system.\n\n",
    "Parent topic: ", parent_title, "\n\n",
    "Google Trends evidence:\n", evidence_text, "\n\n",
    "Determine whether these queries form a coherent story related to consumer demand, ",
    "scarcity, stockouts, restocks, recalls, pricing, supply constraints, ",
    "distribution disruption, inventory, retailer availability, or another ",
    "commercially relevant real-world development.\n\n",
    "Do NOT reward a cluster merely because the parent is famous, a public company, ",
    "a stock ticker, a celebrity, or currently in the news.\n",
    "The supporting queries must plausibly refer to the same underlying consumer ",
    "or market development.\n\n",
    "Also identify the canonical entity or story name that should be used to group ",
    "duplicate Google Trends candidates.\n",
    "Examples:\n",
    '- Parent "why are needohs sold out everywhere" should likely canonicalize to "NeeDoh".\n',
    '- Parent "needoh" should canonicalize to "NeeDoh".\n',
    '- Parent "walmart grocery price cuts" may canonicalize to "Walmart grocery pricing".\n\n',
    "The canonical_entity should be concise and should identify the product, brand, ",
    "company, commodity, or event actually represented by the searches.\n",
    "If two differently worded candidates clearly describe the same underlying product ",
    "or story, use the SAME canonical_entity wording for both.\n\n",
    "Return ONLY valid JSON with exactly these fields:\n",
    "{\n",
    '  "coherent": true,\n',
    '  "coherence_score": 0,\n',
    '  "consumer_relevance_score": 0,\n',
    '  "canonical_entity": "short canonical story/entity name",\n',
    '  "reason": "short explanation"\n',
    "}\n\n",
    "coherence_score and consumer_relevance_score must be integers from 0 to 100."
  )
}

run_coherence_classifier <- function(
    parent_title,
    evidence_text,
    model = OPENAI_COHERENCE_MODEL
) {
  
  response <- openai_curl_request(
    url = "https://api.openai.com/v1/responses",
    method = "POST",
    body = list(
      model = model,
      input = build_coherence_prompt(
        parent_title,
        evidence_text
      )
    ),
    timeout_s = 180
  )
  
  raw_text <- extract_response_text(response)
  
  cleaned <- raw_text |>
    stringr::str_replace("^```json\\s*", "") |>
    stringr::str_replace("^```\\s*", "") |>
    stringr::str_replace("\\s*```$", "") |>
    stringr::str_trim()
  
  parsed <- tryCatch(
    jsonlite::fromJSON(
      cleaned,
      simplifyVector = TRUE
    ),
    error = function(e) NULL
  )
  
  if (is.null(parsed)) {
    return(
      tibble(
        coherent = FALSE,
        coherence_score = 0,
        consumer_relevance_score = 0,
        canonical_entity = parent_title,
        coherence_reason = paste(
          "Classifier output could not be parsed:",
          substr(raw_text, 1, 300)
        )
      )
    )
  }
  
  canonical_entity_value <- as.character(
    parsed$canonical_entity %||% parent_title
  )
  
  if (
    length(canonical_entity_value) == 0 ||
    is.na(canonical_entity_value) ||
    !nzchar(stringr::str_trim(canonical_entity_value))
  ) {
    canonical_entity_value <- parent_title
  }
  
  tibble(
    coherent = isTRUE(parsed$coherent),
    coherence_score = suppressWarnings(
      as.numeric(parsed$coherence_score %||% 0)
    ),
    consumer_relevance_score = suppressWarnings(
      as.numeric(parsed$consumer_relevance_score %||% 0)
    ),
    canonical_entity = canonical_entity_value,
    coherence_reason = as.character(
      parsed$reason %||% ""
    )
  )
}

if (nrow(research_queue_pre_coherence) > 0) {
  
  coherence_results <- purrr::map_dfr(
    seq_len(nrow(research_queue_pre_coherence)),
    function(i) {
      
      row_i <- research_queue_pre_coherence[i, ]
      
      message(
        "Coherence check ",
        i,
        " of ",
        nrow(research_queue_pre_coherence),
        ": ",
        row_i$parent_title[[1]]
      )
      
      result <- tryCatch(
        run_coherence_classifier(
          row_i$parent_title[[1]],
          row_i$evidence_text[[1]]
        ),
        error = function(e) {
          tibble(
            coherent = FALSE,
            coherence_score = 0,
            consumer_relevance_score = 0,
            canonical_entity = row_i$parent_title[[1]],
            coherence_reason = paste(
              "Coherence classifier failed:",
              conditionMessage(e)
            )
          )
        }
      )
      
      bind_cols(
        tibble(
          parent_title = row_i$parent_title[[1]]
        ),
        result
      )
    }
  )
  
} else {
  
  coherence_results <- tibble(
    parent_title = character(),
    coherent = logical(),
    coherence_score = double(),
    consumer_relevance_score = double(),
    canonical_entity = character(),
    coherence_reason = character()
  )
}

research_queue_scored <- research_queue_pre_coherence |>
  left_join(
    coherence_results,
    by = "parent_title"
  ) |>
  mutate(
    coherence_pass =
      dplyr::coalesce(coherent, FALSE) &
      dplyr::coalesce(coherence_score, 0) >=
      MIN_COHERENCE_SCORE_FOR_RESEARCH &
      dplyr::coalesce(consumer_relevance_score, 0) >=
      MIN_COHERENCE_SCORE_FOR_RESEARCH,
    
    canonical_entity =
      dplyr::coalesce(
        canonical_entity,
        parent_title
      ),
    
    canonical_story_key =
      normalize_query_text(
        canonical_entity
      ),
    
    original_parent_title =
      parent_title
  ) |>
  filter(
    coherence_pass
  )

if (nrow(research_queue_scored) > 0) {
  
  research_queue <- research_queue_scored |>
    arrange(
      canonical_story_key,
      desc(discovery_score),
      desc(coherence_score),
      desc(consumer_relevance_score)
    ) |>
    group_by(
      canonical_story_key
    ) |>
    summarise(
      parent_title =
        first(canonical_entity),
      
      discovery_score =
        safe_max(discovery_score),
      
      discovery_priority =
        first(discovery_priority),
      
      queue_status =
        first(queue_status),
      
      queue_reason =
        paste(
          unique(
            queue_reason[
              !is.na(queue_reason) &
                nzchar(queue_reason)
            ]
          ),
          collapse = " | "
        ),
      
      entity_confidence =
        first(entity_confidence),
      
      coherence_score =
        safe_max(coherence_score),
      
      consumer_relevance_score =
        safe_max(consumer_relevance_score),
      
      coherence_reason =
        paste(
          unique(
            coherence_reason[
              !is.na(coherence_reason) &
                nzchar(coherence_reason)
            ]
          ),
          collapse = " | "
        ),
      
      n_queries =
        sum(n_queries, na.rm = TRUE),
      
      breakout_count =
        sum(breakout_count, na.rm = TRUE),
      
      high_growth_count =
        sum(high_growth_count, na.rm = TRUE),
      
      max_growth_value =
        safe_max(max_growth_value),
      
      origin_seed_count =
        safe_max(origin_seed_count),
      
      origin_seeds =
        paste(
          unique(
            origin_seeds[
              !is.na(origin_seeds) &
                nzchar(origin_seeds)
            ]
          ),
          collapse = " | "
        ),
      
      evidence_text =
        paste(
          unique(
            evidence_text[
              !is.na(evidence_text) &
                nzchar(evidence_text)
            ]
          ),
          collapse = "\n"
        ),
      
      merged_candidate_count =
        n(),
      
      merged_candidate_names =
        paste(
          unique(original_parent_title),
          collapse = " | "
        ),
      
      .groups = "drop"
    ) |>
    arrange(
      desc(discovery_score)
    ) |>
    slice_head(
      n = MAX_WEEKLY_PARENT_REPORTS
    )
  
} else {
  
  research_queue <- tibble(
    canonical_story_key = character(),
    parent_title = character(),
    discovery_score = double(),
    discovery_priority = character(),
    queue_status = character(),
    queue_reason = character(),
    entity_confidence = character(),
    coherence_score = double(),
    consumer_relevance_score = double(),
    coherence_reason = character(),
    n_queries = integer(),
    breakout_count = integer(),
    high_growth_count = integer(),
    max_growth_value = double(),
    origin_seed_count = double(),
    origin_seeds = character(),
    evidence_text = character(),
    merged_candidate_count = integer(),
    merged_candidate_names = character()
  )
}

readr::write_csv(
  research_queue_pre_coherence |>
    left_join(
      coherence_results,
      by = "parent_title"
    ),
  file.path(
    WEEKLY_OUTPUT_DIR,
    "coherence_audit.csv"
  )
)

message(
  "Candidates before coherence gate: ",
  nrow(research_queue_pre_coherence)
)

message(
  "Candidates passing coherence gate before deduplication: ",
  nrow(research_queue_scored)
)

message(
  "Unique stories after canonical deduplication: ",
  nrow(research_queue)
)

if (nrow(research_queue) > 0) {
  
  research_queue <- research_queue |>
    mutate(
      research_prompt =
        purrr::pmap_chr(
          list(
            parent_title,
            n_queries,
            breakout_count,
            high_growth_count,
            max_growth_value,
            origin_seed_count,
            origin_seeds,
            evidence_text
          ),
          build_parent_research_prompt
        )
    )
  
} else {
  
  research_queue$research_prompt <- character()
}

print(
  research_queue |>
    select(
      parent_title,
      merged_candidate_count,
      merged_candidate_names,
      discovery_score,
      coherence_score,
      consumer_relevance_score,
      n_queries,
      breakout_count,
      high_growth_count,
      origin_seed_count
    ),
  n = Inf,
  width = Inf
)

if (nrow(research_queue) > 0) {
  
  stopifnot(
    "research_prompt" %in%
      names(research_queue)
  )
  
  stopifnot(
    is.character(
      research_queue$research_prompt
    )
  )
  
  stopifnot(
    all(
      nzchar(
        research_queue$research_prompt
      )
    )
  )
}

# ============================================================
# 15. Run web research
# ============================================================

parent_research_results <- vector("list", nrow(research_queue))

if (nrow(research_queue) > 0) {
  for (i in seq_len(nrow(research_queue))) {
    row_i <- research_queue[i, ]
    parent_name <- row_i$parent_title[[1]]
    parent_prompt <- row_i$research_prompt[[1]]
    
    message(
      "Starting parent research ",
      i, " of ", nrow(research_queue),
      ": ", parent_name
    )
    
    base_result <- list(
      parent_title = parent_name,
      discovery_score = row_i$discovery_score[[1]],
      discovery_priority = row_i$discovery_priority[[1]],
      queue_status = row_i$queue_status[[1]],
      queue_reason = row_i$queue_reason[[1]],
      entity_confidence = row_i$entity_confidence[[1]],
      coherence_score = row_i$coherence_score[[1]],
      consumer_relevance_score = row_i$consumer_relevance_score[[1]],
      coherence_reason = row_i$coherence_reason[[1]],
      merged_candidate_count = row_i$merged_candidate_count[[1]],
      merged_candidate_names = row_i$merged_candidate_names[[1]],
      n_queries = row_i$n_queries[[1]],
      breakout_count = row_i$breakout_count[[1]],
      high_growth_count = row_i$high_growth_count[[1]],
      origin_seed_count = row_i$origin_seed_count[[1]],
      evidence_text = row_i$evidence_text[[1]],
      prompt = parent_prompt
    )
    
    job <- tryCatch(
      start_web_research(parent_prompt),
      error = function(e) {
        message(
          "Could not start research for ",
          parent_name, ": ",
          conditionMessage(e)
        )
        NULL
      }
    )
    
    if (is.null(job)) {
      parent_research_results[[i]] <- c(
        base_result,
        list(status = "failed_to_start")
      )
      next
    }
    
    slug <- parent_name |>
      stringr::str_to_lower() |>
      stringr::str_replace_all("[^a-z0-9]+", "_") |>
      stringr::str_replace_all("^_|_$", "")
    
    run_state_file <- file.path(
      WEEKLY_OUTPUT_DIR,
      paste0("active_research_", slug, ".csv")
    )
    
    readr::write_csv(
      tibble(
        parent_title = parent_name,
        response_id = job$id,
        started_at = as.character(Sys.time()),
        status = job$status %||% "submitted"
      ),
      run_state_file
    )
    
    response <- tryCatch(
      wait_for_openai_response(
        job$id,
        poll_every_s = 30,
        max_polls = 120
      ),
      error = function(e) {
        message(
          "Research polling failed for ",
          parent_name, ": ",
          conditionMessage(e)
        )
        NULL
      }
    )
    
    if (is.null(response)) {
      parent_research_results[[i]] <- c(
        base_result,
        list(
          response_id = job$id,
          status = "polling_failed"
        )
      )
      next
    }
    
    report <- extract_response_text(response)
    sources <- extract_url_citations(response)
    
    parent_research_results[[i]] <- c(
      base_result,
      list(
        response_id = job$id,
        status = response$status %||% "completed",
        response = response,
        report = report,
        sources = sources,
        input_tokens = response$usage$input_tokens %||% NA_integer_,
        output_tokens = response$usage$output_tokens %||% NA_integer_,
        total_tokens = response$usage$total_tokens %||% NA_integer_,
        web_search_requests =
          response$tool_usage$web_search$num_requests %||% NA_integer_
      )
    )
    
    readr::write_csv(
      tibble(
        parent_title = parent_name,
        response_id = job$id,
        completed_at = as.character(Sys.time()),
        status = response$status %||% "completed"
      ),
      run_state_file
    )
  }
}

# ============================================================
# 16. Save parent reports and summaries
# ============================================================

for (result in parent_research_results) {
  if (is.null(result) || is.null(result$parent_title)) next
  slug <- result$parent_title |> str_to_lower() |> str_replace_all("[^a-z0-9]+", "_") |> str_replace_all("^_|_$", "")
  folder <- file.path(WEEKLY_OUTPUT_DIR, "topic_reports", paste0("research_", slug))
  dir.create(folder, recursive = TRUE, showWarnings = FALSE)
  
  metadata <- list(
    parent_title = result$parent_title,
    discovery_score = result$discovery_score %||% NA_real_,
    discovery_priority = result$discovery_priority %||% NA_character_,
    queue_status = result$queue_status %||% NA_character_,
    queue_reason = result$queue_reason %||% NA_character_,
    entity_confidence = result$entity_confidence %||% NA_character_,
    coherence_score = result$coherence_score %||% NA_real_,
    consumer_relevance_score = result$consumer_relevance_score %||% NA_real_,
    coherence_reason = result$coherence_reason %||% NA_character_,
    merged_candidate_count = result$merged_candidate_count %||% NA_integer_,
    merged_candidate_names = result$merged_candidate_names %||% NA_character_,
    n_queries = result$n_queries %||% NA_integer_,
    breakout_count = result$breakout_count %||% NA_integer_,
    high_growth_count = result$high_growth_count %||% NA_integer_,
    origin_seed_count = result$origin_seed_count %||% NA_integer_,
    response_id = result$response_id %||% NA_character_,
    status = result$status %||% NA_character_,
    saved_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
  write_json(metadata, file.path(folder, "metadata.json"), auto_unbox = TRUE, null = "null", pretty = TRUE)
  if (!is.null(result$evidence_text)) writeLines(result$evidence_text, file.path(folder, "trend_evidence_used.txt"), useBytes = TRUE)
  if (!is.null(result$prompt)) writeLines(result$prompt, file.path(folder, "research_prompt.txt"), useBytes = TRUE)
  if (!is.null(result$report)) writeLines(result$report, file.path(folder, "topic_research_report.md"), useBytes = TRUE)
  if (!is.null(result$response)) write_json(result$response, file.path(folder, "deep_research_response.json"), auto_unbox = TRUE, null = "null", pretty = TRUE)
  if (!is.null(result$sources)) write_csv(result$sources, file.path(folder, "research_sources.csv"))
  merged_names <- stringr::str_split(
    result$merged_candidate_names %||% result$parent_title,
    "\\s*\\|\\s*"
  )[[1]]
  
  write_csv(
    trend_candidates |>
      filter(parent_title %in% merged_names),
    file.path(folder, "trend_candidates_used.csv")
  )
}

parent_research_summary <- map_dfr(parent_research_results, function(result) {
  if (is.null(result) || is.null(result$parent_title)) return(tibble())
  tibble(
    parent_title = result$parent_title,
    discovery_score = result$discovery_score %||% NA_real_,
    discovery_priority = result$discovery_priority %||% NA_character_,
    queue_status = result$queue_status %||% NA_character_,
    queue_reason = result$queue_reason %||% NA_character_,
    entity_confidence = result$entity_confidence %||% NA_character_,
    coherence_score = result$coherence_score %||% NA_real_,
    consumer_relevance_score = result$consumer_relevance_score %||% NA_real_,
    coherence_reason = result$coherence_reason %||% NA_character_,
    merged_candidate_count = result$merged_candidate_count %||% NA_integer_,
    merged_candidate_names = result$merged_candidate_names %||% NA_character_,
    n_queries = result$n_queries %||% NA_integer_,
    breakout_count = result$breakout_count %||% NA_integer_,
    high_growth_count = result$high_growth_count %||% NA_integer_,
    origin_seed_count = result$origin_seed_count %||% NA_integer_,
    response_id = result$response_id %||% NA_character_,
    status = result$status %||% NA_character_,
    report_available = !is.null(result$report) && nzchar(result$report),
    source_count = if (!is.null(result$sources)) nrow(result$sources) else 0L,
    input_tokens = result$input_tokens %||% NA_integer_,
    output_tokens = result$output_tokens %||% NA_integer_,
    total_tokens = result$total_tokens %||% NA_integer_,
    web_search_requests = result$web_search_requests %||% NA_integer_
  )
})

write_csv(parent_research_summary, file.path(WEEKLY_OUTPUT_DIR, "parent_research_summary.csv"))
print(parent_research_summary)

# ============================================================
# 17. Build weekly editor packet
# ============================================================

safe_truncate <- function(x, max_characters = MAX_REPORT_CHARACTERS_FOR_EDITOR) {
  x <- as.character(x %||% "")
  if (!nzchar(x) || nchar(x, type = "chars") <= max_characters) return(x)
  paste0(substr(x, 1, max_characters), "\n\n[Report truncated for editor context.]")
}

completed_weekly_reports <- keep(
  parent_research_results,
  function(result) {
    !is.null(result) &&
      identical(result$status %||% "", "completed") &&
      nchar(result$report %||% "", type = "chars") >= MIN_REPORT_CHARACTERS &&
      !is.null(result$sources) &&
      nrow(result$sources) >= MIN_REPORT_SOURCES
  }
)

weekly_editor_packet <- map_chr(completed_weekly_reports, function(result) {
  source_text <- if (!is.null(result$sources) && nrow(result$sources) > 0) {
    paste0("\n\nEXTRACTED SOURCE URLS\n",
           paste0("- ", result$sources$title, ": ", result$sources$url, collapse = "\n"))
  } else ""
  
  paste0(
    "\n\n========================================\n",
    "TOPIC: ", result$parent_title, "\n",
    "DISCOVERY CONTEXT ONLY — NOT AN INVESTMENT RANK\n",
    "DISTINCT SUPPORTING QUERIES: ", result$n_queries, "\n",
    "BREAKOUT QUERIES: ", result$breakout_count, "\n",
    "HIGH-GROWTH QUERIES: ", result$high_growth_count, "\n",
    "========================================\n\n",
    safe_truncate(result$report), source_text
  )
}) |> paste(collapse = "\n")

writeLines(weekly_editor_packet, file.path(WEEKLY_OUTPUT_DIR, "weekly_editor_packet.txt"), useBytes = TRUE)

# ============================================================
# 18. Weekly editor prompt
# ============================================================

# ============================================================
# Weekly editor prompt
# ============================================================

build_weekly_editor_prompt <- function(
    issue_id,
    issue_date,
    reports_packet,
    max_stories = MAX_WEEKLY_BRIEFING_STORIES
) {
  
  paste0(
    
    "You are the senior editor of a weekly consumer-demand, market-intelligence, and investment-research briefing.\n\n",
    
    "Issue: ",
    issue_id,
    "\n",
    
    "Issue date: ",
    issue_date,
    "\n",
    
    "Geography: United States\n",
    
    "Observation window: approximately the preceding seven days.\n\n",
    
    "You are receiving completed analyst reports. Compare them; do not merely concatenate them and do not perform a fresh broad investigation.\n",
    
    "Use web search only to verify questionable, contradictory, ownership-related, financially material, citation-related, instrument-related, or time-sensitive claims from analyst reports. Do not use web search for new broad discovery.\n\n",
    
    
    # --------------------------------------------------------
    # Editorial principles
    # --------------------------------------------------------
    
    "EDITORIAL PRINCIPLES\n",
    
    "1. Rank topics using verified post-research investment relevance, not Google Trends popularity, query count, Breakout count, or discovery scores.\n",
    
    "2. Do not merge unrelated subthemes merely because they share a retailer, company, industry, or brand.\n",
    
    "3. A public-company mention is insufficient. Require a credible, evidence-backed economic transmission mechanism.\n",
    
    "4. Do not include companies merely because they sell similar products, compete in the same category, could theoretically benefit, or belong to the same industry.\n",
    
    "5. Prefer verified, persistent, financially material developments over novelty-driven attention.\n",
    
    "6. Penalize ambiguous entities, weak ownership mapping, unsupported shortages, one-day spikes, duplicated stories, speculative exposure tables, and unsupported financial-transmission claims.\n",
    
    "7. Preserve the strongest source citations and full URLs from analyst reports, but verify important citations before relying on them.\n",
    
    "8. Select no more than ",
    max_stories,
    " lead stories, but do not force any story to qualify.\n",
    
    "9. A lead story should normally have a verified event, credible public-company link, at least moderate financial materiality, strong evidence, and a plausible catalyst or persistence mechanism.\n",
    
    "10. RED = 80-100; ORANGE = 65-79; YELLOW = 50-64; WHITE = below 50.\n",
    
    "11. If no topic scores at least 50, state exactly: 'No lead investment story qualified this week.' Put surviving topics under Monitoring signals instead of forcing Ranked stories.\n",
    
    "12. If you materially change an analyst investment score, explain why and identify the evidence responsible for the change.\n",
    
    "13. For each rejected topic, provide one explicit reason: ambiguous entity, no verified event, private-company-only exposure, negligible materiality, stale trend, unsupported viral claim, speculative company exposure, duplicate story, or unsupported transmission mechanism.\n",
    
    "14. Check analyst reports for internal inconsistencies before synthesizing them. If a report contains contradictory ownership, impossible dates, unsupported financial precision, incorrect security classification, or obvious factual conflict, independently verify the issue when possible and reduce confidence rather than repeating it as fact.\n",
    
    "15. For investment, ownership, financing, or portfolio-company exposure, verify the exact instrument when material to the thesis and distinguish FAIR-VALUE/NAV sensitivity from INCOME/CASH-FLOW sensitivity. Debt, preferred equity, common equity, and hybrid securities must not be treated as economically equivalent.\n",
    
    "16. Do not assume that stronger sales, profits, demand, or valuation at an underlying company automatically increase the public investor's earnings or cash income.\n",
    
    "17. For DEBT, distinguish contractual interest income and credit/recovery effects from changes in enterprise value. Higher borrower profits do not automatically increase interest income.\n",
    
    "18. For PREFERRED EQUITY, distinguish fair-value/NAV effects from dividend or distribution rights. Verify whether dividends are fixed, variable, participating, cumulative, PIK, or otherwise performance-sensitive when the terms are available.\n",
    
    "19. For COMMON EQUITY, distinguish appreciation in the ownership stake from dividends or cash distributions actually received.\n",
    
    "20. For CONVERTIBLE or HYBRID securities, distinguish debt-like contractual economics from equity-like participation when evidence permits.\n",
    
    "21. If the analyst claims an income benefit but contractual terms or financial disclosures do not support that transmission mechanism, remove or qualify the income claim, identify it under Analyst-quality flags, and score the thesis using only the verified mechanism.\n",
    
    "22. When reliable figures are available, quantify material exposure relative to the public company's portfolio, NAV, assets, revenue, earnings, or another economically appropriate denominator. Do not manufacture precision when the denominator is unavailable.\n",
    
    "23. Prefer primary filings, company disclosures, regulatory records, and official sources when verifying ownership, portfolio exposure, financial figures, security types, dates, or materiality.\n",
    
    "24. Do not provide personalized financial advice or unconditional buy/sell calls.\n\n",
    
    
    # --------------------------------------------------------
    # Source verification
    # --------------------------------------------------------
    
    "SOURCE VERIFICATION RULE (MANDATORY)\n",
    
    "Audit all important citations and source descriptions before finalizing the weekly report.\n\n",
    
    "For each important claim verify that:\n",
    
    "1. The cited source is actually the source described.\n",
    
    "2. The source directly supports the claim it is attached to.\n",
    
    "3. Company names, legal entities, URLs, tickers, exchanges, dates, ownership relationships, security types, and financial figures match the source.\n",
    
    "4. An unrelated or merely topically similar source has not been attached accidentally.\n",
    
    "5. The analyst has not confused debt exposure with equity exposure, cost with fair value, current-period values with prior-period values, or one portfolio company's figures with another's.\n",
    
    "6. The analyst has not confused a fair-value/NAV benefit with an income/cash-flow benefit.\n",
    
    "7. A secondary source is not being relied upon when a primary SEC filing, investor-relations disclosure, regulatory document, or official company record is available and materially stronger.\n\n",
    
    "If an analyst citation is mismatched, irrelevant, stale, or weaker than an available authoritative source, replace it with the stronger source when verification is possible.\n",
    
    "If the underlying claim cannot be verified, qualify or remove the claim rather than repeating it.\n",
    
    "Do not preserve an analyst citation merely because it appeared in the original report.\n\n",
    
    
    # --------------------------------------------------------
    # Financial verification
    # --------------------------------------------------------
    
    "FINANCIAL VERIFICATION AND CORRECTION RULE\n",
    
    "Independently verify any financial figure that materially affects ranking, investment score, public-company exposure, or financial-materiality conclusions.\n",
    
    "When possible, verify cost basis, fair value, portfolio percentage, NAV, debt balance, equity value, ownership percentage, security type, and reported appreciation/depreciation against the most recent primary filing available as of the issue date.\n",
    
    "If the analyst used an older reporting period and a newer filing exists within the permissible issue-date window, prefer the newer filing and explain any material change.\n",
    
    "If you discover a material numerical, ownership, instrument-classification, or transmission-mechanism error, correct it explicitly in the weekly report and explain how the correction affects the investment score or editorial ranking.\n\n",
    
    
    # --------------------------------------------------------
    # Instrument transmission audit
    # --------------------------------------------------------
    
    "INSTRUMENT-SPECIFIC TRANSMISSION AUDIT (MANDATORY)\n",
    
    "For every material investment-company, BDC, holding-company, or portfolio-company connection, determine:\n",
    
    "1. What instrument creates the exposure: debt, preferred equity, common equity, convertible/hybrid security, or another instrument?\n",
    
    "2. What can change the FAIR VALUE of that instrument and therefore affect NAV or book value?\n",
    
    "3. What can change the CURRENT CASH INCOME produced by the instrument?\n",
    
    "4. Is the income contractual, fixed, floating, participating, performance-sensitive, discretionary, or unknown?\n",
    
    "5. Has any increase in underlying-company value already been reflected in current fair-value marks?\n",
    
    "6. What incremental economic effect remains possible if the consumer trend persists?\n\n",
    
    "Do not collapse fair-value appreciation and current income into one generic 'benefit' statement.\n\n",
    
    
    # --------------------------------------------------------
    # Chronology
    # --------------------------------------------------------
    
    "ISSUE-DATE DISCIPLINE\n",
    
    "Do not use information that occurred after the stated issue date as if it were known during the issue window.\n",
    
    "If a source was accessed later but describes an event occurring before or during the issue window, clearly distinguish event date, publication/update date, and access date.\n",
    
    "Do not allow later information to leak backward into the historical weekly assessment unless explicitly labeled as a subsequent update.\n\n",
    
    
    # --------------------------------------------------------
    # Required output
    # --------------------------------------------------------
    
    "REQUIRED MARKDOWN OUTPUT\n\n",
    
    "# ",
    issue_id,
    " Weekly Market Intelligence\n\n",
    
    "## Executive briefing\n",
    
    "Write 4-7 concise bullets. Begin with whether any lead investment story qualified.\n\n",
    
    "## Ranked stories\n",
    
    "Include only topics scoring at least 50 after research and editorial verification.\n",
    
    "For each selected story provide rank, distinct topic or subtheme, RED/ORANGE/YELLOW priority, investment score, verified development, why it matters, strongest verified public-company connection, investment instrument when relevant, fair-value/NAV mechanism, income/cash-flow mechanism, financial materiality, evidence quality, catalyst, monitoring trigger, one-sentence editorial verdict, and key Markdown-linked sources.\n",
    
    "Avoid language such as 'tradable' unless the evidence supports a sufficiently clear and timely market mechanism. Prefer terms such as 'public-market look-through', 'research-worthy', 'monitoring candidate', or 'investment-relevant theme' when appropriate.\n",
    
    "If none qualifies, write the required quiet-week sentence instead.\n\n",
    
    "## Comparison table\n",
    
    "Rank | Topic/subtheme | Priority | Investment score | Verified driver | Public-company exposure | Instrument | Fair-value/NAV mechanism | Income/cash-flow mechanism | Materiality | Evidence quality | Status\n\n",
    
    "## Monitoring signals\n",
    
    "List credible topics below 50 separately by subtheme. Explain what evidence would move each upward.\n\n",
    
    "## Rejected or low-value signals\n",
    
    "List rejected topics and one explicit rejection reason each.\n\n",
    
    "## Analyst-quality flags\n",
    
    "List any factual, ownership, chronology, citation, source-claim, security-classification, fair-value/NAV, income-transmission, or financial-reasoning inconsistencies detected in the underlying analyst reports.\n",
    
    "Explicitly flag material numerical corrections, mismatched citations, unsupported income claims, and instrument-classification errors.\n",
    
    "If none, state 'No material analyst-quality flags identified.'\n\n",
    
    "## Next-week watchlist\n",
    
    "For each surviving topic state move-up, move-down, and drop conditions.\n\n",
    
    "## Sources\n",
    
    "Provide a deduplicated list with story supported, title, publisher, publication date, event date when relevant, full URL, source quality, and claim supported.\n",
    
    "Prefer primary sources for ownership, financial exposure, filings, portfolio values, security terms, regulatory status, and company-specific claims.\n\n",
    
    "COMPLETED ANALYST REPORTS\n",
    
    reports_packet
  )
}

weekly_editor_prompt <- build_weekly_editor_prompt(
  ISSUE_ID, as.character(ISSUE_DATE), weekly_editor_packet
)
writeLines(weekly_editor_prompt, file.path(WEEKLY_OUTPUT_DIR, "weekly_editor_prompt.txt"), useBytes = TRUE)

# ============================================================
# 19. Run weekly editor and save publication
# ============================================================

run_weekly_editor <- function(
    prompt,
    model = OPENAI_EDITOR_MODEL
) {
  openai_curl_request(
    url = "https://api.openai.com/v1/responses",
    method = "POST",
    body = list(
      model = model,
      tools = list(
        list(
          type = "web_search_preview",
          search_context_size = "medium"
        )
      ),
      input = prompt
    ),
    timeout_s = 600,
    retries = 3
  )
}

weekly_editor_response <- NULL
weekly_briefing <- ""

if (length(completed_weekly_reports) == 0) {
  warning("No completed parent reports were available. The weekly briefing was not generated.")
} else {
  weekly_editor_response <- tryCatch(
    run_weekly_editor(weekly_editor_prompt),
    error = function(e) { message("Weekly editor request failed: ", conditionMessage(e)); NULL }
  )
  if (!is.null(weekly_editor_response)) weekly_briefing <- extract_response_text(weekly_editor_response)
}

# ============================================================
# Save weekly briefing
# ============================================================

# If the weekly editor produced no briefing, create a minimal
# quiet-week publication so every scheduled run can still be
# reviewed and published.
if (!nzchar(weekly_briefing)) {
  weekly_briefing <- paste0(
    "# ", ISSUE_ID, " Weekly Market Intelligence\n\n",
    "## Executive briefing\n\n",
    "No completed research stories qualified for inclusion ",
    "in this week's market-intelligence briefing.\n\n",
    "## Ranked stories\n\n",
    "No qualifying investment-relevant stories were identified ",
    "during this research cycle.\n\n",
    "## Monitoring signals\n\n",
    "No monitoring candidates were available from completed research.\n\n",
    "## Rejected or low-value signals\n\n",
    "See the pipeline diagnostics and candidate audit for the ",
    "underlying discovery results.\n\n",
    "## Analyst-quality flags\n\n",
    "No completed analyst reports were available for editorial review.\n\n",
    "## Next-week watchlist\n\n",
    "Continue the automated consumer-demand discovery process ",
    "during the next weekly research cycle.\n"
  )
}

# Save the raw Markdown version with the pipeline artifacts.
WEEKLY_MARKDOWN_FILE <- file.path(
  WEEKLY_OUTPUT_DIR,
  "weekly_market_intelligence.md"
)

writeLines(
  weekly_briefing,
  WEEKLY_MARKDOWN_FILE,
  useBytes = TRUE
)

message(
  "Weekly Markdown briefing saved as: ",
  normalizePath(
    WEEKLY_MARKDOWN_FILE,
    winslash = "/",
    mustWork = FALSE
  )
)

# ============================================================
# Create Quarto research page
# ============================================================

QUARTO_REPORT_FILE <- file.path(
  RESEARCH_DIR,
  paste0(ISSUE_ID, ".qmd")
)

# The weekly editor already writes an H1 such as:
# # 2026-W36 Weekly Market Intelligence
# Quarto supplies the page title through YAML front matter, so
# remove the first H1 to avoid showing the title twice.
quarto_body <- weekly_briefing

quarto_body <- sub(
  "^#[[:space:]]+[^\\r\\n]+[\\r\\n]+",
  "",
  quarto_body
)

quarto_body <- sub(
  "^[\\r\\n]+",
  "",
  quarto_body
)

quarto_front_matter <- c(
  "---",
  paste0('title: "', ISSUE_ID, ' Weekly Market Intelligence"'),
  paste0('date: "', as.character(ISSUE_DATE), '"'),
  "categories:",
  "  - Consumer Demand",
  "  - Market Intelligence",
  "toc: true",
  "---",
  ""
)

quarto_page <- c(
  quarto_front_matter,
  quarto_body
)

writeLines(
  quarto_page,
  QUARTO_REPORT_FILE,
  useBytes = TRUE
)

message(
  "Quarto research page created: ",
  normalizePath(
    QUARTO_REPORT_FILE,
    winslash = "/",
    mustWork = FALSE
  )
)
if (!is.null(weekly_editor_response)) {
  write_json(weekly_editor_response, file.path(WEEKLY_OUTPUT_DIR, "weekly_editor_response.json"),
             auto_unbox = TRUE, null = "null", pretty = TRUE)
}

pipeline_diagnostics <- tibble(
  metric = c(
    "raw_seed_queries", "unique_expansion_seeds", "expanded_query_rows",
    "unique_normalized_queries", "parent_candidates", "automatic_candidates",
    "exploratory_candidates", "rejected_candidates", "reports_requested",
    "reports_completed", "briefing_created"
  ),
  value = c(
    nrow(harvest_related_queries), nrow(expanded_seeds), nrow(expanded_rq),
    n_distinct(trend_candidates$normalized_query), nrow(parent_candidates),
    sum(parent_candidates$queue_status == "automatic"),
    sum(parent_candidates$queue_status == "exploratory"),
    sum(parent_candidates$queue_status == "reject"),
    nrow(research_queue), length(completed_weekly_reports), nzchar(weekly_briefing)
  )
)
write_csv(pipeline_diagnostics, file.path(WEEKLY_OUTPUT_DIR, "pipeline_diagnostics.csv"))
print(pipeline_diagnostics)

weekly_manifest <- list(
  issue_id = ISSUE_ID,
  issue_date = as.character(ISSUE_DATE),
  research_window = TIME_WINDOW,
  seed_queries = SEED_QUERIES,
  parent_candidates = nrow(parent_candidates),
  automatic_candidates = sum(parent_candidates$queue_status == "automatic"),
  exploratory_candidates = sum(parent_candidates$queue_status == "exploratory"),
  rejected_candidates = sum(parent_candidates$queue_status == "reject"),
  parent_reports_requested = nrow(research_queue),
  coherence_candidates_before_gate = nrow(research_queue_pre_coherence),
  coherence_candidates_after_gate = nrow(research_queue),
  parent_reports_completed = length(completed_weekly_reports),
  maximum_lead_stories = MAX_WEEKLY_BRIEFING_STORIES,
  research_model = OPENAI_RESEARCH_MODEL,
  editor_model = OPENAI_EDITOR_MODEL,
  briefing_created = nzchar(weekly_briefing),
  created_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")
)
write_json(weekly_manifest, file.path(WEEKLY_OUTPUT_DIR, "weekly_manifest.json"),
           auto_unbox = TRUE, null = "null", pretty = TRUE)


# ============================================================
# Update Weekly Research index
# ============================================================

RESEARCH_INDEX_FILE <- file.path(
  RESEARCH_DIR,
  "index.qmd"
)

AUTO_INDEX_START <- "<!-- AUTO-WEEKLY-START -->"
AUTO_INDEX_END   <- "<!-- AUTO-WEEKLY-END -->"

weekly_report_files <- list.files(
  RESEARCH_DIR,
  pattern = "^[0-9]{4}-W[0-9]{2}\\.qmd$",
  full.names = TRUE
)

extract_yaml_value <- function(lines, field) {
  pattern <- paste0(
    "^",
    field,
    ":[[:space:]]*[\"']?(.*?)[\"']?[[:space:]]*$"
  )

  match_line <- grep(
    pattern,
    lines,
    value = TRUE
  )

  if (length(match_line) == 0) {
    return(NA_character_)
  }

  value <- sub(
    pattern,
    "\\1",
    match_line[[1]]
  )

  stringr::str_trim(value)
}

weekly_index_data <- purrr::map_dfr(
  weekly_report_files,
  function(report_file) {
    lines <- readLines(
      report_file,
      warn = FALSE,
      encoding = "UTF-8"
    )

    filename <- basename(report_file)

    issue_id <- stringr::str_remove(
      filename,
      "\\.qmd$"
    )

    title <- extract_yaml_value(lines, "title")
    date <- extract_yaml_value(lines, "date")

    if (is.na(title) || !nzchar(title)) {
      title <- paste(issue_id, "Weekly Market Intelligence")
    }

    tibble::tibble(
      issue_id = issue_id,
      title = title,
      date = date,
      filename = filename
    )
  }
)

if (nrow(weekly_index_data) > 0) {
  weekly_index_data <- weekly_index_data |>
    dplyr::arrange(dplyr::desc(issue_id))
}

if (nrow(weekly_index_data) > 0) {
  automatic_index_entries <- purrr::map_chr(
    seq_len(nrow(weekly_index_data)),
    function(i) {
      row <- weekly_index_data[i, ]

      issue_label <- stringr::str_replace(
        row$issue_id,
        "-W",
        " · Week "
      )

      date_text <- ""

      if (!is.na(row$date) && nzchar(row$date)) {
        date_text <- paste0(
          "\n\n**Published:** ",
          row$date
        )
      }

      paste0(
        "### ",
        issue_label,
        "\n\n",
        row$title,
        date_text,
        "\n\n",
        "[Read the full report →](",
        row$filename,
        ")"
      )
    }
  )

  automatic_index_block <- c(
    AUTO_INDEX_START,
    "",
    "## Latest automated reports",
    "",
    paste(
      automatic_index_entries,
      collapse = "\n\n---\n\n"
    ),
    "",
    AUTO_INDEX_END
  )
} else {
  automatic_index_block <- c(
    AUTO_INDEX_START,
    "",
    "## Latest automated reports",
    "",
    "No automated weekly reports have been published yet.",
    "",
    AUTO_INDEX_END
  )
}

if (file.exists(RESEARCH_INDEX_FILE)) {
  index_lines <- readLines(
    RESEARCH_INDEX_FILE,
    warn = FALSE,
    encoding = "UTF-8"
  )
} else {
  index_lines <- c(
    "---",
    'title: "Weekly Research"',
    "---",
    "",
    paste(
      "Consumer Demand Intelligence publishes structured weekly",
      "research on emerging consumer-demand signals and their",
      "potential public-market implications."
    ),
    ""
  )
}

start_position <- which(index_lines == AUTO_INDEX_START)
end_position <- which(index_lines == AUTO_INDEX_END)

if (
  length(start_position) == 1 &&
  length(end_position) == 1 &&
  end_position > start_position
) {
  before_block <- if (start_position > 1) {
    index_lines[seq_len(start_position - 1)]
  } else {
    character()
  }

  after_block <- if (end_position < length(index_lines)) {
    index_lines[(end_position + 1):length(index_lines)]
  } else {
    character()
  }

  index_lines <- c(
    before_block,
    automatic_index_block,
    after_block
  )
} else {
  yaml_end_positions <- which(index_lines == "---")

  if (length(yaml_end_positions) >= 2) {
    yaml_end <- yaml_end_positions[[2]]

    after_yaml <- if (yaml_end < length(index_lines)) {
      index_lines[(yaml_end + 1):length(index_lines)]
    } else {
      character()
    }

    index_lines <- c(
      index_lines[seq_len(yaml_end)],
      "",
      automatic_index_block,
      "",
      after_yaml
    )
  } else {
    index_lines <- c(
      automatic_index_block,
      "",
      index_lines
    )
  }
}

writeLines(
  index_lines,
  RESEARCH_INDEX_FILE,
  useBytes = TRUE
)

message(
  "Weekly Research index updated: ",
  normalizePath(
    RESEARCH_INDEX_FILE,
    winslash = "/",
    mustWork = FALSE
  )
)

# ============================================================
# Validate publication outputs
# ============================================================

message("Validating publication outputs...")

if (!file.exists(QUARTO_REPORT_FILE)) {
  stop(
    paste0(
      "Publication validation failed.\n",
      "Expected Quarto report was not created:\n",
      QUARTO_REPORT_FILE
    )
  )
}

quarto_report_size <- file.info(QUARTO_REPORT_FILE)$size

if (is.na(quarto_report_size) || quarto_report_size < 500) {
  stop(
    paste0(
      "Publication validation failed.\n",
      "Quarto report is unexpectedly small.\n",
      "File: ",
      QUARTO_REPORT_FILE,
      "\n",
      "Size: ",
      quarto_report_size,
      " bytes"
    )
  )
}

quarto_report_lines <- readLines(
  QUARTO_REPORT_FILE,
  warn = FALSE,
  encoding = "UTF-8"
)

quarto_report_text <- paste(
  quarto_report_lines,
  collapse = "\n"
)

yaml_markers <- which(quarto_report_lines == "---")

if (length(yaml_markers) < 2 || yaml_markers[[1]] != 1) {
  stop(
    paste0(
      "Publication validation failed.\n",
      "The generated Quarto report does not contain valid YAML front matter."
    )
  )
}

expected_title_text <- paste0(
  'title: "',
  ISSUE_ID,
  ' Weekly Market Intelligence"'
)

if (!any(quarto_report_lines == expected_title_text)) {
  stop(
    paste0(
      "Publication validation failed.\n",
      "Expected Quarto title was not found:\n",
      expected_title_text
    )
  )
}

required_sections <- c(
  "## Executive briefing",
  "## Ranked stories",
  "## Monitoring signals",
  "## Rejected or low-value signals",
  "## Analyst-quality flags",
  "## Next-week watchlist"
)

missing_sections <- required_sections[
  !vapply(
    required_sections,
    function(section) {
      stringr::str_detect(
        quarto_report_text,
        stringr::fixed(section)
      )
    },
    logical(1)
  )
]

if (length(missing_sections) > 0) {
  stop(
    paste0(
      "Publication validation failed.\n",
      "The following required report sections are missing:\n",
      paste(
        paste0("- ", missing_sections),
        collapse = "\n"
      )
    )
  )
}

if (!file.exists(RESEARCH_INDEX_FILE)) {
  stop(
    paste0(
      "Publication validation failed.\n",
      "Weekly Research index does not exist:\n",
      RESEARCH_INDEX_FILE
    )
  )
}

research_index_lines <- readLines(
  RESEARCH_INDEX_FILE,
  warn = FALSE,
  encoding = "UTF-8"
)

expected_report_link <- paste0(
  "](",
  basename(QUARTO_REPORT_FILE),
  ")"
)

index_contains_report <- any(
  stringr::str_detect(
    research_index_lines,
    stringr::fixed(expected_report_link)
  )
)

if (!index_contains_report) {
  stop(
    paste0(
      "Publication validation failed.\n",
      "research/index.qmd does not link to this week's report:\n",
      basename(QUARTO_REPORT_FILE)
    )
  )
}

start_marker_count <- sum(research_index_lines == AUTO_INDEX_START)
end_marker_count <- sum(research_index_lines == AUTO_INDEX_END)

if (start_marker_count != 1 || end_marker_count != 1) {
  stop(
    paste0(
      "Publication validation failed.\n",
      "Automatic index markers are missing or duplicated.\n",
      "Start markers found: ",
      start_marker_count,
      "\n",
      "End markers found: ",
      end_marker_count
    )
  )
}

message("")
message("============================================")
message("PUBLICATION VALIDATION PASSED")
message("============================================")
message("Issue: ", ISSUE_ID)
message("Report: ", QUARTO_REPORT_FILE)
message("Index: ", RESEARCH_INDEX_FILE)
message("Report size: ", quarto_report_size, " bytes")
message("============================================")
message("")

message("Weekly issue folder: ", normalizePath(WEEKLY_OUTPUT_DIR))
if (nzchar(weekly_briefing)) {
  message("Weekly briefing saved as: ", normalizePath(file.path(WEEKLY_OUTPUT_DIR, "weekly_market_intelligence.md")))
}
message("Weekly market-intelligence pipeline finished.")