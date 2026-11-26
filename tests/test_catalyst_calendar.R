# ============================================================
# Catalyst-calendar test (no API calls)
#
# Loads the pipeline's functions and settings, then checks which
# remembered stories would be re-checked on several dates, how the
# catalyst brief reads, and how memory updates merge catalysts.
# ============================================================

suppressPackageStartupMessages({
  library(dplyr); library(purrr); library(tibble); library(stringr); library(jsonlite)
})

exprs <- parse("consumer_demand_pipeline.R", keep.source = FALSE)
for (e in exprs) {
  if (!is.call(e) || !identical(e[[1]], as.name("<-"))) next
  lhs <- e[[2]]; rhs <- e[[3]]
  is_fun <- is.call(rhs) && identical(rhs[[1]], as.name("function"))
  is_setting <- is.name(lhs) && grepl("^[A-Z][A-Z0-9_]*$", as.character(lhs)) &&
    (is.atomic(rhs) || (is.call(rhs) && identical(rhs[[1]], as.name("c"))))
  if (is_fun || is_setting) eval(e, envir = globalenv())
}

check <- function(label, ok) {
  cat(if (isTRUE(ok)) "PASS " else "FAIL ", label, "\n", sep = "")
  if (!isTRUE(ok)) failures <<- failures + 1L
}
failures <- 0L

memory <- fromJSON("story_memory.json", simplifyVector = FALSE)$stories
# Simulated September story: PS5 Pro sell-outs after the GTA VI trailer,
# remembered as "monitoring" with the GTA VI release as a catalyst.
memory[[length(memory) + 1]] <- list(
  story_key = "ps5proseptember", title = "PS5 Pro sell-outs after GTA VI trailer",
  first_reported = "2026-W36", last_reported = "2026-W36", last_reported_date = "2026-09-06",
  times_reported = 1L, last_status = "monitoring", last_priority = "WHITE", last_score = 45,
  last_summary = "PS5 Pro sold out after the GTA VI Extended Look; restocked within days.",
  pinned_facts = list(),
  catalysts = list(list(event = "GTA VI release", date = "2026-11-19",
                        beneficiaries = "TTWO; SONY; AMD", source_url = "https://www.rockstargames.com/VI"))
)

keys <- function(sel) vapply(sel, function(s) s$story_key, character(1))

cat("\n== Scenario A: 2026-11-10 (GTA VI release in 9 days)\n")
selA <- select_memory_rechecks(memory, character(), as.Date("2026-11-10"))
print(keys(selA))
check("A: September PS5 story is re-checked despite being 65 days old", "ps5proseptember" %in% keys(selA))
check("A: catalyst check comes first", identical(keys(selA)[1], "ps5proseptember"))
check("A: at most MAX_MEMORY_RECHECKS", length(selA) <= MAX_MEMORY_RECHECKS)

cat("\n== Scenario B: 2026-12-28 (ACOTAR book 7 in 15 days)\n")
selB <- select_memory_rechecks(memory, character(), as.Date("2026-12-28"))
print(keys(selB))
check("B: ACOTAR catalyst check comes first", identical(keys(selB)[1], "acotar"))
check("B: September PS5 story dropped (catalyst passed, story old)", !"ps5proseptember" %in% keys(selB))

cat("\n== Scenario C: 2027-03-01 (nothing in window, all stories old)\n")
selC <- select_memory_rechecks(memory, character(), as.Date("2027-03-01"))
print(keys(selC))
check("C: no re-checks", length(selC) == 0)

cat("\n== Scenario D: 2026-11-10, story already in this week's queue\n")
selD <- select_memory_rechecks(memory, c("PS5 Pro sell-outs after GTA VI trailer"), as.Date("2026-11-10"))
check("D: not re-checked twice", !"ps5proseptember" %in% keys(selD))

cat("\n== Catalyst brief for scenario A\n")
brief <- catalyst_check_rules(selA[[1]], today = as.Date("2026-11-10"))
cat(brief)
check("brief names the event and date", grepl("GTA VI release", brief) && grepl("2026-11-19 \\(in 9 days\\)", brief))
check("brief appended to the follow-up prompt", grepl("CATALYST CHECK", build_memory_recheck_prompt(selA[[1]])))
check("no brief for stories without a catalyst in window", identical(catalyst_check_rules(list()), ""))

cat("\n== Memory update merges and prunes catalysts\n")
upd <- list(stories = list(list(
  story_key = "acotar", title = "ACOTAR", status = "update", priority = "ORANGE", score = 70,
  summary = "x", pinned_facts = list(),
  catalysts = list(
    list(event = "ACOTAR book 7 publication", date = "2027-01-13", beneficiaries = "BMY.L"),
    list(event = "Bloomsbury trading update (month only)", date = "2027-03-01", beneficiaries = "BMY.L"),
    list(event = "Bad date", date = "soon")
  )
)))
mem2 <- update_story_memory(memory, upd, "2026-W48", as.Date("2026-11-29"))
ac <- find_memory_story("acotar", mem2)
print(vapply(ac$catalysts, function(c) paste(c$event, c$date), character(1)))
check("same event updated, not duplicated", sum(grepl("book 7", vapply(ac$catalysts, `[[`, "", "event"))) == 1 &&
        any(vapply(ac$catalysts, `[[`, "", "date") == "2027-01-13"))
check("new catalyst added", length(ac$catalysts) == 2)
check("invalid date ignored", !any(grepl("Bad date", vapply(ac$catalysts, `[[`, "", "event"))))
mem3 <- update_story_memory(mem2, upd, "2027-W12", as.Date("2027-03-20"))
ac3 <- find_memory_story("acotar", mem3)
check("catalysts more than 45 days past are pruned", !any(vapply(ac3$catalysts, `[[`, "", "date") == "2027-01-13"))

cat("\n== Editor memory text shows catalysts\n")
txt <- format_memory_for_editor(memory)
check("editor sees dated catalysts", grepl("Dated catalysts", txt) && grepl("ACOTAR book 7 publication", txt))

cat("\nFailures:", failures, "\n")
if (failures > 0) quit(status = 1)
