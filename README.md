# Threads Lead-Scoring Pipeline

A parallel Playwright scraper that searches [Threads](https://www.threads.net)
for public posts matching a configurable keyword list, extracts each post's
body and replies, classifies every post through a gated scoring pipeline,
and exports a deduplicated Excel workbook of Hot / Warm / General leads.

> Originally built during an internship for a real lead-generation use case.
> Client-specific keywords, branding, and lead data have been removed or
> genericized for this public version. Shared for educational / portfolio
> purposes.

## What it does

- Searches Threads for posts matching a configurable list of keywords
- Runs multiple browser profiles in parallel (one process per logged-in account)
- Opens each matching post, extracts the original post body and its replies
- Classifies every post through an ordered pipeline:
  1. Confirm the topic is actually mentioned in the post body (not just the search snippet)
  2. Exclude sellers, service providers, and advertisements
  3. Score remaining posts on independent intent signals (personal context,
     immediate action, urgency, active problem, research/comparison, etc.)
  4. Bucket into **Hot** / **Warm** / **General discussion** / **Irrelevant**
- Deduplicates by canonical URL and by content fingerprint (catches reposts
  and thread-view duplicates that have different URLs but identical text)
- Exports a ranked Excel workbook with per-lead reasoning, so every
  classification is auditable rather than a black box

## What changed this session

Six bugs found and fixed in the classifier / extractor:

1. `extract_main_post_body` was returning the thread container (original
   post + replies mixed together). Fixed with a DOM check requiring exactly
   one `<time>` element to isolate the original post.
2. `strip_reply_tail` was cutting at the post author's own username line,
   wiping entire post bodies. Fixed with a `seen_op_text` guard so it only
   cuts once real author text has already been captured.
3. The third-person detection regex matched job titles (marketing, sales,
   agent), falsely demoting genuine leads whose text merely mentioned those
   words. Narrowed the pattern to actual third-person pronouns.
4. Quote-post and reply-permalink records were being attributed to the wrong
   author. Added a guard that checks whether the first line of the body is a
   username that doesn't match the URL's author, and skips it if so.
5. Fingerprint collisions on short "N views"-only bodies were causing
   distinct posts to be treated as duplicates. Added a minimum length
   threshold before a post is eligible for fingerprint-based dedup.
6. `KeyboardInterrupt` was escaping a worker's exception handling, since
   `except Exception` doesn't catch it — one interrupted post could kill an
   entire worker process. Added a per-keyword try/except so a single failure
   can't take down a worker.

**Result:** precision on the Hot/Warm pool improved substantially after
these fixes, and the number of manually-verified leads increased several
times over compared to the pre-fix baseline.

## How to run

Install dependencies:

```bash
pip install -r requirements.txt
playwright install chromium
```

Run a self-test of the classifier (no browser required):

```bash
python threads_kpr_scraper.py --self-test
```

Run a full scrape:

```bash
python threads_kpr_scraper.py --target 2000 --output ./output
```

On first run you'll be asked to log in to Threads manually in the opened
browser window — the script never sees your password. Type `stop` and press
Enter at any time during a run to stop safely; progress is checkpointed so
you can resume later.

## Notes

- Runs as a set of parallel worker processes, each with its own persistent
  browser profile, so multiple accounts can scrape different keyword
  subsets simultaneously.
- The classifier is a hand-tuned, regex/rule-based pipeline rather than a
  trained model — interpretable and easy to audit, at the cost of needing
  manual tuning as language patterns drift.
- Automated scraping of a platform via browser automation may fall outside
  that platform's terms of service. This code is shared for educational
  purposes; using it against a live platform is at your own discretion and risk.
