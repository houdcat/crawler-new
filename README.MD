# Web Crawler for books.toscrape.com

**Author:** Filip Houdek  

**Contact:** houdekfilip@email.cz

**School:** SPŠE Ječná

**Date:** 24/11/2025

## 1. Purpose of the Application and User Requirements
The project implements a simple multi-process web crawler for the website books.toscrape.com, designed for educational purposes.
The goal is to collect structured data about books and store them in a JSON file.

### Basic information
- Automatically detects the number of available listing pages.
- Collects all book URLs and processes them in parallel.
- Extracts predefined fields: title, price, stock, rating, UPC, review count, category, and the book URL.
- Shows progress during crawling.
- Saves the results to a JSON file inside the `data` directory.
- Automatically creates the output directory if missing.
- Automatically saves partial results after a defined number of processed items.

## 2. Application Architecture
The program is implemented as a single Python script containing several functional modules:

- scrape_book(url) – Downloads and extracts structured information for one book.
- get_book_urls(base_url, total_pages) – Collects links to all books from all pages.
- worker_process(url_queue, result_queue) – Parallel worker that receives URLs and returns scraped results.
- save_books(data, filename) – Saves gathered data to a JSON file.
- main() – Coordinates all crawling steps, initializes multiprocessing, and manages output.

The architecture relies on:
- queues for inter-process communication
- multiple worker processes running independently
- a central controller (main) responsible for scheduling and saving results

## 3. Application Runtime Description
The program flow is as follows:

1. The crawler starts by displaying an introductory message and generating a timestamped output filename.
2. It downloads the first page to determine how many listing pages exist.
3. All book URLs are collected from each page.
4. These URLs are placed into a multiprocessing queue.
5. A chosen number of worker processes is started. Each worker continuously:
   - reads one URL
   - downloads and parses the book page
   - sends the extracted information back through the result queue
6. The main process receives results and tracks successful and failed scrapes.
7. Results are periodically saved to the output file.
8. After all URLs are processed, workers are stopped, and the final output is stored.
9. The program prints a completion summary.

## 4. Interfaces, Protocols, and Third-Party Dependencies

### Third-party libraries
- requests – HTTP communication
- BeautifulSoup4 (bs4) – HTML parsing
- multiprocessing – parallel processing
- json – serialization of output
- urllib.parse – URL normalization

### Protocols and external dependencies
- Uses HTTP GET requests to retrieve HTML pages.
- No external APIs or databases are used.
- Depends on the website books.toscrape.com.

### Non-functional requirements
- Must be able to terminate cleanly even if some URLs fail.
- Must use request timeouts to prevent infinite waiting.
- Must run correctly on Windows systems (mp.freeze_support).
- Should not exceed the server’s polite request limits.

## 5. Legal and Licensing Aspects
- This project is a school assignment, not intended for commercial use.
- The website books.toscrape.com is openly provided for web-scraping education and does not restrict automated access.
- All code written by the author can be released under an open-source license (e.g., MIT), unless the school specifies otherwise.

## 6. Configuration
Configuration is currently done directly in the script. Important options include:

| Option | Meaning |
|--------|---------|
| base_url | Base URL of the website to crawl |
| num_workers | Number of worker processes (auto-calculated but can be changed) |
| timeout | How long the script waits for a server response |
| JSON output path | Generated automatically, can be modified |

## 7. Installation and Running the Application

### Requirements
- Python 3.10+
- pip-installed libraries:
  ```
  pip install requests beautifulsoup4
  ```

### Running the crawler
1. Download or clone the project.
2. Run the script it using:
   ```
   python crawler.py
   ```
4. The output will be saved into the `data/` directory as a JSON file.

## 8. Error States and Handling

| Error | Cause | Handling |
|-------|--------|-----------|
| Request timeout | Slow or non-responding server | The worker skips the book and reports it as failed |
| Non-200 HTTP code | Missing page or server issue | Result is marked as failed |
| Parsing failure | Unexpected HTML structure | Affected book returns None |
| Queue timeout | Workers finish early | Main loop stops waiting for results |

The crawler continues running even if some books fail to process.

## 9. Verification, Testing, and Validation
Testing was performed on:
- various network speeds
- different numbers of worker processes
- manual review of output JSON

Validation criteria:
- all books produce valid JSON entries
- total number of books matches website’s content
- crawler does not freeze or run indefinitely
- results can be imported into external tools

## 10. Version History and Known Issues

### Version history
- v1.0 – initial release, functional multiprocessing crawler
- v1.1 – improved error handling, autosave, structured output format

### Known issues
- If the website structure changes, scraping may fail.
- A console GUI would be preferable for configuration and controlling the program.

## 11. Import / Export Format Specification

### Export format
The crawler outputs a JSON array of objects. Each object contains:

| Field | Description                        |
|------|------------------------------------|
| url | Book detail page URL               |
| title | Extracted book title               |
| category |  Category from the breadcrumb path |
| rating | Star rating converted to 1–5       |
| upc | Product UPC code                   |
| price | Cleaned numeric price              |
| currency | Always GBP                         |
| in_stock | Boolean stock availability         |
| reviews | Number of reviews                  |