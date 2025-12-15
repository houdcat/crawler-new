"""
Crawler for books.toscrape.com
"""
import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
import multiprocessing as mp
from urllib.parse import urljoin


def clean_price(price_text):
    """
    Convert price text containing currency symbols to a clean float value

    Removes all non-numeric characters except decimal points from price strings.

    :param price_text: Price text that may contain currency symbols and special characters
    :type price_text: str

    :return: Cleaned price as float, or 0.0 if conversion fails
    :rtype: float
    """
    clean = ''
    for char in price_text:
        if char.isdigit() or char == '.':
            clean += char
    return float(clean) if clean else 0.0


def scrape_book(book_url):
    """
    Extracts title, price, stock status, rating, UPC, review count, and category
    from the book's page

    :param book_url: URL of the book's page
    :type book_url: str

    :return: Dictionary containing all extracted book data, None if crawling fails
    :rtype: dict or None
    """
    try:
        # Make HTTP request
        r = requests.get(book_url, timeout=5)

        # Verify the request was successful before processing
        if r.status_code != 200:
            return None

        # Parse HTML content
        soup = BeautifulSoup(r.text, 'html.parser')

        # Extract book title from h1 tag
        title = soup.find('h1').text if soup.find('h1') else 'No title'

        # Extract and clean price from price_color class
        price_elem = soup.find('p', class_='price_color')
        price = clean_price(price_elem.text) if price_elem else 0.0

        # Determine if book is in stock
        stock = soup.find('p', class_='instock availability')
        in_stock = bool(stock and "In stock" in stock.text)

        # Convert textual star rating to numerical value (1-5)
        rating_elem = soup.find('p', class_='star-rating')
        rating = 0
        if rating_elem:
            rating_map = {
                'One': 1,
                'Two': 2,
                'Three': 3,
                'Four': 4,
                'Five': 5
            }
            for word, num in rating_map.items():
                if word in str(rating_elem):
                    rating = num
                    break

        # Extract UPC and review count from product information table
        upc = ''
        reviews = 0
        table = soup.find('table')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    if 'UPC' in th.get_text():
                        upc = td.text.strip()
                    elif 'review' in th.get_text().lower():
                        try:
                            reviews = int(td.text.strip())
                        except ValueError:
                            reviews = 0

        # Extract category from breadcrumb navigation (third item)
        category = 'Unknown'
        breadcrumb = soup.find('ul', class_='breadcrumb')
        if breadcrumb:
            items = breadcrumb.find_all('li')
            if len(items) >= 3:
                category = items[2].get_text(strip=True)

        # Return structured data dictionary with specific field order
        return {
            'url': book_url,
            'title': title,
            'category': category,
            'rating': rating,
            'upc': upc,
            'price': price,
            'currency': 'GBP',
            'in_stock': in_stock,
            'reviews': reviews
        }

    except Exception:
        return None


def worker_process(url_queue, result_queue):
    """
    Worker process function for the multiprocessing pool

    Continuously retrieves URLs from the input queue, crawls them,
    places results in the output queue until a stop request is received

    :param url_queue: Queue containing book URLs to crawl
    :type url_queue: mp.Queue

    :param result_queue: Multiprocessing queue for storing crawling results
    :type result_queue: mp.Queue
    """
    while True:
        try:
            # Get next URL with timeout to prevent indefinite blocking
            book_url = url_queue.get(timeout=2)

            # Check for stop request (None) which signals termination
            if book_url is None:
                break

            # Crawl the book and enqueue the result
            result = scrape_book(book_url)
            if result:
                result_queue.put(result)
            else:
                # Put None to indicate failed scrape while maintaining count
                result_queue.put(None)

        except Exception:
            break


def get_book_urls(base_url, total_pages):
    """
    Collect all book page URLs from all listing pages

    Iterates through all pages and extracts URLs
    for individual books.

    :param base_url: Root URL of the website
    :type base_url: str

    :param total_pages: Total number of listing pages to process
    :type total_pages: int

    :return: List of all book detail page URLs
    :rtype: list[str]
    """
    book_urls = []

    for page in range(1, total_pages + 1):
        if page == 1:
            page_url = base_url
        else:
            page_url = f"{base_url}catalogue/page-{page}.html"

        print(f"  Crawling page {page}...")

        try:
            # Fetch and parse the listing page
            r = requests.get(page_url, timeout=10)
            soup = BeautifulSoup(r.text, 'html.parser')

            # Find all book containers on the page
            books = soup.find_all('article', class_='product_pod')

            print(f"    Found {len(books)} books")

            # Extract and convert each book link to absolute URL
            for book in books:
                link = book.find('h3').find('a')['href']

                # Convert relative URL to absolute URL using current page as base
                book_url = urljoin(page_url, link)

                book_urls.append(book_url)

        except Exception as e:
            print(f"    Error on page {page}: {e}")

    return book_urls


def save_books(books, filename):
    """
    Save a list of book dictionaries to a JSON file

    :param books: List of dictionaries that contain information about
     the books to save
    :type books: list[dict]

    :param filename: Path to the output JSON file
    :type filename: str
    """
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(books, f, indent=2, ensure_ascii=False)


def main():
    """
    Uses all functions to crawl through the page

    :raises SystemExit: If initial page discovery fails
    """

    # Create output directory if it doesn't exist
    if not os.path.exists('data'):
        os.makedirs('data')

    # Generate filename using date and time
    timestamp = datetime.now().strftime("%d-%m-%Y@%H%M%S")
    filename = f"data/books_{timestamp}.json"

    base_url = "https://books.toscrape.com/"

    # Display startup banner
    print("=" * 60)
    print("Crawler for books.toscrape.com")
    print("=" * 60)
    print(f"Output file: {filename}")
    print("-" * 60)

    # Discover total amount of pages
    print("\n[STEP 1] Discovering total pages...")
    try:
        r = requests.get(base_url, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')

        total_pages = 1
        pager = soup.find('ul', class_='pager')
        if pager:
            current = pager.find('li', class_='current')
            if current:
                total_pages = int(current.get_text(strip=True).split()[-1])

        print(f"  Found {total_pages} pages with books")

    except Exception as e:
        print(f"  Error discovering pages: {e}")
        return

    # Collect all book URLs
    print(f"\n[STEP 2] Collecting book URLs from {total_pages} pages...")
    book_urls = get_book_urls(base_url, total_pages)
    print(f"  Total books to crawl: {len(book_urls)}")


    # Crawl books
    print(f"\n[STEP 3] Crawling books with multiprocessing...")

    all_books = []
    crawled_count = 0
    failed_count = 0

    # Create inter-process communication queues
    url_queue = mp.Queue()      # URLs to crawl
    result_queue = mp.Queue()   # Crawled URLs

    # Add all URLs to url_queue
    for url in book_urls:
        url_queue.put(url)

    # Determine optimal number of worker processes
    num_workers = min(4, mp.cpu_count())
    print(f"  Using {num_workers} worker processes")

    # Add stop requests (None values) to signal workers to terminate
    for _ in range(num_workers):
        url_queue.put(None)

    # Start worker processes
    workers = []
    for _ in range(num_workers):
        p = mp.Process(target=worker_process, args=(url_queue, result_queue))
        workers.append(p)
        p.start()

    # Collect and process results from workers
    total_crawl_count = len(book_urls)

    while crawled_count + failed_count < total_crawl_count:
        try:
            # Get next result with timeout to prevent hanging
            result = result_queue.get(timeout=30)

            if result is not None:
                all_books.append(result)
                crawled_count += 1
            else:
                failed_count += 1

            # Display progress every 10 items
            if (crawled_count + failed_count) % 10 == 0:
                print(f"    Processed: {crawled_count + failed_count}/{total_crawl_count} "
                      f"(Success: {crawled_count}, Failed: {failed_count})")

            # Auto-save every 20 books
            if crawled_count % 20 == 0 and crawled_count > 0:
                save_books(all_books, filename)
                print(f"    Auto-saved {crawled_count} books")

        except Exception:
            print("    Timeout waiting for results")
            break

    # Wait for all worker processes to finish
    for p in workers:
        p.join(timeout=5)

    # Final save and print
    print(f"\n[STEP 4] Finalizing...")
    save_books(all_books, filename)

    # Display completion summary
    print("\n" + "=" * 60)
    print("Crawling complete")
    print("=" * 60)
    print(f"Total books crawled: {len(all_books)}")
    print(f"Failed to crawl: {failed_count}")
    print(f"Saved to: {filename}")

    print("\n" + "=" * 60)
    print("Process completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    """
    Script execution starts here
    
    mp.freeze_support() is for Windows compatibility when using
    multiprocessing
    """
    # Required for multiprocessing on Windows
    mp.freeze_support()
    main()