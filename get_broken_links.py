import requests
import concurrent.futures
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def get_all_links(url):
    if url.split('.')[-1] == 'ipynb':
        return set()
    links = set()
    try:
        response = requests.get(url, timeout=5)
        #response.raise_for_status()
        parsed_start_url = urlparse(start_url)
        domain =parsed_start_url.netloc # this will be "auto.gluon.ai"
        stable_or_dev = parsed_start_url.path.split('/')[1] 
        if response.status_code == 200 and domain+'/'+stable_or_dev in url:
            soup = BeautifulSoup(response.text, 'html.parser')
            for link in soup.find_all('a'):
                href = link.get('href')
                if href and not href.startswith('#'):  # Exclude anchor links
                    absolute_url = urljoin(url, href)
                    parsed_url = urlparse(absolute_url)
                    if parsed_url.scheme and parsed_url.netloc:  # Ensure valid URL
                        links.add(absolute_url)
            return links
    except requests.Timeout:
        print("Request timed out")
    except Exception as e:
        print(f"Error while processing {url}: {e}")
        links.add(url)
    return links

def check_link_status(link):
    print(link)
    if link.split('.')[-1] == 'ipynb':
        return link, 0
    try:
        response = requests.head(link, allow_redirects=True, timeout=5)
        response.raise_for_status()
        return link, response.status_code
    except requests.exceptions.RequestException as e:
        print(f"Error while checking {link}: {e}")
        return link, str(e)

def main(start_url, filename):
    all_links = set([start_url])
    crawled_links = set()
    broken_links = []
    max_workers = 10  

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while all_links - crawled_links:
            # Process links in batches to improve efficiency
            batch_links = list(all_links - crawled_links)[:max_workers]
            crawled_links |= set(batch_links)

            # Crawl each link and get new links
            links = {executor.submit(get_all_links, link): link for link in batch_links}
            for valid_link in concurrent.futures.as_completed(links):
                new_links = valid_link.result()
                all_links |= (new_links - crawled_links)

            # Check link status concurrently
            link_statuses = {executor.submit(check_link_status, link): link for link in batch_links}
            for link_status in concurrent.futures.as_completed(link_statuses):
                link, status_code = link_status.result()
                crawled_links.add(link)
                if isinstance(status_code, str):
                    if "HTTPSConnectionPool" in status_code:
                        continue
                    broken_links.append((''.join(status_code.split(':')[:2]), link))
                elif status_code >= 400 and status_code != 405:
                    broken_links.append((status_code, link))
            
    # Convert broken links to a pandas DataFrame and display it
    df = pd.DataFrame(broken_links, columns=["Status Code / Error", "URL"])
    df.to_csv(f"Broken Links {filename}")

if __name__ == "__main__":
    start_url = "https://auto.gluon.ai/stable/index.html" 
    main(start_url, "Stable")
    start_url = "https://auto.gluon.ai/dev/index.html" 
    main(start_url, "Dev")