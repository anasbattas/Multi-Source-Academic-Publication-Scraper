import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import random
import spacy
import logging
from concurrent.futures import ThreadPoolExecutor

# ------------------- CONFIGURATION -------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("academic_scraper.log"), logging.StreamHandler()]
)

# Configuration MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["academic_database44"]
collection = db["publications"]

# Configuration spaCy
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    spacy.cli.download("en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")

# Configuration globale
REQUEST_DELAY = (1, 3)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
]

# ------------------- FONCTIONS UTILITAIRES -------------------
def get_random_header():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.google.com/',
        'DNT': str(random.randint(0, 1))
    }

def ethical_delay():
    time.sleep(random.uniform(*REQUEST_DELAY))

def extract_entities(text):
    if not text:
        return {}
    doc = nlp(text)
    return {ent.label_: list(set(ent.text for ent in doc.ents)) for ent in doc.ents}

def is_duplicate(title):
    return collection.count_documents({'title': title}) > 0

# ------------------- SCRAPERS -------------------
class AcademicScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(get_random_header())

    def safe_request(self, url, params=None, retries=3, backoff_factor=0.5):
        for attempt in range(retries):
            try:
                ethical_delay()
                response = self.session.get(url, params=params, timeout=30)  # Increased timeout
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Too Many Requests
                    logging.warning(f"429 Too Many Requests, retrying in {backoff_factor * (2 ** attempt)} seconds")
                    time.sleep(backoff_factor * (2 ** attempt))  # Exponential backoff
                elif e.response.status_code == 403:
                    logging.warning(f"403 Forbidden, retrying in {backoff_factor * (2 ** attempt)} seconds")
                    time.sleep(backoff_factor * (2 ** attempt))
                else:
                    logging.error(f"Request error: {str(e)}")
                    return None
            except Exception as e:
                logging.error(f"Request error: {str(e)}")
                return None

    def arxiv_scraper(self, query, max_results=500):
        results = []
        url = "http://export.arxiv.org/api/query"
        params = {'search_query': query, 'max_results': max_results}
        
        response = self.safe_request(url, params)
        if not response:
            return results

        soup = BeautifulSoup(response.text, 'lxml-xml')
        for entry in soup.find_all('entry'):
            try:
                title = entry.title.text.strip() if entry.title else 'Untitled'
                if is_duplicate(title):
                    continue
                
                publication = {
                    'title': title,
                    'authors': [a.find('name').text for a in entry.find_all('author') if a.find('name')],
                    'year': int(entry.published.text[:4]) if entry.published else None,
                    'journal': 'ArXiv',
                    'abstract': entry.summary.text.strip() if entry.summary else '',
                    'link': entry.id.text if entry.id else '',
                    'keywords': [cat['term'] for cat in entry.find_all('category')],
                    'entities': extract_entities(entry.summary.text if entry.summary else ''),
                    'source': 'arXiv'
                }
                results.append(publication)
            except Exception as e:
                logging.error(f"ArXiv processing error: {str(e)}")

        self._save_results(results)
        return results

    def openalex_scraper(self, query, email, max_results=500):
        results = []
        url = "https://api.openalex.org/works"
        params = {
            'filter': f'title.search:{query}',
            'mailto': email,
            'per_page': 200
        }

        for page in range(1, (max_results // 200) + 2):
            params['page'] = page
            response = self.safe_request(url, params)
            if not response:
                continue

            data = response.json()
            for work in data.get('results', []):
                try:
                    title = work.get('title', 'Untitled')
                    if is_duplicate(title):
                        continue

                    publication = {
                        'title': title,
                        'authors': [a.get('author', {}).get('display_name') for a in work.get('authorships', [])],
                        'year': int(work.get('publication_date', '0000')[:4]) if work.get('publication_date') else None,
                        'journal': work.get('primary_location', {}).get('source', {}).get('display_name', 'Unknown'),
                        'abstract': work.get('abstract', ''),
                        'link': work.get('doi', ''),
                        'keywords': [kw.get('display_name') for kw in work.get('keywords', [])],
                        'entities': extract_entities(work.get('abstract', '')),
                        'source': 'OpenAlex'
                    }
                    results.append(publication)
                except Exception as e:
                    logging.error(f"OpenAlex processing error: {str(e)}")

            if len(results) >= max_results:
                break

        self._save_results(results)
        return results

    def pubmed_scraper(self, query, max_results=500):
        results = []
        base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
        
        # Phase de recherche
        search_params = {
            'db': 'pubmed',
            'term': query,
            'retmax': max_results
        }
        response = self.safe_request(f"{base_url}esearch.fcgi", search_params)
        if not response:
            return results

        soup = BeautifulSoup(response.text, 'lxml-xml')
        ids = [id.text for id in soup.find_all('Id')]

        # Phase de récupération des détails
        for i in range(0, len(ids), 100):
            batch_ids = ids[i:i+100]
            fetch_params = {
                'db': 'pubmed',
                'id': ','.join(batch_ids),
                'retmode': 'xml'
            }
            response = self.safe_request(f"{base_url}efetch.fcgi", fetch_params)
            if not response:
                continue

            details_soup = BeautifulSoup(response.text, 'lxml-xml')
            for article in details_soup.find_all('PubmedArticle'):
                try:
                    title = article.find('ArticleTitle').text.strip() if article.find('ArticleTitle') else 'Untitled'
                    if is_duplicate(title):
                        continue

                    publication = {
                        'title': title,
                        'authors': [f"{auth.find('LastName').text} {auth.find('ForeName').text}".strip() 
                                   for auth in article.find_all('Author') if auth.find('LastName') and auth.find('ForeName')],
                        'year': int(article.find('PubDate').Year.text) if article.find('PubDate') and article.find('PubDate').Year else None,
                        'journal': article.find('Journal').Title.text if article.find('Journal') and article.find('Journal').Title else '',
                        'abstract': ' '.join([t.text for t in article.find_all('AbstractText')]) if article.find_all('AbstractText') else '',
                        'link': f"https://pubmed.ncbi.nlm.nih.gov/{article.find('PMID').text}/" if article.find('PMID') else '',
                        'keywords': [kw.text for kw in article.find_all('Keyword')] if article.find_all('Keyword') else [],
                        'entities': extract_entities(' '.join([t.text for t in article.find_all('AbstractText')])),
                        'source': 'PubMed'
                    }
                    results.append(publication)
                except Exception as e:
                    logging.error(f"PubMed processing error: {str(e)}")

        self._save_results(results)
        return results

    def scilit_scraper(self, query, max_results=500):
        results = []
        url = "https://scilit.net/api/v1/search"
        params = {
            'q': query,
            'limit': max_results
        }

        response = self.safe_request(url, params)
        if not response:
            return results

        data = response.json()
        for item in data.get('results', []):
            try:
                title = item.get('title', 'Untitled')
                if is_duplicate(title):
                    continue

                publication = {
                    'title': title,
                    'authors': [author.get('name') for author in item.get('authors', [])],
                    'year': item.get('year'),
                    'journal': item.get('journal'),
                    'abstract': item.get('abstract', ''),
                    'link': item.get('doi', ''),
                    'keywords': item.get('keywords', []),
                    'entities': extract_entities(item.get('abstract', '')),
                    'source': 'Scilit'
                }
                results.append(publication)
            except Exception as e:
                logging.error(f"Scilit processing error: {str(e)}")

        self._save_results(results)
        return results

    def google_scholar_scraper(self, query, max_results=20):
        results = []
        for start in range(0, max_results, 10):
            url = f"https://scholar.google.com/scholar?start={start}&q={query}"
            response = self.safe_request(url)
            if not response:
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            for item in soup.find_all('div', class_='gs_ri'):
                try:
                    title = item.find('h3').text.strip()
                    if is_duplicate(title):
                        continue

                    publication = {
                        'title': title,
                        'authors': item.find('div', class_='gs_a').text if item.find('div', class_='gs_a') else '',
                        'source': 'Google Scholar',
                        'link': item.find('a')['href'] if item.find('a') else ''
                    }
                    results.append(publication)
                except Exception as e:
                    logging.error(f"Google Scholar processing error: {str(e)}")

        self._save_results(results)
        return results

    def springer_scraper(self, query, max_results=500):
        results = []
        url = "https://link.springer.com/search"
        params = {
            'query': query,
            'show': max_results
        }

        response = self.safe_request(url, params)
        if not response:
            return results

        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.find_all('li', class_='result-item'):
            try:
                title = item.find('h2').text.strip()
                if is_duplicate(title):
                    continue

                publication = {
                    'title': title,
                    'authors': [author.text.strip() for author in item.find_all('span', class_='authors')] if item.find_all('span', class_='authors') else [],
                    'year': item.find('span', class_='year').text.strip() if item.find('span', class_='year') else '',
                    'journal': item.find('span', class_='journal').text.strip() if item.find('span', class_='journal') else '',
                    'link': item.find('a')['href'] if item.find('a') else '',
                    'source': 'Springer'
                }
                results.append(publication)
            except Exception as e:
                logging.error(f"Springer processing error: {str(e)}")

        self._save_results(results)
        return results

    def hal_scraper(self, query, max_results=500):
        results = []
        url = "https://hal.archives-ouvertes.fr/search/index/"
        params = {
            'q': query,
            'rows': max_results
        }

        response = self.safe_request(url, params)
        if not response:
            return results

        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.find_all('div', class_='record'):
            try:
                title = item.find('h2').text.strip()
                if is_duplicate(title):
                    continue

                publication = {
                    'title': title,
                    'authors': [author.text.strip() for author in item.find_all('span', class_='author')] if item.find_all('span', class_='author') else [],
                    'year': item.find('span', class_='year').text.strip() if item.find('span', class_='year') else '',
                    'journal': item.find('span', class_='journal').text.strip() if item.find('span', class_='journal') else '',
                    'link': item.find('a')['href'] if item.find('a') else '',
                    'source': 'HAL'
                }
                results.append(publication)
            except Exception as e:
                logging.error(f"HAL processing error: {str(e)}")

        self._save_results(results)
        return results

    def medline_scraper(self, query, max_results=500):
        results = []
        base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
        
        # Phase de recherche
        search_params = {
            'db': 'medline',
            'term': query,
            'retmax': max_results
        }
        response = self.safe_request(f"{base_url}esearch.fcgi", search_params)
        if not response:
            return results

        soup = BeautifulSoup(response.text, 'lxml-xml')
        ids = [id.text for id in soup.find_all('Id')]

        # Phase de récupération des détails
        for i in range(0, len(ids), 100):
            batch_ids = ids[i:i+100]
            fetch_params = {
                'db': 'medline',
                'id': ','.join(batch_ids),
                'retmode': 'xml'
            }
            response = self.safe_request(f"{base_url}efetch.fcgi", fetch_params)
            if not response:
                continue

            details_soup = BeautifulSoup(response.text, 'lxml-xml')
            for article in details_soup.find_all('MedlineCitation'):
                try:
                    title = article.find('ArticleTitle').text.strip() if article.find('ArticleTitle') else 'Untitled'
                    if is_duplicate(title):
                        continue

                    publication = {
                        'title': title,
                        'authors': [f"{auth.find('LastName').text} {auth.find('ForeName').text}".strip() 
                                   for auth in article.find_all('Author') if auth.find('LastName') and auth.find('ForeName')],
                        'year': int(article.find('PubDate').Year.text) if article.find('PubDate') and article.find('PubDate').Year else None,
                        'journal': article.find('Journal').Title.text if article.find('Journal') and article.find('Journal').Title else '',
                        'abstract': ' '.join([t.text for t in article.find_all('AbstractText')]) if article.find_all('AbstractText') else '',
                        'link': f"https://pubmed.ncbi.nlm.nih.gov/{article.find('PMID').text}/" if article.find('PMID') else '',
                        'keywords': [kw.text for kw in article.find_all('Keyword')] if article.find_all('Keyword') else [],
                        'entities': extract_entities(' '.join([t.text for t in article.find_all('AbstractText')])),
                        'source': 'Medline'
                    }
                    results.append(publication)
                except Exception as e:
                    logging.error(f"Medline processing error: {str(e)}")

        self._save_results(results)
        return results

    def researchgate_scraper(self, query, max_results=500):
        results = []
        url = "https://www.researchgate.net/search"
        params = {
            'q': query,
            'type': 'publication',
            'offset': 0,
            'limit': max_results
        }

        response = self.safe_request(url, params)
        if not response:
            return results

        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.find_all('div', class_='publication-item'):
            try:
                title = item.find('h2').text.strip()
                if is_duplicate(title):
                    continue

                publication = {
                    'title': title,
                    'authors': [author.text.strip() for author in item.find_all('span', class_='author')] if item.find_all('span', class_='author') else [],
                    'year': item.find('span', class_='year').text.strip() if item.find('span', class_='year') else '',
                    'journal': item.find('span', class_='journal').text.strip() if item.find('span', class_='journal') else '',
                    'link': item.find('a')['href'] if item.find('a') else '',
                    'source': 'ResearchGate'
                }
                results.append(publication)
            except Exception as e:
                logging.error(f"ResearchGate processing error: {str(e)}")

        self._save_results(results)
        return results

        def citeseerx_scraper(self, query, max_results=500):
            results = []
            url = "http://citeseerx.ist.psu.edu/search"
            params = {
                'q': query,
                'start': 0,
                'rows': max_results
            }

            response = self.safe_request(url, params)
            if not response:
                return results

            soup = BeautifulSoup(response.text, 'html.parser')
            for item in soup.find_all('div', class_='result'):
                try:
                    title = item.find('h3').text.strip()
                    if is_duplicate(title):
                        continue

                    publication = {
                        'title': title,
                        'authors': [author.text.strip() for author in item.find_all('span', class_='author')] if item.find_all('span', class_='author') else [],
                        'year': item.find('span', class_='year').text.strip() if item.find('span', class_='year') else '',
                        'journal': item.find('span', class_='journal').text.strip() if item.find('span', class_='journal') else '',
                        'link': item.find('a')['href'] if item.find('a') else '',
                        'source': 'CiteSeerx'
                    }
                    results.append(publication)
                except Exception as e:
                    logging.error(f"CiteSeerx processing error: {str(e)}")

            self._save_results(results)
            return results

    def _save_results(self, results):
        if not results:
            return
        try:
            for publication in results:
                collection.update_one(
                    {'title': publication['title']},
                    {'$set': publication},
                    upsert=True
                )
            logging.info(f"Inserted {len(results)} publications")
        except Exception as e:
            logging.error(f"MongoDB error: {str(e)}")

# ------------------- EXECUTION -------------------
if __name__ == "__main__":
    scraper = AcademicScraper()
    
    # Example usage
    try:
        results = []
        results += scraper.arxiv_scraper("Sultan Moulay Slimane University", 1000)
        results += scraper.openalex_scraper("Sultan Moulay Slimane University", "anas.battas@usms.ac.ma", 50)
        results += scraper.pubmed_scraper("Sultan Moulay Slimane University", 1000)
        results += scraper.google_scholar_scraper("Sultan Moulay Slimane University", 1009)
        results += scraper.springer_scraper("Sultan Moulay Slimane University", 1000)
        results += scraper.hal_scraper("Sultan Moulay Slimane University", 1000)
        results += scraper.medline_scraper("Sultan Moulay Slimane University", 1000)
        results += scraper.researchgate_scraper("Sultan Moulay Slimane University", 1000)
        results += scraper.citeseerx_scraper("Sultan Moulay Slimane University", 1000)
        results += scraper.scilit_scraper("Sultan Moulay Slimane University", 1000)
        
        logging.info(f"Total publications collected: {len(results)}")
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user")
    except Exception as e:
        logging.error(f"Critical error: {str(e)}")
