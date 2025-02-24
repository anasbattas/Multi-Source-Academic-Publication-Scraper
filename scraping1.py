import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import random
import spacy

# Configuration du modèle spaCy
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    print("Téléchargement du modèle spaCy...")
    spacy.cli.download("en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")

# Configuration MongoDB
client = MongoClient('localhost', 27017)
db = client['usms']
collection = db['test']

def extract_entities(text):
    """Extrait les entités nommées avec spaCy"""
    if not text:
        return {'auteurs': [], 'institutions': [], 'concepts': [], 'ecoles': []}
    doc = nlp(text)
    return {
        'auteurs': [ent.text for ent in doc.ents if ent.label_ == 'PERSON'],
        'institutions': [ent.text for ent in doc.ents if ent.label_ == 'ORG'],
        'concepts': [ent.text for ent in doc.ents if ent.label_ in ['NORP', 'LOC', 'PRODUCT']],
        'ecoles': [ent.text for ent in doc.ents if ent.text.lower() in ['école', 'collège']]
    }

def openalex_extractor(query, email, max_results=500):
    url = "https://api.openalex.org/works"
    params = {
        'filter': f'title.search:{query}',
        'mailto': email,
        'per_page': 200
    }
    results = []
    
    try:
        for page in range(1, (max_results // 200) + 2):
            params['page'] = page
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()  # Check for HTTP errors
            data = response.json()
            
            for work in data.get('results', []):
                try:
                    authorships = work.get('authorships', [])
                    authors = [a.get('author', {}).get('display_name', 'Auteur inconnu') for a in authorships]
                    primary_location = work.get('primary_location', {})
                    source = primary_location.get('source', {})
                    
                    entry = {
                        'title': work.get('title', 'Titre inconnu'),
                        'authors': authors,
                        'year': int(work.get('publication_date', '0000')[:4]) if work.get('publication_date') else None,
                        'journal': source.get('display_name', 'Journal inconnu'),
                        'abstract': work.get('abstract', ''),
                        'link': work.get('doi', ''),
                        'keywords': [kw.get('display_name', '') for kw in work.get('keywords', [])],
                        'entities': extract_entities(work.get('abstract', ''))
                    }
                    results.append(entry)
                except Exception as e:
                    print(f"Erreur traitement entrée OpenAlex: {e}")
            
            if len(results) >= max_results:
                break
        
        if results:
            collection.insert_many(results)
            print(f"{len(results)} publications OpenAlex insérées.")
            
    except Exception as e:
        print(f"Erreur OpenAlex: {str(e)}")
    
    return results

def pubmed_scraper(query, max_results=500):
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    results = []
    
    try:
        # Recherche
        esearch = f"{base_url}esearch.fcgi?db=pubmed&term={query}&retmax={max_results}"
        response = requests.get(esearch, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml-xml')
        ids = [id.text for id in soup.find_all('Id')]
        
        # Récupération des détails
        efetch = f"{base_url}efetch.fcgi?db=pubmed&id={','.join(ids)}&retmode=xml"
        details_response = requests.get(efetch, timeout=10)
        details_response.raise_for_status()
        details_soup = BeautifulSoup(details_response.text, 'lxml-xml')
        
        for article in details_soup.find_all('PubmedArticle'):
            try:
                title = article.find('ArticleTitle').text if article.find('ArticleTitle') else 'Sans titre'
                
                authors = []
                for auth in article.find_all('Author'):
                    last_name = auth.find('LastName').text if auth.find('LastName') else ''
                    fore_name = auth.find('ForeName').text if auth.find('ForeName') else ''
                    authors.append(f"{last_name} {fore_name}".strip())
                
                pub_date = article.find('PubDate')
                year = pub_date.Year.text if pub_date and pub_date.Year else None
                journal = article.find('Journal').Title.text if article.find('Journal') else ''
                
                abstract = ' '.join([t.text for t in article.find_all('AbstractText')])
                pmid = article.find('PMID').text if article.find('PMID') else ''
                
                results.append({
                    'title': title,
                    'authors': authors,
                    'year': int(year) if year else None,
                    'journal': journal,
                    'abstract': abstract,
                    'link': f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else '',
                    'keywords': [kw.text for kw in article.find_all('Keyword')],
                    'entities': extract_entities(abstract)
                })
            except Exception as e:
                print(f"Erreur traitement article PubMed: {e}")
        
        if results:
            collection.insert_many(results)
            print(f"{len(results)} publications PubMed insérées.")
            
    except Exception as e:
        print(f"Erreur PubMed: {str(e)}")
    
    return results

def arxiv_scraper(query, max_results=500):
    url = "http://export.arxiv.org/api/query"
    params = {'search_query': query, 'max_results': max_results}
    results = []
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml-xml')
        
        for entry in soup.find_all('entry'):
            try:
                title = entry.title.text.strip() if entry.title else 'Sans titre'
                authors = [a.find('name').text for a in entry.find_all('author') if a.find('name')]
                published = entry.published.text[:4] if entry.published else None
                abstract = entry.summary.text.strip() if entry.summary else ''
                
                results.append({
                    'title': title,
                    'authors': authors,
                    'year': int(published) if published else None,
                    'journal': 'ArXiv',
                    'abstract': abstract,
                    'link': entry.id.text if entry.id else '',
                    'keywords': [cat['term'] for cat in entry.find_all('category') if cat.has_attr('term')],
                    'entities': extract_entities(abstract)
                })
            except Exception as e:
                print(f"Erreur traitement entrée ArXiv: {e}")
        
        if results:
            collection.insert_many(results)
            print(f"{len(results)} publications ArXiv insérées.")
            
    except Exception as e:
        print(f"Erreur ArXiv: {str(e)}")
    
    return results

def ieee_scraper(query, max_results=500):
    """Placeholder for IEEE scraping without API key"""
    print("IEEE Xplore scraping requires an API key. Skipping...")
    return []

if __name__ == "__main__":
    query = "Sultan Moulay Slimane University"
    email = "anas.battas@usms.ac.ma"
    
    try:
        # Delay the script for more than 5 minutes
        print("Démarrage de l'extraction...")
        time.sleep(310)  # 5 minutes and 10 seconds delay

        # Extract data from sources
        openalex_results = openalex_extractor(query, email, 500)
        pubmed_results = pubmed_scraper(query, 500)
        arxiv_results = arxiv_scraper(query, 500)
        ieee_results = ieee_scraper(query, 500)
        
        total = sum([len(openalex_results), len(pubmed_results), len(arxiv_results), len(ieee_results)])
        print(f"Total publications stockées avec succès : {total}")
        
        # Affichage d'un exemple
        sample = collection.find_one()
        if sample:
            print("\nExemple de document stocké :")
            for key, value in sample.items():
                print(f"{key}: {value[:80] + '...' if isinstance(value, str) else value}")
                
    except KeyboardInterrupt:
        print("\nOpération interrompue par l'utilisateur")
    except Exception as e:
        print(f"Erreur globale: {str(e)}")
