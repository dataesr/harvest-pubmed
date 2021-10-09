import gzip
import xml.etree.ElementTree as ET
import requests
from bs4 import BeautifulSoup
import io

def harvest_baseline():
    medline_url = 'https://ftp.ncbi.nlm.nih.gov/pubmed/baseline' #/updatefiles
    soup = BeautifulSoup(requests.get(medline_url).text, 'lxml')
    pubmed_files =[f'{medline_url}/{a.text}' for a in soup.find_all('a') if a.text.startswith('pubmed') and a.text.endswith('xml.gz')]

    for p in pubmed_files:
        s=requests.get(p).content
        input_content = gzip.open(io.BytesIO(s))
        tree = ET.parse(input_content)
        root = tree.getroot()
        for child in root:
            citation = child.find('MedlineCitation')
            if citation:
                new_pmid = citation.find('PMID').text
            elif child.tag == 'DeleteCitation':
                removed_pmids = [c.text for c in child.findall('PMID')]
            else:
                print("unexpected")
                print(ET.dump(child))
