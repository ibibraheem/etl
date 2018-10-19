import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from urllib.parse import urljoin


def scrape_html(args):
    urls = args["urls"]
    elements = args["elements"]
    parser = args["parser"]
    tables = []
    links = []
    for url in urls:
        resp = requests.get(url=url, stream=True)
        soup = BeautifulSoup(resp.text, parser)

        for element in elements:
            tag = element["tag"]
            attributes = element["attributes"]
            if tag == "table":
                for table in soup.findAll(tag, attrs=attributes):
                    df = pd.read_html(table.prettify())
                    tables.append(df)
            elif tag == "a":
                for link in soup.findAll(tag, attrs=attributes):
                    links.append(urljoin(url, link['href']))

    output = {'tables': [], 'links': links}

    for i, df in enumerate(tables):
        file_name = 'table_{}.csv'.format(i)
        with open(file_name, 'w') as f:
            df.to_csv(f, header=True)
        output['tables'].append(file_name)

    return output

scrape_html({'urls': ['https://www.sec.gov/dera/data/financial-statement-data-sets.html'],'elements': [{'tag': 'a','attributes': {'href': re.compile("\.zip$")}}], 'parser':'lxml'})