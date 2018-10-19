import requests
from celery import Celery
import re
from bs4 import BeautifulSoup
import pandas as pd
from urllib import parse
from urllib.parse import urljoin
from zipfile import ZipFile
import os


app = Celery('tasks', broker='pyamqp://', backend='redis://')
app.Task.track_started = True


@app.task(bind=True)
def start_task(self, pipeline):
    pipeline = inject_self(pipeline)
    return eval(pipeline)


@app.task
def scrape_task(self, args):
    self.update_state(state='SCRAPING')
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

    output = {'tables': [], 'urls': links}

    for i, df in enumerate(tables):
        file_name = 'table_{}.csv'.format(i)
        with open(file_name, 'w') as f:
            df.to_csv(f, header=True)
        output['tables'].append(file_name)

    return output


@app.task
def download_task(self, args):
    urls = args["urls"]
    files = []
    num_urls = len(urls)
    for i, url in enumerate(urls):
        resp = requests.get(url=url, stream=True)
        self.update_state(state='DOWNLOADING FILE {} OF {}'.format(i+1, num_urls))
        if resp.status_code == 200:
            local_filename = os.path.basename(parse.urlparse(url).path)
            with open(local_filename, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            files.append(local_filename)
    return {"files": files}


@app.task
def decompress_task(self, args, members, pwd=None):
    self.update_state(state='Extracting files')
    zip_dirs = args["files"]
    output = {"data_dirs": []}
    for zip_dir in zip_dirs:
        with ZipFile(zip_dir, 'r') as zip_files:
            for member in members:
                extracted_dir_path = "./decompressed_{}".format(zip_dir)
                zip_files.extract(member=member, path=extracted_dir_path, pwd=pwd)
            output["data_dirs"].append(extracted_dir_path)
    return output


@app.task
def transform_task(self, args, metadata):
    self.update_state(state='Transforming files')
    data_dirs = args["data_dirs"]
    for data_dir in data_dirs:
        for element in os.scandir(data_dir):
            if element.is_file():
                to_csv(self, element, metadata[element.name])


def to_csv(self, file, metadata):
    self.update_state(state='Parsing {}'.format(file.path))
    in_columns= metadata['in_columns']
    out_columns = metadata['out_columns']
    delimiter = metadata['delimiter']
    header_row = metadata['header_row']
    datetime_fields = metadata['datetime_fields']
    boolean_fields = metadata['boolean_fields']
    dtypes = metadata['dtypes']

    chunks = pd.read_csv(file.path, sep=delimiter, header=header_row,
                         names=in_columns, parse_dates=datetime_fields,
                         infer_datetime_format=True, chunksize=1024)
    os.makedirs(os.path.join('./output'), exist_ok=True)
    for chunk in chunks:
        chunk.to_csv('output/'+file.name, mode='a', columns=out_columns, index=False)



@app.task
def load_task(file):
    return "loading {}".format(file)


def inject_self(pipeline):
    return re.sub(r'_task\(', '_task(self,', pipeline)
