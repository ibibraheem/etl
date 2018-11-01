This project is essentially a POC to demonstrate the possiblity of building a api that functions as a wrapper to Pandas and BeautifulSoup (combined) in an effor to make writing ETL pipelines as simple as writing nested function calls such as `transform(decompress(download(scrape())))`. Each function takes a certain set of parameters.

Pre-requisites:
install the following (preferably in a vertual environment):
  * python 3
  * celery job queue
  * redis server(or any backend for celery to keep track of the tasks’ states)
  * RabbitMQ (or any message broker you prefer for celery)

Download the files to a directory and from that directory run:
```sh
$ python api.py
$ celery -A tasks worker --loglevel=info --concurrency=1
$ redis-server
```

Test the etl pipeline by executing:
```sh
$ curl -X POST http://localhost:5000/pipeline -H "Content-Type: application/json" -d @test-pipeline.json
```

The `output` directory should contain a transformed CSV file for the SEC's 2017 Q4 financial statement data set.
