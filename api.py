from flask import Flask
from flask_restful import Api
from pipeline import Pipeline


app = Flask(__name__)
api = Api(app)


routes = [
    '/pipeline',
    '/pipeline/<string:_id>',
]
api.add_resource(Pipeline, *routes)

if __name__ == '__main__':
    app.run(debug=True)