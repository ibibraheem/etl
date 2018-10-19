from flask_restful import Resource, reqparse
import tasks
from celery.result import AsyncResult


PIPELINES = {}

parser = reqparse.RequestParser()
parser.add_argument('pipeline')


class Pipeline(Resource):
    def get(self, _id):
        result = AsyncResult(id=_id, app=tasks.app)
        return result.status

    def post(self):
        args = parser.parse_args()
        print(args)
        pipeline = args['pipeline']
        # store pipeline in a job queue
        result = tasks.start_task.delay(pipeline)
        return result.status + " " + result.id
