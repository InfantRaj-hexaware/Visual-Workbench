import os
import time
import traceback
import logging.config
import logging
import werkzeug
import logging
import itertools
import codecs
from pathlib import Path
from flask_restful import Resource, Api, request, reqparse
from flask import Flask, jsonify,make_response
from flask_cors import CORS, cross_origin
from logging.handlers import TimedRotatingFileHandler
from app_model import get_ann_model

app = Flask(__name__)

app.config['SECRET_KEY'] = 'thisissecret'

base_path = os.path.abspath(os.path.dirname(__file__))
log_file_path = os.path.join(base_path,'vaas.log')
logs_storage = os.path.join(base_path,'logs','vaas.log')
logger = logging.getLogger(__name__)

api = Api(app)

CORS(app)
logger = logging.getLogger(__name__)

class SparkNLP(Resource):
    def __init__(self):
        pass
    # @app.route('/user', methods=['POST'])
    def post(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : ''}
            data = request.json
            userStory = data['userStory']
            #NLP starts
            if get_ann_model(userStory):
                output = get_ann_model(userStory)
                print("------------output-------------")
                print(output)
                #NLP Ends
                output["userStory"] = userStory  
                response['message'] = 'Success'
                response['success'] = True
                response['payload'] = output
                status = 200
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

api.add_resource(SparkNLP, '/sparkNLP')
@app.route('/test')

def test():
    return "API Working"

if __name__ == '__main__':  
    logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    logging.Formatter.converter = time.localtime
    # use very short interval for this example,typical 'when' would be 'midnight' and no explicit interval
    handler = logging.handlers.TimedRotatingFileHandler(logs_storage, when="midnight", interval=1, backupCount=10)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.info("Starting Flask")
    app.run(host='0.0.0.0', port = 8000,debug=True)