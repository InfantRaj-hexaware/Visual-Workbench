from cmath import nan
from numpy import random
from numpy.lib.arraysetops import unique
# from sqlalchemy import null
import pymongo
from flask import Flask, jsonify,make_response
from flask_restful import Resource, Api, request, reqparse
import json
import traceback
import re 
import math
import base64
from bson.objectid import ObjectId
import data_generator
import jwt
from datetime import datetime
import nltk
import os
import time
from logging.handlers import TimedRotatingFileHandler
import logging.config
import logging
import copy
import pandas as pd
import textract
import requests
import pprint
import string
import gridfs
import werkzeug
import operator
import ast
# import suggestions
import drill_down
import sys
import numpy as np
from countryinfo import CountryInfo
from math import isnan
from collections import defaultdict, OrderedDict
if 'tokenize' not in dir(nltk):
    nltk.download('punkt')
from nltk.tokenize import sent_tokenize
from flask_cors import CORS, cross_origin
from entity_extractor import ParserUserStory,TrainMLModel
from chart_generator import Charts, ChartViewer
from drill_down import DrillDown
from general_func import GeneralFunc
from sklearn.model_selection import train_test_split
from constants import RECORDING_FILE_PATH, RECORDING_OUTPUT_FILE_NAME
#second instance imports starts
import spacy
#for 44 server
# NER_dir='/home/ubuntu/Vaas_BI/Backend/WB_Deployment'
#for 172 server
# NER_dir='/root/vaas/backend/WB_Deployment'
#for New 172 server
# NER_dir='/home/vaas_bi/backend/WB_Deployment'
#for local
NER_dir = 'D:\\CTO\Vaas_BI\\backend\\WB_Deployment'
# if server enable NER_dir path has this
# NER_dir = '/home/ec2-user/Vaas_BI_2/backend/WB_Deployment'
nlp=spacy.load(NER_dir)
#second instance imports ends

    
base_path = os.path.abspath(os.path.dirname(__file__))
log_file_path = os.path.join(base_path,'vaas.log')
logs_storage = os.path.join(base_path,'logs','vaas.log')
# logger = logging.getLogger()
import logging
from pathlib import Path
import itertools
import codecs
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["local_mongo"]

app = Flask(__name__)
CORS(app)

app.config['SECRET_KEY'] = 'thisissecret'
base_path = os.path.abspath(os.path.dirname(__file__))
app.config['UPLOAD_FOLDER'] = os.path.join(base_path,"Uploads")
# app.config['MAX_CONTENT_PATH'] = 
api = Api(app)

dimension = []
variable_logos = {"Revenue" : "RevenueLogo", "Expense" : "ExpenseLogo", "Profit" : "ProfitLogo"}
possibleChartList = ['numberTile', 'kpiTile', 'pieChart', 'treeMap', 'lineChart', 'multiLineChart', 'barChart', 'areaChart', 'scatterChart', 'stackedBarChart', 'waterflowChart', 'twoColumnStackedBarChart']

class Users(Resource):
    def __init__(self):
        pass
    # @app.route('/user', methods=['POST'])
    def post(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : ''}
            mycol = mydb["users"]
            users = request.json
            print("==============post ===============")
            print("mycol   :   ",mycol)
            print("user  :  ",users)
            if(list(mycol.find({"email":f"{users['email']}"}))):
                response['message'] = 'User Already Exist'
                status = 400
            else:
                encoded_password = base64.b64encode(users['password'].encode())
                users['password'] = encoded_password
                mycol.insert_one(users)
                response['message'] = 'User Added Successfully'
                response['success'] = True
                response['payload'] = 'User Added Successfully'
                status = 200
    # def post(self):
    #     try:
    #         response = {'success' : False, 'message' : 'Failure', "payload" : ''}
    #         mycol = mydb["users"]
    #         users = request.form
    #         email = users.get('email', None)
    #         name = users.get('name', None)
    #         password = users.get('password', None)
    #         logo = request.files.get('logo', [])
    #         if logo:
    #             logo.save(os.path.join(app.config['UPLOAD_FOLDER'],*["Logo",logo.filename]))
    #         users = dict(users)
    #         print("================users=================")
    #         print(users)
    #         if(list(mycol.find({"email":f"{users['email']}"}))):
    #             response['message'] = 'UserÂ AlreadyÂ Exist'
    #             status = 400
    #         else:
    #             encoded_password = base64.b64encode(users['password'].encode())
    #             users['password'] = encoded_password
    #             if logo:
    #                 logo.save(os.path.join(app.config['UPLOAD_FOLDER'],*["Logo",logo.filename]))
    #             users["logo"] = os.path.abspath(logo.filename)
    #             mycol.insert_one(users)
    #             response['message'] = 'User Added Successfully'
    #             response['success'] = True
    #             response['payload'] = 'User Added Successfully'
    #             status = 200
        
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))
        
    # @app.route('/user', methods=['DELETE'])
    def delete(self):
        try:
            mycol = mydb["users"]
            email = request.args.get('email')
            if(list(mycol.find({"email":f"{email}"}))):
                myquery = { "email": f"{email}" }
                mycol.delete_one(myquery)
                res = jsonify({'result' : " user has been deleted"})
                status = 200
            else:
                res = jsonify({'result' : "this  user doesn't exists"})
                status = 200
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            res = jsonify({'result' : "not updated"})
            status = 400
        finally:
            return(make_response(res, status))

    # @app.route('/view_user')
    def get(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success = 400, False
            mycol = mydb["users"]
            output=[]
            user = request.json
            for q in mycol.find({"email":f"{user['email']}"}):
                if 'logo' in q:
                    output.append({'name' : q['name'], 'email' : q['email'], 'logo' : q['logo']})
                else:
                    logo =Path('/home/ec2-user/Vaas_BI/backend/Uploads/Logo/sample_img.jpg')
                    logo = str(logo)
                    output.append({'name' : q['name'], 'email' : q['email'], 'logo' : logo})
            status = 200
            res={'data' : output}
            response['payload'] = res
            response['message'] = 'success'
            response['success'] = True
            
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(jsonify(response), status))
    
    def put(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : ''}
            mycol = mydb["users"]
            user_update = request.form
            email = user_update.get('email', None)
            name = user_update.get('name', None)
            password = user_update.get('password', None)
            user_data = mycol.find_one({"email":f"{user_update['email']}"})
            logo = request.files.get('logo', [])
            
            if not user_data:
                response['message'] = 'User Does Not exist'
                status = 400
            else:
                if logo:
                    logo.save(os.path.join(app.config['UPLOAD_FOLDER'],*["Logo",logo.filename]))
                user_update = dict(user_update)
                user_update["logo"] = os.path.abspath(logo.filename)
                if user_update.get('password', []):
                    encoded_password = base64.b64encode(user_update['password'].encode())
                    user_update['password'] = encoded_password
                mycol.update_one({'email' : user_update['email']}, {"$set" : user_update})
                response['message'] = 'User Data Updated Successfully'
                response['success'] = True
                response['payload'] = 'User Data Updated Successfully'
                status = 200
        
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))



class SellectAll(Resource):
    def get_id(self, table : str,param_name : str, _id : list)->list:
        '''
        This return all the id if we pass table name, param name and Id
        '''
        mycol = mydb[table]
        myquery = {param_name :{'$in':_id}}
        all_id = [] 
        for i in mycol.find(myquery):
            all_id.append(i[param_name])
        
        return all_id
    # @app.route('/deleteSelected ',methods=['DELETE'])
    def delete(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            mycol = mydb["workspace"]
            widget_db = mydb['widget']
            widget_col = mydb['widget']
            data_col = mydb['data']
            req_json = request.json
            workspaceIds = req_json['workspaceIds']
            for i in workspaceIds:
                if(list(mycol.find({"_id":ObjectId(f"{i}")}))):
                    all_id = self.get_id("widget", "workspaceId", [i])
                    data_col.delete_many({'workspaceId' : i})    
                    widget_col.delete_many({'workspaceId' : i})   
                    myquery = {"_id":ObjectId(f"{i}")}
                    mycol.delete_one(myquery)     
                    message = 'workspace has been deleted'
                    is_success = True
                    status = 200
                else:
                    message = "workspace doesn't exists"
                    status = 400
                response['message'] = message
                response['success'] = is_success
                response['payload'] = message
                
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

# @app.route('/login',methods=['POST'])
class Login(Resource):
    def __init__(self):
        pass
    def post(self):
        try:
            status = 400
            mycol = mydb["users"]
            req_json = request.json
            email = req_json['email']
            password =req_json['password']
            response = {'success' : False, 'message' : 'Failure', "payload" : ''}
            payload = {'isLoggedIn' : False, 'name' : ''}
            user_found = mycol.find_one({"email":email})
            decoded_password = base64.b64decode(user_found.pop('password')).decode('utf-8')
            user_found.pop('_id')
            if not user_found:
                status = 404
                message = 'User Not Found'
                payload = "User Not Found"
            else:
                user_name =  user_found.get('name',None)
                if 'logo' in user_found:
                    user_found = user_found
                else:
                    #reading the image
                    #method 2 starts
                    datafile = open("sample_img.jpg", "rb")
                    thedata = datafile.read()
                    imgdatabase = myclient['image']
                    fs = gridfs.GridFS(imgdatabase)
                    stored = fs.put(thedata, filename="sample_img.jpg")
                    outputdata =fs.get(stored).read()
                    #for local path
                    # outfilename = Path("E:\CTO\Vaas_BI\backend")
                    #for server path
                    outfilename = Path("/home/ec2-user/Vaas_BI/backend")
                    output= open("decode_img","wb")
                    output.write(outputdata)
                    #method 2 ends
                    #method 3 starts
                    with open("decode_img", "rb") as image_file:
                        encoded_string = base64.b64encode(image_file.read())
                    #method 3 ends
                    logo =Path('/home/ec2-user/Vaas_BI/backend/Uploads/Logo/sample_img.jpg')
                    logo = str(logo)
                    user_found['logo'] = str(outfilename)
                    user_found['logo_details'] = ({"stored_id": str(stored),"output": str(output), "outputdata_binary" : str(outputdata), "base64" : str(encoded_string) })
                    print(user_found)
                if 'confirmpassword' in user_found:
                    del user_found['confirmpassword']
                isLoggedIn = False if not user_name else True
                success = False if not user_name else True
                message = 'Failure' if not user_name else 'Success'
                
                if password != decoded_password:
                    message = 'Incorrect Password'
                    payload = 'Incorrect Password'
                    status = 400
                else:
                    payload = user_found
                    payload['isLoggedIn'] = isLoggedIn
                    status = 200
            response['payload'] = payload
            response['success']  = success
            response['message'] = message         
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

#this is used to upload Logo
class ClientLogoUploader(Resource):
     # @app.route('/clientLogoUploader', methods=['POST'])
     def post(self):
         try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            input_ = request.json
            logodb = mydb["logo"]
            workspace_id = input_.get("workspaceId", None)
            logo = input_.get("logo", None)
            print("------workspace_id------")
            print(workspace_id)
            data = {}
            output = []
            logodata = logodb.find_one({"workspaceId" : workspace_id})
            if logodata:
                data = {"workspaceId" : workspace_id, "logo" :   (logo) }
                # logodb.insert_one(data)
                logodb.update_many({'workspaceId' : workspace_id}, {"$set" : data})
            else:
                data = {"workspaceId" : workspace_id, "logo" :   (logo) }
                logodb.insert_one(data)
                # logodb.update_many({'workspaceId' : workspace_id}, {"$set" : data})
            message = "Client Logo Uploaded Successfully"
            status = 200
            is_success = True
            output =  {"workspaceId" : workspace_id, "logo" :   (logo) }
            response['message'] = message
            response['success'] = True
            response['payload'] = output
         except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
         finally:
            return(make_response(response, status))
     def put(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            logodb = mydb["logo"]
            input_ = request.json
            workspace_id = input_.get("workspaceId", None)
            logo = input_.get("logo", None)
            data = {"workspaceId" : workspace_id, "logo" :  logo}
            logodb.update_many({'workspaceId' : workspace_id}, {"$set" : data})
            message = "Client Logo Uploaded Successfully"
            status = 200
            is_success = True
            response['message'] = message
            response['success'] = True
            response['payload'] = data
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

     def get(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            logodb = mydb["logo"]
            output = []
            workspace_id = str(request.args.get('workspaceId'))

            logodata = logodb.find_one({"workspaceId" : workspace_id})
            if logodata:
                # print("========logodata========")
                # print(logodata)
                logodata = logodata
            else:
                with open('Hexaware.jpg', 'rb') as f:
                    im_b64 = base64.b64encode(f.read())
                # print("------------------im_b64--------------------")
                # print(type(im_b64))
                ENCODING = 'utf-8'
                decoded = im_b64.decode(ENCODING)
                # print("======================")
                # print(decoded)
                data = {"workspaceId" : workspace_id, "logo" :   ("data:image/jpeg;base64,"+decoded)}

                logodb.insert_one(data)
                logodata = data
                # print("========logodata===========")
                # print(logodata["logo"])
            message = "Success"
            status = 200
            is_success = True
            logodata = { "logo": logodata["logo"] }
            response['message'] = message
            response['success'] = True
            response['payload'] = logodata
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

class Workspaces(Resource):
    # @app.route('/workspaces', methods=['POST'])
    def post(self):
        try:
            print("======workspace post is called=====")
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            chart_layout_dict, chart_layout_array = {"i": "", "x": 0, "y": 0, "w": 4, "h": 10, "minW": 4, "minH": 9, "maxH": 10}, []
            layout_x = [0,4,8]
            mycol = mydb["workspace"]
            data_db = mydb['data']
            logodb = mydb["logo"]
            widget_db = mydb['widget']
            user_db = mydb['users']
            ploty_workspace_db = mydb["ploty_workspace_db"]
            workspace = request.json
            workspace_name = workspace.get('workspaceName', None)
            no_of_widgets = workspace.get('noOfWidgets',1)
            workspace_type = workspace.get('workspaceType', None)
            output = []
            ##Multiple Dashboard starts
            subDashboard = workspace.get('subDashboard', False)
            # def str2bool(v):
            #     return v.lower() in ("yes", "true", "t", "1", "True")
            # subDashboard = str2bool(subDashboard)
            inputSubDash = workspace.get('inputSubDash', {})
            ##Multiple Dashboard ends
            try:
                no_of_widgets = int(no_of_widgets)
            except:
                no_of_widgets = 1
            email = workspace.get('email', None)
            theme = workspace.get('theme', None)
            currency = workspace.get('currencies', user_db.find_one({"email": email}).get('currencies', "$"))
            # theme = json.loads(theme)
            user_story = workspace.get('userStory', None)
            upload_file = request.files.get('file', [])
            print("======workspace post is called 2=====")
            if upload_file:
                upload_file.save(os.path.join(app.config['UPLOAD_FOLDER'],*["UserStories",upload_file.filename]))
                user_story = textract.process(os.path.join(app.config['UPLOAD_FOLDER'],*["UserStories",upload_file.filename])).decode("utf-8")
            workspace_exists = mycol.find_one({"email":email, "workspaceName" : workspace_name})
            if workspace_exists:
                message = 'Workspace Already Exists'
            else:
                if user_story:
                    print("====user_story / widget post else======")
                    parse_story = ParserUserStory()
                    widget_payload , user_story, consolidatedData = parse_story.text_to_structure(user_story, currency)
                    no_of_widgets = len(user_story)
                    user_story_str = " ".join(user_story)
                    workspace_id = mycol.insert_one({"email":email, "workspaceName" : workspace_name.capitalize(), "userStory" : user_story_str,
                                "noOfWidgets": no_of_widgets, "workspaceType" : workspace_type, "currencies": currency, "theme": theme, "subDashboard" : subDashboard, "inputSubDash" : inputSubDash})
                    workspace_id = str(workspace_id.inserted_id)
                    output = [{"workspaceId" : workspace_id,"email":email, "workspaceName" : workspace_name.capitalize(), "userStory" : user_story_str,
                                "noOfWidgets": no_of_widgets, "workspaceType" : workspace_type, "currencies": currency, "theme": theme, "subDashboard" : subDashboard, "inputSubDash" : inputSubDash}]
                    if widget_payload:
                        print("===widget_payload / if =====")
                        for i in range(0,no_of_widgets):
                            layout = chart_layout_dict.copy()
                            layout["i"] = list(string.ascii_lowercase)[i]
                            layout["x"] = layout_x[i%3]
                            if widget_payload[i]["recommendedCharts"]:
                                if widget_payload[i]["recommendedCharts"][0]['chartType'] in ["numberTile","kpiTile"]:
                                    layout["h"] = 5
                                    layout["minH"] = 5
                            chart_layout_array.append(layout)
                            widget_name = f'Widget {i+1}'
                            temp_user_story = user_story[i] if user_story else "none"
                            # print('-------------final output---------------')
                            # pprint.pprint({'workspaceId' : workspace_id, 'widgetName' : widget_name.capitalize(),
                            #             'userStory' : temp_user_story.capitalize(), 
                            #             'chartData' : widget_payload[i], 
                            #             'currencies':currency})
                            widget_id = widget_db.insert_one({'workspaceId' : workspace_id, 'widgetName' : widget_name.capitalize(),
                                        'userStory' : temp_user_story.capitalize(),
                                        'chartData' : widget_payload[i], 
                                        'currencies':currency,
                                        "subDashboard" : subDashboard,
                                        "inputSubDash" : inputSubDash})

                           
                    ####Plotly starts
                    '''
                    Creates a new Database in plotly called "ploty_workspace_db" this will have all the values from workspace database 
                    and also it will have chart data, parameter data and single data set - i.e consolidated data.  
                    '''
                    #parameter data
                    widget_data = widget_db.find({'workspaceId' : workspace_id})
                    temp2 = []
                    for w_data in widget_data:
                        temp1 = {}
                        # temp1['userStory'] = w_data.get('userStory', None)
                        # temp1['widgetName'] = w_data.get('widgetName',None)
                        temp1['widgetId'] = str(w_data.get('_id',None))
                        temp2.append(temp1)
                    parameterData = []
                    if widget_payload :
                        for wp in widget_payload:
                            temp = wp['chartDimension']
                            temp['Statement'] = wp['userStory']
                            temp['Suggested_Chart'] = wp['recommendedCharts'][0]['chartType']
                            parameterData.append(temp)
                    for i in range(len(parameterData)):
                        parameterData[i]['widgetId'] = temp2[i]['widgetId']
                        parameterData[i]['Categorical_Dimension'] = parameterData[i].pop('Categorical Dimension')
                        parameterData[i]['Goal_Measure'] = parameterData[i].pop('Goal Measure')
                        parameterData[i]['Timeline_Dimension'] = parameterData[i].pop('Timeline Dimension')

                        parameterData[i]['Categorical_Dimension'] = ', '.join([str(i) for i in parameterData[i]['Categorical_Dimension']])
                        parameterData[i]['Goal_Measure'] = ', '.join([str(i) for i in parameterData[i]['Categorical_Dimension']])
                        parameterData[i]['Measure'] = ', '.join([str(i) for i in parameterData[i]['Measure']])
                    ploty_workspace_db.insert_one({"email":email, "workspaceName" : workspace_name.capitalize(), "userStory" : user_story_str,
                        "noOfWidgets": no_of_widgets, "workspaceType" : workspace_type, "currencies": currency, "theme": theme, "subDashboard" : subDashboard, "inputSubDash" : inputSubDash,
                        "chartData" : widget_payload, "singleDataSet" : consolidatedData, "parameterData" : parameterData})
                    ####Plotly ends
                    data_db.insert_one({'workspaceId' : workspace_id, 'data' : consolidatedData, "layout": chart_layout_array,'dataDistribution' : {}})
                    
                message = 'Workspace Created Successfully'
                status = 200
                is_success = True
            response['message'] = message
            response['success'] = is_success
            response['payload'] = output
            
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))



    def get_id(self, table : str,param_name : str, _id : list)->list:
        '''
        This return all the id if we pass table name, param name and Id
        '''
        mycol = mydb[table]
        print("====workspace get id======")
        myquery = {param_name :{'$in':_id}}
        all_id = [] 
        for i in mycol.find(myquery):
            all_id.append(i[param_name])
        
        return all_id

    # @app.route('/workspace', methods=['DELETE'])
    def delete(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            mycol = mydb["workspace"]
            widget_col = mydb['widget']
            data_col = mydb['data']
            _id = request.args.get('workspaceId')
            if(list(mycol.find({"_id":ObjectId(f"{_id}")}))):
                all_id = self.get_id("widget", "workspaceId", [_id])
                data_col.delete_many({'workspaceId' : _id})    
                widget_col.delete_many({'workspaceId' : _id})   
                myquery = {"_id":ObjectId(f"{_id}")}
                mycol.delete_one(myquery)     
                message = 'workspace has been deleted'
                is_success = True
                status = 200
            else:
                message = "workspace doesn't exists"
                status = 400
            response['message'] = message
            response['success'] = is_success
            response['payload'] = message
                
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))
        

    # @app.route('/workspaces', methods=['GET'])
    def get(self):
        try:
            print("====workspace get ======")
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            output=[]
            mycol = mydb["workspace"]
            for q in mycol.find({'email' :request.args.get('email')}):
                q["_id"] = str(q["_id"])
                q['workspaceId'] = str(q["_id"])
                output.append(q)
            output.reverse()
            #Multiple workspace starts
            #NOTE : 'output' contains all the mainDashboard and subDashboard Datas.
            newOutput = {}
            newOutput['subDashboard'] = []
            newOutput['mainDashboard'] = []
            subDashData = []
            mainDashData = []
            for i in output:
                mainDashData = output.copy()
                if "subDashboard" in i:
                    # if i['subDashboard'] == True:
                    #     pass
                        # print("======subDashboard - Deatils======")
                        # pprint.pprint(i["inputSubDash"])
                        # for j in i['subDashName']:
                        #     for z in mainDashData:
                        #         if j == z['workspaceName']  :
                        #             print("====z====")
                        #             pprint.pprint(z)
                                    # i['subDashData'].append(z)
                                    # subDashData = {i['workspaceName'] : }
                                    # newOutput['subDashboard'] = 
                    if i["subDashboard"] == False:
                        newOutput['mainDashboard'].append(i)
            #Multiple workspace ends
            response['message'] = 'success'
            response['success'] = True
            response['payload'] = newOutput['mainDashboard']
            status = 200
            
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

    def put(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            chart_layout_dict, chart_layout_array = {"i": "", "x": 0, "y": 0, "w": 4, "h": 10, "minW": 4, "minH": 9, "maxH": 10}, []
            layout_x = [0,4,8]
            mycol = mydb["workspace"]
            data_db = mydb['data']
            widget_db = mydb['widget']
            user_db = mydb['users']
            workspace = request.json
            _id = workspace.get('workspaceId', None)
            newUserStory = workspace.get('newUserStory', None)
            workspace_name = workspace.get('workspaceName', None)
            workspace_type = workspace.get('workspaceType', None)
            user_story = workspace.get('userStory', None)
            # currency = workspace.get('currencies', '$')
            no_of_widgets = workspace.get('noOfWidgets',1)
            workspace_data = mycol.find_one({"_id":ObjectId(f"{_id}")})
            print("==========workspace_data / put ==================")
            pprint.pprint(workspace_data)
            if not workspace_data:
                message = "Workspace does not exist"
            else:
                currency = workspace.get('currencies', user_db.find_one({"email": workspace_data["email"]}).get('currencies', "$"))
                value = { k : workspace_data[k] for k in set(workspace_data) - set(workspace) }
                del(value["_id"])
                if value:
                    workspace.update(value)
                if workspace['workspaceId']:
                    del(workspace["workspaceId"])
                mycol.replace_one({
                    '_id': ObjectId(f"{_id}")
                        },workspace, upsert = True)
                if not user_story:
                    user_story = workspace_data['userStory']
                data_db.delete_many({"workspaceId" : _id})
                widget_db.delete_many({"workspaceId" : _id})
                parse_story = ParserUserStory()
                if newUserStory is not None:
                    to_replace = ["--", "-", ",", "---"]
                    for i in to_replace:
                       newUserStory = newUserStory.replace(i, " ")
                    newUserStory = newUserStory.split()
                    for i in range(len(newUserStory)):
                        if newUserStory[i] == 'Y':
                            newUserStory[i] = "Year"
                        if newUserStory[i] == "M":
                            newUserStory[i] = "Month"
                        if newUserStory[i] == "D":
                            newUserStory[i] = "Day"
                    newUserStory= " ".join(i for i in newUserStory)
                    user_story = user_story
                    print("======newUserStory======")
                    print(newUserStory)
                widget_payload , user_story, consolidatedData = parse_story.text_to_structure(user_story, currency)
                no_of_widgets = len(user_story)
                if widget_payload:
                    for i in range(0,no_of_widgets):
                        layout = chart_layout_dict.copy()
                        layout["i"] = list(string.ascii_lowercase)[i]
                        layout["x"] = layout_x[i%3]
                        if widget_payload[i]["recommendedCharts"]:
                            if widget_payload[i]["recommendedCharts"][0]['chartType'] in ["numberTile","kpiTile"]:
                                layout["h"] = 5
                                layout["minH"] = 5
                        chart_layout_array.append(layout)
                        widget_name = f'Widget {i+1}'
                        temp_user_story = user_story[i] if user_story else ""
                        widget_id = widget_db.insert_one({'workspaceId' : _id, 'widgetName' : widget_name.capitalize(),
                                    'userStory' : temp_user_story.capitalize(), 'chartData' : widget_payload[i], 'currencies': currency})
                data_db.insert_one({'workspaceId' : _id, 'data' : consolidatedData, "layout": chart_layout_array})
                message = "Updated Successfully / workspace put"
                    
            status, is_success = 200, True
            response['message'] = message
            response['success'] = is_success
            response['payload'] = message
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))


class Widget(Resource):
    # @app.route('/widget', methods=['POST'])
    def post(self):
        try:
            print("================widget post starts===================")
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            chart_layout_dict, chart_layout_array = {"i": "", "x": 0, "y": 0, "w": 4, "h": 10, "minW": 4, "minH": 9, "maxH": 10}, []
            layout_x = [0,4,8]
            widget = request.json
            mycol = mydb["widget"]
            data_db = mydb["data"]
            workspace_db = mydb["workspace"]
            input_ = request.json
            # user_story = input_.get('userStory',None)
            chart_data = input_
            workspace_id = chart_data.pop('workspaceId')
            all_data = data_db.find_one({"workspaceId" :  workspace_id})
            workspace_data = workspace_db.find_one({"_id":ObjectId(f"{workspace_id}")})
            single_chart_details = all_data['data']['dataDetails']
            dataSet ={}
            combined_dataSet = all_data['data']['dataSet']
            user_currency = workspace_data.get('currencies', "$")
            combined_dataSet["userCurrency"] = user_currency
            variables_list = single_chart_details.get('variables', [])
            isCurrencyList = []
            if variables_list:
                isCurrencyList = [variable['variableName'] for variable in variables_list if variable["isCurrency"]]
            combined_dataSet["currencyColumns"] = isCurrencyList
            widget_name = chart_data.pop('widgetName')
            widget_name = widget_name.capitalize()
            currency = workspace_db.find_one({"_id": ObjectId(f"{workspace_id}")}).get('currencies', '$')
            chart_dict = {}
            if(list(mycol.find({"workspaceId" : workspace_id, "widgetName" : widget_name}))):
                message = "widget already exits"
                success = False
                status = 400
            else:
                final_chart_out = []
                chart_viewer = ChartViewer()
                data_db = mydb['data']
                single_dataSet = data_db.find_one({"workspaceId" : workspace_id})
                if not single_dataSet:
                    status = 400
                    message = "No data set found"
                    success = False
                    response['message'] = message
                    response['success'] = success
                    response['payload'] = message
                    return(make_response(response, status))
                chart_layout_array = single_dataSet.get('layout', [])
                single_dataSet = single_dataSet['data']['dataSet']
                dimension = []
                aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(chart_data, single_dataSet, dimension)
                chart_data['aggregatedDataset'] = aggregated_dataset
                chart_data['aggregationType'] = aggregated_function
                chart_data['userStory'] = widget_name
                chart_data['currencies'] = currency
                chart_data['chartTitle'] = "title"

                parse_story = ParserUserStory()
                text_to_struct_out = parse_story.text_to_structure(' '.join(i for i in chart_data['xAxisDataKeys']+ chart_data['yAxisDataKeys']), currency)
                if len(text_to_struct_out)>=1:
                    chart_dict['chartDimension'] = text_to_struct_out[0][0]['chartDimension']
                    chart_dict['possibleCharts'] = text_to_struct_out[0][0]['possibleCharts']
                    chart_dict['periodicity'] = all_data['data']['dataDetails']['periodicity']
                chart_dict['recommendedCharts'] = final_chart_out
                final_chart_out.append(chart_data)
                
                if chart_layout_array:
                    i = len(chart_layout_array)
                    layout = chart_layout_dict.copy()
                    layout["i"] = list(string.ascii_lowercase)[i]
                    layout["x"] = layout_x[i%3]
                    if len(chart_dict["recommendedCharts"])>=1:
                        if 'chartType' in chart_dict["recommendedCharts"][0]:
                            if chart_dict["recommendedCharts"][0]['chartType'] in ["numberTile","kpiTile"]:
                                layout["h"] = 5
                                layout["minH"] = 5
                    chart_layout_array.append(layout)

                #code for isCurrency starts
                # if input_["chartType"] == "numberTile":
                #     print("==============chartType================")
                #     print(chart_dict['recommendedCharts'][0]['chartData']['recommendedCharts'][0]['chartType'])
                #     print(chart_dict['recommendedCharts'][0]['chartData']['recommendedCharts'][0]['xAxisDataKeys'])
                #     print(combined_dataSet['currencyColumns'])
                #     if len(chart_dict['recommendedCharts'][0]) >= 1:
                #         if (chart_dict['recommendedCharts'][0]['chartData']['recommendedCharts'][0]['chartType']) == "numberTile":
                #             if len(combined_dataSet['currencyColumns']) == 0:
                #                 isCurrency = "False in all data - currency 0"
                #                 chart_dict['recommendedCharts'][0]['chartData']['recommendedCharts'][0]['isCurrency'] = isCurrency
                #                 chart_dict['recommendedCharts'][0]['isCurrency'] = isCurrency
                #             if len(combined_dataSet['currencyColumns']) >= 1:
                #                 isCurrency_ = set(chart_dict['recommendedCharts'][0]['chartData']['recommendedCharts'][0]['xAxisDataKeys']).issubset(combined_dataSet['currencyColumns'])
                #                 isCurrency_ = str(isCurrency_)
                #                 if isCurrency_ == "True":
                #                     print("Ture in widget")
                #                     isCurrency= "trueee in widget"
                #                 if isCurrency_ == "False":
                #                     isCurrency = "False in widget"
                #                     print("False in widget")
                #                 chart_dict['recommendedCharts'][0]['chartData']['recommendedCharts'][0]['isCurrency'] = isCurrency
                #                 chart_dict['recommendedCharts'][0]['isCurrency'] = isCurrency
                                
                            

                #code for isCurrency ends
                #########################
                # Testing for isCurrecny method 2 starts
                all_data_db = mydb["all_data_db"]
                all_data_details = all_data_db.find_one({"workspaceId" :  workspace_id})
                if all_data_details is not None:
                    if len(all_data_details['allData']['chartData'][0]['recommendedCharts'])>=1:
                        if 'isCurrency' not  in (all_data_details['allData']['chartData'][0]['recommendedCharts'][0]):
                            if 'chartData' in chart_dict['recommendedCharts'][0]:
                                for q in (chart_dict['recommendedCharts'][0]['chartData']['recommendedCharts']):
                                    if  q['chartType'] == "numberTile":
                                        q['isCurrency']  = (all_data_details['allData']['chartData'][0]['recommendedCharts'][0]['isCurrency'])
                                        chart_dict['isCurrency'] = True
                                        
                            else:
                                for q in chart_dict['recommendedCharts']:
                                    if q['chartType'] == 'numberTile':
                                        if len(combined_dataSet["currencyColumns"]) == 0:
                                            q['isCurrency'] = False
                                            print("===============len is 0 ==================")
                                        if len(combined_dataSet["currencyColumns"]) >= 1:
                                            # if input_['xAxisDataKeys'] == combined_dataSet["currencyColumns"]:
                                            #     q['isCurrency'] = True
                                            # if input_['xAxisDataKeys'] in combined_dataSet["currencyColumns"]:
                                            #     q['isCurrency'] = True
                                            print("===============len is 1 ==================")
                                            if any(x in input_['xAxisDataKeys'] for x in combined_dataSet["currencyColumns"]):
                                                q['isCurrency'] = True
                                                print("======true in if======")
                                            else:
                                                print("=====false in else=======")
                                                q['isCurrency'] = False
                                    else:
                                        print("======if is excuted inside for=======")
                                    print("-----------q---------------")
                                    print(q)

                if all_data_details is None:
                    if 'chartData' in chart_dict['recommendedCharts'][0]:
                            for q in (chart_dict['recommendedCharts'][0]['chartData']['recommendedCharts']):
                                if len(combined_dataSet["currencyColumns"]) == 0:
                                    q['isCurrency'] = False
                                if len(combined_dataSet["currencyColumns"]) >=1:
                                    print("q1   ;", q['chartType'])
                                    print("===========    The value of currency column in  data db   :   ",combined_dataSet["currencyColumns"])
                                    print("===========    The value of XAxisDatakeys in chart dict recommended charts  :   ", q['xAxisDataKeys'])
                                    if  q['chartType'] == "numberTile":
                                        if q['xAxisDataKeys'] == combined_dataSet["currencyColumns"]:
                                            q['isCurrency']  = True
                                            chart_dict['isCurrency'] = True
                                        else:
                                            chart_dict['isCurrency'] = False
                                            q['isCurrency'] = False
                   
                    # pprint.pprint(chart_dict['recommendedCharts'][0]['chartData'])
                #Testing for isCurrecny method 2 ends
                #########################
                #############################################################
                #to get the tabular chart starts
                if input_["chartType"] == "tabularChart":
                    if 'recommendedCharts' in chart_dict:
                        aggregated_dataset = chart_dict["recommendedCharts"][0]["aggregatedDataset"]
                        if len(aggregated_dataset)>= 1:
                            aggregated_dataset = aggregated_dataset
                        else:
                            combined_dataset = single_dataSet["tableData"]
                            combined_dataset_df = pd.DataFrame(combined_dataset)
                            a = (input_["yAxisDataKeys"])+(input_["xAxisDataKeys"])
                            combined_dataset_df = combined_dataset_df[a]
                            aggregated_dataset = combined_dataset_df.to_dict("list")
                            
                        aggregated_dataset_df = pd.DataFrame(aggregated_dataset)
                        #dimension only for get_tabular_chart starts
                        dimension_values_list = list(aggregated_dataset.keys())
                        dimension_values_list_str = ' '.join(map(str, dimension_values_list))
                        parse_story = ParserUserStory()
                        dimension = parse_story.text_to_structure(dimension_values_list_str, currency)
                        dimension = (dimension[0][0]["chartDimension"])
                        
                        #dimension only for get_tabular_chart ends
                        tabular_data = chart_viewer.get_tabular_chart(aggregated_dataset_df, dimension)
                        #for sorting the tabular chart starts
                        tabular_data_keys = tabular_data.keys()
                        # print("--------tabular_data_keys---------")
                        # print(tabular_data_keys)
                        # print(str(list(tabular_data_keys)))
                        tabular_data_keys = ((list(tabular_data_keys)))
                        listToStr = ' '.join(map(str, tabular_data_keys))
                        parse_story = ParserUserStory()
                        dimension = parse_story.text_to_structure(listToStr, currency)
                
                        dimension = (dimension[0][0]["chartDimension"])
                        # print("------dimension---------")
                        # pprint.pprint(dimension)
                        # if "Period" in tabular_data_keys:
                        #     dimension["Timeline Dimension"] = ["M"]
                        # print("------dimension 2---------")
                        # pprint.pprint(dimension)
                        a = dimension
                        b = {'Categorical Dimension' : a['Categorical Dimension'],'Timeline Dimension' : a['Timeline Dimension'],'Measure' : a['Measure'],'Goal Measure' : a['Goal Measure']}
                        c = b
                        d = list(c.values())
                        # z = list(map(str,str(d)))
                        z = [x for x in d if len(x)>=1] #for getting only the values with not empty list
                        y = list(itertools.chain(*z))
                        y = [y.replace(('Y' or 'M'), 'Period') for y in y]
                        for n, i in enumerate(y):
                            if i == "M":
                                y[n] = "Period"
                            if i == "Y":
                                y[n] = "Period"
                            y = [sub.replace('M', 'Period') for sub in y]
                            y = [sub.replace('Y', 'Period') for sub in y]
                            print("==========y=========")
                            print(y)
                            final_agg_list = {}
                            for i in y:
                                final_agg_list[i] = aggregated_dataset[i]
                        chart_dict["recommendedCharts"][0]["aggregated_dataset_order"] = ((list(final_agg_list.keys())))
                        chart_dict["recommendedCharts"][0]["aggregatedDataset"] = tabular_data
                        chart_dict["chartDimension"] = dimension
                        #grouping for tabular chart starts
                        # dimension_list_values = sum(b.values(), [])
                        # chart_dict["recommendedCharts"][0]["aggregatedDataset"] = tabular_data.grouby(['Period', 'Region'], as_index= False).mean()
                        #grouping for tabular chart ends


                #     #for sorting the tabular chart ends
                # #to get the tabular chart ends
                # #############################################################

                mycol.insert_one({'workspaceId' : workspace_id, 'widgetName' : widget_name, 'userStory' : widget_name, 'chartData' : chart_dict, 'currencies': currency})
                data_db.update_one({'workspaceId' : workspace_id}, {"$set" : {"layout": chart_layout_array}})
                ##################
                #to update the tabular in all data starts
                tochangewidget = mycol.find({"workspaceId" :  workspace_id})
                for w_data in tochangewidget:
                    temp, chartData, parameter_dimension = {}, {}, {}
                    temp['userStory'] = w_data.get('userStory', None)
                    temp['widgetName'] = w_data.get('widgetName',None)
                    temp['widgetId'] = str(w_data.get('_id',None))
                    print("widgetid   :   ",temp['widgetId'])
                    chartData = w_data.get("chartData", [])
                    if type(chartData) == list:
                        chartData = chartData[0]
                    else:
                        chartData = chartData
                chart_dict["userStory"] = temp["userStory"]
                chart_dict["widgetName"] = temp["widgetName"]
                chart_dict["widgetId"] = temp["widgetId"]
                (all_data_details['allData']['chartData']).append(chart_dict)
                all_data_db.update_one({'workspaceId' : workspace_id}, {"$set" : {"allData": all_data_details['allData']}})



                #to update the tabular ends
                ##################

                message = "widget added"
                status = 200
            response['message'] = message
            response['success'] = True
            response['payload'] = chart_dict
            print("================widget post ends===================")
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))


    # @app.route('/widget',methods=['GET'])
    def get(self):
        print("================widget get starts===================")
        output=[]
        response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
        status, is_success = 400, False
        mycol = mydb["widget"]
        data_db = mydb["data"]
        workspace_db = mydb["workspace"]
        res = {}
        try:
            workspace_id = str(request.args.get('workspaceId'))
            #to get the currency colum starts
            all_data = data_db.find_one({"workspaceId" :  workspace_id})
            workspace_data = workspace_db.find_one({"_id":ObjectId(f"{workspace_id}")})
            single_chart_details = all_data['data']['dataDetails']
            dataSet ={}
            combined_dataSet = all_data['data']['dataSet']
            user_currency = workspace_data.get('currencies', "$")
            combined_dataSet["userCurrency"] = user_currency
            variables_list = single_chart_details.get('variables', [])
            isCurrencyList = []
            if variables_list:
                isCurrencyList = [variable['variableName'] for variable in variables_list if variable["isCurrency"]]
            combined_dataSet["currencyColumns"] = isCurrencyList
            #to get the currency colum ends
            for q in mycol.find({'workspaceId' : workspace_id}):
                q["_id"] = str(q['_id'])
                q['widgetId'] = str(q['_id'])
                #code for isCurrency starts
                all_data_db = mydb["all_data_db"]
                all_data_details = all_data_db.find_one({"workspaceId" :  workspace_id})
                if all_data_details is not None:
                    if len(all_data_details['allData']['chartData'][0]['recommendedCharts']) > 0:
                        if 'isCurrency' in (all_data_details['allData']['chartData'][0]['recommendedCharts'][0]):
                            # print("=================get in widget / iscurrency======================")
                            # pprint.pprint(len(q['chartData']['recommendedCharts']))
                            # print("==========================##")
                            if 'chartData' in q:
                                for i in q['chartData']['recommendedCharts']:
                                    if i['chartType'] == 'numberTile':
                                        i['isCurrency']  = (all_data_details['allData']['chartData'][0]['recommendedCharts'][0]['isCurrency'])
                                        q['chartData']['isCurrency'] = (all_data_details['allData']['chartData'][0]['recommendedCharts'][0]['isCurrency'])
                                pprint.pprint(q['chartData']['recommendedCharts'])
                            else:
                                print("=====================================================")
                                print("====================chartData is empty in q in get widget =================")
                                print("=====================================================")
                        else:
                                print("=====================================================")
                                print("====================iscurrency is empty in q in get widget =================")
                                print("=====================================================")
                if all_data_details is None:
                    if 'chartData' in q:
                            for i in q['chartData']['recommendedCharts']:
                                if i['chartType'] == 'numberTile':
                                    if len(combined_dataSet["currencyColumns"]) == 0:
                                        i['isCurrency'] = False
                                    if len(combined_dataSet["currencyColumns"]) >= 1:
                                        if 'xAxisDataKeys' in i:
                                            if (i['xAxisDataKeys']) == combined_dataSet["currencyColumns"]:
                                                i['isCurrency'] = True
                                                q['chartData']['isCurrency'] = True
                                            else:
                                                q['chartData']['isCurrency'] = False
                                                i['isCurrency'] = False
                                        else:
                                            print("=====================================")
                                            print("=================axis datakeys not present=======================")
                                            print("==========================================")
                                            for i in q['chartData']['recommendedCharts'][0]['chartData']['recommendedCharts']:
                                                if (i['xAxisDataKeys']) == combined_dataSet["currencyColumns"]:
                                                    i['isCurrency'] = True
                                                    q['chartData']['isCurrency'] = True
                                                else:
                                                    q['chartData']['isCurrency'] = False
                                                    i['isCurrency'] = False
                #code for isCurrency ends
                    
                output.append(q)
            response['message'] = 'success'
            response['success'] = True
            response['payload'] = output
            status = 200
            print("================widget get ends===================")
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

    # @app.route('/widget', methods=['DELETE'])
    def delete(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            mycol = mydb["widget"]
            # data_col = mydb["data"]
            _id = request.args.get('widgetId')
            if(list(mycol.find({"_id":ObjectId(f"{_id}")}))):
                myquery = {"_id":ObjectId(f"{_id}")}
                mycol.delete_one(myquery)
                # data_col.delete_many({"widgetId" : _id})
                message = 'Chart(Widget) has been deleted'
                status = 200
                is_success = True
            else:
                message = "Chart(Widget) doesn't exists"
                status = 400
            response['message'] = message
            response['success'] = is_success
            response['payload'] = message
                
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

    def put(self):
        try:
            print("================widget put starts===================")
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            mycol = mydb["widget"]
            workspace_db = mydb["workspace"]
            user_db = mydb["users"]
            widgets = request.json
            _id = widgets.get('widgetId', None)
            widget_name = widgets.get('widgetName', None)
            user_story = widgets.get('userStory', None)
            widget_data = mycol.find_one({"_id":ObjectId(f"{_id}")})
            workspace_id = widget_data.get('workspaceId')
            workspace_data = workspace_db.find_one({"_id":ObjectId(f"{workspace_id}")})
            currency = widgets.get('currencies', user_db.find_one({"email": workspace_data["email"]}).get('currencies', "$"))
            if not widget_data:
                message = "Widget does not exist"
            else:
                if user_story:
                    parse_story = ParserUserStory()
                    widget_payload , user_story= parse_story.text_to_structure(user_story, currency)
                    no_of_widgets = len(user_story)
                if no_of_widgets > 1:
                    message = 'Only One user story allowed'
                    response['message'] = message
                    response['success'] = True
                    response['payload'] = message
                    return(make_response(response, status))
                if workspace_data:
                    workspace_user_story = workspace_data.get('userStory', None)
                    if workspace_user_story:
                        workspace_user_story = workspace_user_story.replace(widget_data['userStory'], user_story[0])
                else:
                    workspace_user_story = None 
                user_story = user_story[0] if isinstance(user_story, list) and user_story else user_story 
                value = { k : widget_data[k] for k in set(widget_data) - set(widgets) }
                if "_id" in value.keys():
                    del(value["_id"])
                if value:
                    widgets.update(value)
                if widgets['widgetId']:
                    del(widgets["widgetId"])
                mycol.replace_one({'_id': ObjectId(f"{_id}")},widgets, upsert = True)
                data_db = mydb['data']
                if user_story:
                    data_update = { "$set": {"data" : widget_payload[0]} }
                    data_db.update_one({'widgetId' : _id}, data_update)        
                
                myquery = {"_id":ObjectId(f"{workspace_id}")}
                newvalues = { "$set": {"userStory" : workspace_user_story} }
                workspace_db.update_one(myquery, newvalues)
                message = "Widget Updated"
                status = 200
                is_success = True
            response['message'] = message
            response['success'] = is_success
            response['payload'] = message
            print("================widget put ends===================")
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))


class DataTable(Resource):
    # @app.route('/data', methods=['POST']) >>>>>>>>>>> widget-wise dataset version api (deprecated)
    # def post(self):
    #     try:
    #         response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
    #         status, is_success = 400, False
    #         mycol = mydb["data"]
    #         input_ = request.json
    #         widget_id = input_.get('widgetId',None)
    #         # data = input_.get('data',None)
    #         data = input_
    #         # widget_id = data.get('widgetId', None) if not widget_id else widget_id
    #         print(data)
    #         parsed_data = []
    #         temp = {}
    #         user_story = None
    #         random_data = data_generator.get_random_data(data)
    #         temp['dataset'] = random_data
    #         charts_suggested = data.get("recommendedCharts", [])
    #         charts = Charts()
    #         chart_viewer = ChartViewer()
    #         if not charts_suggested:
    #             user_story = data.get("userStory",None)
    #             if not user_story:
    #                 user_story = data.get("story",None)
    #                 if not user_story:
    #                     user_story = data.get("widgetStory",None)
    #             if user_story:
    #                 parse_story = ParserUserStory()
    #                 widget_payload , user_story= parse_story.text_to_structure(user_story)
    #                 print("widget payload",widget_payload)
    #                 if widget_payload and isinstance(widget_payload, list):
    #                     temp = widget_payload[0]
    #             else:
    #                 charts_suggested = charts.find_chart_type(data)
    #                 if charts_suggested:
    #                     final_chart_out = []
    #                     for suggested in charts_suggested:
    #                         aggregated_dataset, aggregated_function = chart_viewer.get_chart_view(suggested, random_data)
    #                         chart_type = suggested.get('chartType',None)
    #                         if chart_type and chart_type == 'numberTile':
    #                             x_axis = suggested.get('xAxisDataKeys',[])
    #                             y_axis = suggested.get('yAxisDataKeys',[])
    #                             if x_axis:
    #                                 suggested['yAxisDataKeys'] = []
    #                         suggested['aggregatedDataset'] = aggregated_dataset
    #                         suggested['aggregationType'] = aggregated_function
    #                         suggested['userStory'] = user_story
    #                         final_chart_out.append(suggested)
    #                     charts_suggested = final_chart_out
    #                 temp['recommendedCharts'] = charts_suggested
    #                 categories = data.get('categories',[])
    #                 variables = data.get('variables',[])
    #                 periodicity = data.get('periodicity',[])

    #                 temp['categories'] = categories
    #                 temp['variables'] = variables
    #                 temp['periodicity'] = periodicity

    #                 if user_story:
    #                     temp['userStory'] = user_story
    #         if(list(mycol.find({"widgetId" : widget_id}))):
    #             mycol.replace_one({
    #                 'widgetId': widget_id
    #                     },{'data':temp , 'widgetId': widget_id}, upsert = True)
    #             message = "Data Updated"
    #         else:
    #             mycol.insert_one({'widgetId' : widget_id, 'data' : temp})
    #         # random_data = data_generator.get_random_data(data)
    #         response['payload']=  temp
    #         response['widgetId'] = widget_id
    #         message = "Data Generated"
    #         status = 200
    #         response['message'] = message
    #         response['success'] = True
    #     except:
    #         print(traceback.format_exc())
    #         logger.error(traceback.format_exc())
    #         response['message'] = "Error"
    #         status = 400
    #     finally:
    #         return(make_response(response, status))
            

    # @app.route('/data', methods=['GET'])
    def get(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success = 400, False
            mycol = mydb["data"]
            keywords_db = mydb["training_keywords"]
            res = {}      
            workspace_id = str(request.args.get('workspaceId'))
            newCategoryName = request.args.get('newCategoryName', None)
            newCategoryLength = request.args.get('newCategoryLength', None)
            newCategory = {'length' : newCategoryLength,
                            'newCategoryName' : newCategoryName}
            out = mycol.find({'workspaceId' : workspace_id})
            out = list(out)
            ## To get the New Category Sub Division Name - Starts.
            if newCategoryName is not None and newCategoryLength is not None:
                keywords_data = keywords_db.find_one({"Keywords" : newCategoryName})
                if not keywords_data:
                    keywords_data = keywords_db.find_one({"Keywords" : newCategoryName.title()})
                if not keywords_data:
                    keywords_data = keywords_db.find_one({"Keywords" : newCategoryName.lower()})
                ####
                if keywords_data:
                    if type(keywords_data['Keywords_list']) == float:
                        keywords_data = keywords_db.find_one({"Keywords" : newCategoryName.title()})
                    if type(keywords_data['Keywords_list']) != float:
                        if len(keywords_data['Keywords_list']) == 0:
                            keywords_data = keywords_db.find_one({"Keywords" : newCategoryName.title()})
                ####
                if keywords_data:
                    if type(keywords_data['Keywords_list']) == float:
                        keywords_data = keywords_db.find_one({"Keywords" : newCategoryName.lower()})
                    if type(keywords_data['Keywords_list']) != float:
                        if len(keywords_data['Keywords_list']) == 0:
                            keywords_data = keywords_db.find_one({"Keywords" : newCategoryName.lower()})
                ####
                if keywords_data:
                    if type(keywords_data['Keywords_list']) != float:
                        if len(keywords_data['Keywords_list'])>1:
                            if keywords_data['Keywords'].lower() == newCategoryName.lower():
                                list2 = []
                                if  int(newCategoryLength) < len(keywords_data['Keywords_list']):
                                    newCategory['syntheticElements'] = keywords_data['Keywords_list'][0 : int(newCategoryLength) ]
                                if  int(newCategoryLength) > len(keywords_data['Keywords_list']):
                                        for i in range(int(newCategoryLength) - len(keywords_data['Keywords_list'])):
                                            i = i +1
                                            list1 = newCategoryName + '_' + str(i)
                                            list2.append(list1)
                                        newCategory['syntheticElements'] = keywords_data['Keywords_list']+list2
                                if int(newCategoryLength) == len(keywords_data['Keywords_list']):
                                    newCategory['syntheticElements'] = keywords_data['Keywords_list']
                        if len(keywords_data['Keywords_list']) == 0:
                            list2 = []
                            for i in range(int(newCategoryLength)):
                                i +=1
                                list1 = newCategoryName + '_' + str(i)
                                list2.append(list1)
                            newCategory['syntheticElements'] = list2
                    if type(keywords_data['Keywords_list']) == float:
                        list2 = []
                        for i in range(int(newCategoryLength)):
                            i +=1
                            list1 = newCategoryName + '_' + str(i)
                            list2.append(list1)
                        newCategory['syntheticElements'] = list2
                if not keywords_data:
                    list2 = []
                    for i in range(int(newCategoryLength)):
                        i +=1
                        list1 = newCategoryName + '_' + str(i)
                        list2.append(list1)
                    newCategory['syntheticElements'] = list2

            ## To get the New Category Sub Division Name - Ends.
            for i in out[0]['data']['dataDetails']['categories']:
                index_num = 0 
                if type(i['noOfSyntheticElements']) == int:
                    index_num = i['noOfSyntheticElements']
                if type(i['noOfSyntheticElements']) == str:
                    if len(i['noOfSyntheticElements'])>=1 :
                        i['noOfSyntheticElements'] = int(i['noOfSyntheticElements'])
                        index_num = i['noOfSyntheticElements']
                i['syntheticElements'] = i['syntheticElements'][0:index_num]
                #nonuniform m4 starts
                if 'nonUniformElements' in i : #added bcous of error on 5-1-21
                    if i['nonUniformElements'] != None:
                        if len(i['nonUniformElements'][0]['elements'])>=1:
                            i['nonUniformElements'][0]['nonUniformLength'] = len(i['nonUniformElements'][0]['elements'])
                #nonuniform m4 sends
            
            if not out:
                response['payload'] = {}
            else:
                out = out[0] if isinstance(out, list) else out
                out["_id"] = str(out['_id'])
                out['workspaceId'] = str(out['workspaceId'])
                out['data']['dataDetails']['newCategory']  = newCategory
                response['payload'] = out['data']['dataDetails']
            status = 200
            
            response['message'] = 'success'
            response['success'] = True
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))
        
        

    # @app.route('/data', methods=['PUT'])

    def put(self):
        try:
            # import pdb
            # pdb.set_trace()
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            mycol = mydb["data"]
            widget_db = mydb["widget"]
            input_ = request.json
            workspace_id = input_.get('workspaceId',None)
            widgetId = input_.get('widgetId',None)
            reviewWindow = input_.get('reviewWindow', False)
            ip_rows = input_.get('ip_rows', 0)
            col_percentage = input_.get('col_percentage', {})
            cat_name = input_.get('cat_name', None)

            all_data = {}
            data = input_.get('data',None)
            widget_data = data
            fromKeywords = input_.get('fromKeywords', False)
            temp = {}
            data_out = mycol.find_one({'workspaceId' : workspace_id})
            chart_layout_array = data_out.get("layout",[])
            final_chart_out = []
            if fromKeywords:
                print("===== yes / fromKeywords / DATATABLE / PUT ===")
                whole_data = data_out['data']['dataDetails']
                for cats in data['categories']:
                    if cats['categoryId'].lower().strip() not in [whole_cat['categoryId'].lower().strip() for whole_cat in whole_data['categories'] if 'categoryId' in whole_cat.keys()]:
                        whole_data['categories'].append(cats)
                for vars in data['variables']:
                    
                    vars['name'] = vars['name'].strip()
                    vars['variableId'] = vars['variableId'].strip()
                    vars['variableName'] = vars['name'].strip()
                    if vars['variableId'].lower().strip() not in [whole_var['variableId'].lower().strip() for whole_var in whole_data['variables'] if 'variableId' in whole_var.keys()]:
                        whole_data['variables'].append(vars)
                data = whole_data
            # if 'categories' in data.keys():
            #     temp_cat = []
            #     for category in data.get('categories'):
            #         # if not category['syntheticElements']:
            #         #     no_syntheticElement = int(category['noOfSyntheticElements']) + 1
            #         #     syntheticElements = list(map(lambda x: category['prefix'] + str(x), range(1,no_syntheticElement)))
            #         #     category['syntheticElements'] = syntheticElements
            #         temp_cat.append(category)
            #     data['categories'] = temp_cat
            random_data = data_generator.get_random_data(copy.deepcopy(data))

            # print("====random_data======")
            # pprint.pprint(random_data["tableColumns"])
            #module for variable name not to repeat starts - 2
            new_variable_list =  [i["variableName"] for i in widget_data["variables"]]
            new_category_list = [i["elementName"] for i in widget_data["categories"]]
            for i in new_variable_list:
                if new_variable_list.count(i)>1:
                    print("========true=========")
                    response = {'success' : False, 'message' : 'Value Repeated', "payload" : 'Failure'}
                    status, is_success = 400, False
                    return response,status, is_success
            for i in new_category_list:
                if new_category_list.count(i)>1:
                    print("========true=========")
                    response = {'success' : False, 'message' : 'Value Repeated', "payload" : 'Failure'}
                    status, is_success = 400, False
                    return response,status, is_success
            if random_data["tableColumns"] == new_variable_list:
                print("========fales=========")
            #module for variable name not to repeat ends - 2
            if not fromKeywords and reviewWindow is False:
                print("===== yes /not fromKeywords / DATATABLE / PUT ===")
                workspace_charts = list(widget_db.find({"workspaceId": workspace_id}))
                charts = Charts()
                # charts_suggested = charts.find_chart_type(data)
                chart_viewer = ChartViewer()
                if workspace_charts:
                    for indi_chart in workspace_charts:
                        new_chart_data = {'categories':[], 'variables':[], 'periodicity':{}}
                        chart_data = indi_chart.get('chartData',None)
                        user_story = chart_data.get('userStory', "")
                        chart_title = chart_data.get('chartTitle', "")
                        currency = chart_data.get('currencies', '$')
                        for chart_cat in chart_data.get('categories',[]):
                            for dataset_cat in data['categories']:
                                if chart_cat['categoryId'] == dataset_cat['categoryId']:
                                    chart_cat.update(dataset_cat)
                            new_chart_data['categories'].append(chart_cat)
                        for chart_var in chart_data.get('variables',[]):
                            for dataset_var in data['variables']:
                                if chart_var['variableId'] == dataset_var['variableId']:
                                    chart_var.update(dataset_var)
                            new_chart_data['variables'].append(chart_var)
                        new_chart_data['periodicity'] = chart_data.get('periodicity',{})
                        charts_suggested = charts.find_chart_type(new_chart_data, None, user_story)
                        possible_charts = []
                        # all_data = data_generator.get_random_data(out_dict)
                        if charts_suggested:
                            for each_chart in possibleChartList:
                                possibleChartInput = {}
                                possibleChartInput['xAxisDataKeys'] = charts_suggested[0].get('xAxisDataKeys',[])
                                possibleChartInput['yAxisDataKeys'] = charts_suggested[0].get('yAxisDataKeys',[])
                                possibleChartInput['aggregationType'] = 'sum'
                                possibleChartInput['userStory'] = user_story
                                possibleChartInput['currencies'] = currency
                                possibleChartInput['chartType'] = each_chart
                                possibleChartInput['chartTitle'] = chart_title
                            if each_chart == 'numberTile':
                                x_axis = possibleChartInput.get('xAxisDataKeys',[])
                                y_axis = possibleChartInput.get('yAxisDataKeys',[])
                                if y_axis:
                                    possibleChartInput['xAxisDataKeys'] = y_axis
                                    possibleChartInput['yAxisDataKeys'] = []
                                if possibleChartInput['xAxisDataKeys'][0] in variable_logos.keys():
                                    possibleChartInput['tileLogo'] = variable_logos[possibleChartInput['xAxisDataKeys'][0]]
                                if len(dimension)== 0:
                                    dimension = []
                                each_chart_aggregated_dataset, each_chart_aggregated_function, dimension = chart_viewer.get_chart_view(possibleChartInput, random_data, new_chart_data['periodicity'], dimension)
                                if each_chart_aggregated_dataset and not isinstance(each_chart_aggregated_dataset, str):
                                    possibleChartInput['aggregatedDataset'] = each_chart_aggregated_dataset
                                    possibleChartInput['aggregationType'] = each_chart_aggregated_function
                                    possible_charts.append(possibleChartInput)
                        final_chart_out = []
                        new_chart_data_backup = []
                        if charts_suggested:
                            for suggested in charts_suggested:
                                # suggested['userStory'] = line.capitalize()
                                # suggested['currencies'] = currency
                                chart_type = suggested.get('chartType',None)
                                if chart_type and chart_type == 'numberTile':
                                    x_axis = suggested.get('xAxisDataKeys',[])
                                    y_axis = suggested.get('yAxisDataKeys',[])
                                    if y_axis:
                                        suggested['xAxisDataKeys'] = y_axis
                                        suggested['yAxisDataKeys'] = []
                                    if suggested['xAxisDataKeys'][0] in variable_logos.keys():
                                        suggested['tileLogo'] = variable_logos[suggested['xAxisDataKeys'][0]]
                                # import pdb
                                # pdb.set_trace()
                                new_chart_data_backup_loop = new_chart_data
                                dimension = []
                                new_chart_data_backup.append(new_chart_data_backup_loop)
                                aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(suggested, random_data, new_chart_data['periodicity'], dimension)
                                suggested['aggregatedDataset'] = aggregated_dataset
                                suggested['aggregationType'] = aggregated_function
                                suggested['currencies'] = currency
                                suggested['userStory'] = user_story
                                suggested['chartTitle'] = chart_title
                                final_chart_out.append(suggested)
                        new_chart_data['recommendedCharts'] = final_chart_out
                        new_chart_data['possibleCharts'] = possible_charts
                        new_chart_data['chartDimension'] = chart_data.get('chartDimension',())
                        new_chart_data['userStory'] = user_story
                        new_chart_data['chartTitle'] = chart_title
                        #code for  chart updation error while newchart n updation method 1 - starts 
                        if len(new_chart_data['recommendedCharts']) == 0:
                            if len(new_chart_data_backup)>=1:
                                new_chart_data = new_chart_data_backup[-1]
                        #code for  chart updation error while newchart n updation method 1 - ends
                        #code for  chart updation error while newchart n updation method 1.1 - starts 
                        indi_chart['chartData'] = new_chart_data
                        widget_id = str(indi_chart["_id"])
                        if len(new_chart_data['recommendedCharts']) == 0:
                            currentWidget = widget_db.find_one({"_id": ObjectId(f"{widget_id}")})
                            if len(currentWidget['chartData']['recommendedCharts'])>=1:
                                new_chart_data['recommendedCharts'] = currentWidget['chartData']['recommendedCharts']
                                # new_chart_data['chartDimension'] = currentWidget['chartData']['chartDimension']
                                new_chart_data['userStory'] = currentWidget['userStory']
                                # new_chart_data['chartTitle'] = currentWidget['chartData']['recommendedCharts'][0]['chartTitle']
                                new_chart_data['chartTitle'] = currentWidget['userStory']
                        #code for  chart updation error while newchart n updation method 1.1 - ends
                        myquery = {"_id" : ObjectId(f"{widget_id}")}
                        newvalues = { "$set": {"chartData": indi_chart['chartData']}}
                        ##error for resolving the chart disappearing starts

                        ##error for resolving the chart disappearing ends
                        widget_db.update_one(myquery, newvalues)
                        widget_db.update_many(myquery, newvalues)
                            #code for chart updation error while newchart n updation starts
                            # if len(new_chart_data['recommendedCharts']) == 0:
                            #     print("=======needed widget Id=========")
                            #     print(widget_id)
                            #     widget_data = widget_db.find_one({"_id":ObjectId(f"{widget_id}")})

                            #     new_chart_data["recommendedCharts"]= widget_data["chartData"]["recommendedCharts"]
                            #     if "possibleCharts" in  widget_data["chartData"]:
                            #         new_chart_data['possibleCharts'] = widget_data["chartData"]["possibleCharts"]
                            #     if "chartDimension" in  widget_data["chartData"]:
                            #         new_chart_data['chartDimension'] = widget_data["chartData"]["chartDimension"]
                            #     new_chart_data['userStory'] = widget_data["userStory"]
                            #     indi_chart['chartData'] = new_chart_data
                            #     myquery = {"_id" : ObjectId(f"{widget_id}")}
                            #     newvalues = { "$set": {"chartData": indi_chart['chartData']}}
                            #     print("========indi_chart['chartData']=========")
                            #     pprint.pprint(indi_chart['chartData'])
                            #     widget_db.update_one(myquery, newvalues)

                            #code for chart updation error while newchart n updation ends
            elif (widgetId and fromKeywords and reviewWindow is False):
                print("===== yes / elif / DATATABLE / PUT ===")
                widget_ = widget_db.find_one({"_id": ObjectId(f"{widgetId}")})
                widget_data['periodicity'] = widget_['chartData']['periodicity'] if 'periodicity' in widget_['chartData'] else {}
                charts = Charts()
                chart_viewer = ChartViewer()
                user_story = widget_.get('userStory', "")
                chart_title = widget_.get('chartTitle', '')
                currency = widget_.get('currencies', '$')
                dimensions = input_.get('dimensions',None)
                dimensions = { 
                    "Measure" : dimensions["Measure"] if "Measure" in dimensions else [],
                    "Goal Measure" : dimensions["Goal Measure"] if "Goal Measure" in dimensions else [],
                    "Categorical Dimension" : dimensions["Categorical Dimension"] if "Categorical Dimension" in dimensions else [],
                    "Timeline Dimension" : dimensions["Timeline Dimension"] if "Timeline Dimension" in dimensions else []
                }
                ############################
                #code for error in Updation of Keywords starts
                if len(chart_title) == 0:
                    
                    # if len(widget_['chartData']['recommendedCharts'])>=1:
                    #     chart_title = widget_['chartData']['recommendedCharts'][0]["chartTitle"]
                    # if len(widget_['chartData']['recommendedCharts'])==0:
                    chart_title = {
                        "Measure" : len(dimensions["Measure"]),
                        "Goal Measure" : len(dimensions["Goal Measure"]),
                        "Categorical Dimension" : len(dimensions["Categorical Dimension"]),
                        "Timeline Dimension" : len(dimensions["Timeline Dimension"])
                    }
                if len(chart_title) >= 1:
                    chart_title = widget_.get('chartTitle', '')
                chart_dimention = dimensions
                # if dimensions: 
                #     chart_dimention = dimensions
                # if not dimensions:  
                #     chart_dimention = widget_['chartData']['chartDimension']
                #code for error in Updation of Keywords ends
                ############################
                #
                # print("=========widget_data=======")
                # pprint.pprint(widget_data)
                charts_suggested_ip  = {}
                charts_suggested_ip['Measure'] = len(chart_dimention['Measure'])
                charts_suggested_ip['Goal Measure'] = len(chart_dimention['Goal Measure'])
                charts_suggested_ip['Categorical Dimension'] = len(chart_dimention['Categorical Dimension'])
                charts_suggested_ip['Timeline Dimension'] = len(chart_dimention['Timeline Dimension'])
                #

                charts_suggested = charts.find_chart_type(charts_suggested_ip, widget_data, user_story)
                possible_charts = []
                # all_data = data_generator.get_random_data(out_dict)
                if charts_suggested:
                    for each_chart in possibleChartList:
                        possibleChartInput = {}
                        possibleChartInput['xAxisDataKeys'] = charts_suggested[0].get('xAxisDataKeys',[])
                        possibleChartInput['yAxisDataKeys'] = charts_suggested[0].get('yAxisDataKeys',[])
                        possibleChartInput['aggregationType'] = 'sum'
                        possibleChartInput['userStory'] = user_story
                        possibleChartInput['currencies'] = currency
                        possibleChartInput['chartType'] = each_chart
                        # dict(charts_suggested[0].get('chartTitle',[]))
                        possibleChartInput['chartTitle'] = chart_title
                        if each_chart == 'numberTile':
                            x_axis = possibleChartInput.get('xAxisDataKeys',[])
                            y_axis = possibleChartInput.get('yAxisDataKeys',[])
                            if y_axis:
                                possibleChartInput['xAxisDataKeys'] = y_axis
                                possibleChartInput['yAxisDataKeys'] = []
                            if possibleChartInput['xAxisDataKeys'][0] in variable_logos.keys():
                                possibleChartInput['tileLogo'] = variable_logos[possibleChartInput['xAxisDataKeys'][0]]
                        dimension = []
                        each_chart_aggregated_dataset, each_chart_aggregated_function, dimension = chart_viewer.get_chart_view(possibleChartInput, random_data, widget_data['periodicity'], dimension)
                        if each_chart_aggregated_dataset and not isinstance(each_chart_aggregated_dataset, str):
                            possibleChartInput['aggregatedDataset'] = each_chart_aggregated_dataset
                            possibleChartInput['aggregationType'] = each_chart_aggregated_function
                            possible_charts.append(possibleChartInput)
                final_chart_out = []
                if charts_suggested:
                    for suggested in charts_suggested:
                        # suggested['userStory'] = line.capitalize()
                        # suggested['currencies'] = currency
                        chart_type = suggested.get('chartType',None)
                        if chart_type and chart_type == 'numberTile':
                            x_axis = suggested.get('xAxisDataKeys',[])
                            y_axis = suggested.get('yAxisDataKeys',[])
                            if y_axis:
                                suggested['xAxisDataKeys'] = y_axis
                                suggested['yAxisDataKeys'] = []
                            if suggested['xAxisDataKeys'][0] in variable_logos.keys():
                                suggested['tileLogo'] = variable_logos[suggested['xAxisDataKeys'][0]]
                        aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(suggested, random_data, widget_data['periodicity'], dimension)
                        suggested['aggregatedDataset'] = aggregated_dataset
                        suggested['aggregationType'] = aggregated_function
                        suggested['currencies'] = currency
                        suggested['userStory'] = user_story
                        suggested['chartTitle'] = chart_title
                        final_chart_out.append(suggested)
                ##added - disappearing of charts starts m2
                if len(final_chart_out)>=1:
                    widget_data['recommendedCharts'] = final_chart_out
                if len(final_chart_out)==0:
                    widget_data['recommendedCharts'] = widget_['chartData']['recommendedCharts']
                ##added - disappearing of charts ends m2
                # widget_data['recommendedCharts'] = final_chart_out
                widget_data['possibleCharts'] = possible_charts
                widget_data['chartDimension'] = chart_dimention
                widget_data['userStory'] = user_story
                widget_['chartData'] = widget_data
                widget_['chartTitle'] = chart_title
                widget_id = str(widget_["_id"])
                myquery = {"_id" : ObjectId(f"{widget_id}")}
                newvalues = { "$set": {"chartData": widget_['chartData']}}
                widget_db.update_one(myquery, newvalues)
            #Review/Modify window Starts
            if reviewWindow is True:
                workspace_charts = list(widget_db.find({"workspaceId": workspace_id}))
                charts = Charts()
                chart_viewer = ChartViewer()
                #Data distribution in review/modify window starts.
                # ip_rows = 10
                if 'dataDistribution' in data_out and ip_rows == 0 : 
                    if 'ip_rows' in data_out['dataDistribution']:
                        ip_rows = data_out['dataDistribution']['ip_rows']
                        col_percentage = data_out['dataDistribution']['col_percentage']
                if ip_rows > 0:
                    dataset_df = pd.DataFrame(random_data['tableData'])
                    # dataset_df = data_out["data"]["dataSet"]["tableData"]
                    dataset_df = pd.DataFrame(dataset_df)
                    rows = dataset_df.shape[0]
                    if ip_rows == dataset_df.shape[0]:
                        random_data['tableData'] = dataset_df.to_dict(orient = "records")
                    if ip_rows<dataset_df.shape[0]:
                        x = dataset_df
                        y = dataset_df
                        percentage = ip_rows/rows
                        X_train,X_test, y_train, y_test = train_test_split(x,y, train_size = 0.1, test_size = percentage, random_state = 100)
                        X_test = X_test.to_dict(orient = "records")
                        random_data['tableData']= X_test
                    if ip_rows>dataset_df.shape[0]:
                        numberToMultiply = ip_rows/rows
                        new_df = dataset_df.loc[dataset_df.index.repeat(math.ceil(numberToMultiply))]
                        # getting the measures and goal measures and sacling with min and max starts. 
                        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
                        numericdf = new_df.select_dtypes(include=numerics)
                        for i in numericdf.columns:
                            new_df[i] = np.random.randint(new_df[i].min(),new_df[i].max(), size=new_df.shape[0])
                        # getting the measures and goal measures and sacling with min and max ends.
                        new_df = new_df.head(ip_rows)
                        new_df = new_df.reset_index(drop = True)
                        new_df = new_df.to_dict(orient = "records")
                        random_data['tableData'] = new_df
                if len(col_percentage) != 0 and cat_name is not None:
                    dataset_df = data_out["data"]["dataSet"]["tableData"]
                    dataset_df = pd.DataFrame(dataset_df)
                    rows = dataset_df.shape[0]
                    cat_percent_value = list(col_percentage.values())
                    cat_percent_value = [rows*i/100 for i in cat_percent_value]
                    # col_unique_list = dataset_df[cat_name].unique()
                    col_unique_list = list(col_percentage.keys())
                    col = np.repeat(col_unique_list, cat_percent_value)
                    col_df = pd.DataFrame(col, columns = [cat_name])
                    dataset_df = dataset_df.reset_index(drop = True)
                    if col_df.shape[0] <  dataset_df.shape[0]:
                        col_df = col_df.append( col_df.iloc[[-1]*(dataset_df.shape[0] - col_df.shape[0] )] )
                        col_df = col_df.reset_index(drop = True)
                    dataset_df.loc[:, [cat_name]] = col_df[[cat_name]]
                    dataset_df = dataset_df.to_dict(orient = "records")
                    random_data['tableData'] = dataset_df
                        

                #Data distribution in review/modify window ends.

                new_chart_data = []
                if workspace_charts:
                    for indi_chart in workspace_charts:
                        widget_id = str(indi_chart["_id"])
                        indi_chart['chartData']['xAxisDataKeys'] = indi_chart['chartData']['recommendedCharts'][0]['xAxisDataKeys']
                        indi_chart['chartData']['yAxisDataKeys'] = indi_chart['chartData']['recommendedCharts'][0]['yAxisDataKeys']
                        indi_chart['chartData']['chartType'] = indi_chart['chartData']['recommendedCharts'][0]['chartType']
                        dimension = indi_chart['chartData']['chartDimension']
                        # print("====dimension before=====")
                        # pprint.pprint(dimension)
                        old_dimension = dimension.copy()
                        periodicity = indi_chart['chartData']['periodicity']
                        # print("======indi_chart['chartData'] before========")
                        # pprint.pprint(indi_chart['chartData'])
                        aggregated_dataset, aggregated_function,dimension = chart_viewer.get_chart_view(indi_chart['chartData'], random_data,periodicity , dimension)
                        # print("=======aggregated_dataset======")
                        # pprint.pprint(aggregated_dataset)
                        # print("====dimension after=====")
                        # pprint.pprint(dimension)
                        indi_chart['chartData']['chartDimension'] = old_dimension
                        indi_chart['chartData']['recommendedCharts'][0]['aggregatedDataset'] = aggregated_dataset
                        del indi_chart['chartData']['xAxisDataKeys']
                        del indi_chart['chartData']['yAxisDataKeys']
                        del indi_chart['chartData']['chartType']
                        myquery = {"_id" : ObjectId(f"{widget_id}")}
                        # print("======indi_chart['chartData'] after========")
                        # pprint.pprint(indi_chart['chartData'])
                        newvalues = { "$set": {"chartData": indi_chart['chartData']}}
                        widget_db.update_one(myquery, newvalues)
            #Review/Modify window Ends
            all_data['dataSet'] = random_data
            # charts_suggested = data.get("recommendedCharts", [])
            # # charts = Charts()
            # # chart_viewer = ChartViewer()
            # # charts_suggested = charts.find_chart_type(copy.deepcopy(data))
            # if charts_suggested:
            #     final_chart_out = []
                
            #     for suggested in charts_suggested:
            #         aggregated_dataset, aggregated_function = chart_viewer.get_chart_view(suggested, random_data, data['periodicity'])
            #         chart_type = suggested.get('chartType',None)
            #         if chart_type and chart_type == 'numberTile':
            #             x_axis = suggested.get('xAxisDataKeys',[])
            #             y_axis = suggested.get('yAxisDataKeys',[])
            #             if x_axis:
            #                 suggested['yAxisDataKeys'] = []
            #         suggested['aggregatedDataset'] = aggregated_dataset
            #         suggested['aggregationType'] = aggregated_function
            #         suggested['userStory'] = user_story
            #         final_chart_out.append(suggested)
            #     charts_suggested = final_chart_out
            # temp['recommendedCharts'] = charts_suggested
            categories = data.get('categories',[])
            variables = data.get('variables',[])
            periodicity = data.get('periodicity',[])
            temp['categories'] = categories
            temp['variables'] = variables
            temp['periodicity'] = periodicity
            all_data['dataDetails'] = temp
            if(temp):
                mycol.replace_one({
                    'workspaceId': workspace_id
                        },{'data':all_data , 'workspaceId': workspace_id, 'layout': chart_layout_array, 'dataDistribution' : {'ip_rows' : ip_rows, 'col_percentage' : col_percentage, 'cat_name' : cat_name}}, upsert = True)
                message = "Data Updated"
            response['payload']=  {"widget_data" :widget_data,
                                    "final_chart_out_len" : len(final_chart_out),
                                    "final_chart_out" : final_chart_out,
                                    # "charts_suggested" : charts_suggested
                                    }
            response['workspaceId'] = workspace_id
            message = "Updated"
            status = 200
            response['message'] = message
            response['success'] = True
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))
        
        

    # @app.route('/data', methods=['DELETE'])
    def delete(self):
        try:
            mycol = mydb["data_generator"]
            _id = str(request.args.get('widgetId'))
            if(list(mycol.find({"_id":ObjectId(f"{_id}")}))):
                myquery = {"_id":ObjectId(f"{_id}")}
                mycol.delete_one(myquery)
                res = jsonify({'result' : "data has been deleted"})
                status = 200
            else:
                res = jsonify({'result' : "this data doesn't exists"})
                status = 200
                
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            res = jsonify({'result' : "not updated"})
            status = 400
        finally:
            return(make_response(res, status))
    
    
class AllData(Resource):
    # @app.route('/allData', methods=['GET'])
    def get(self):
        '''
        Get all data based on workspace Id
        '''
        message = "No yet started"
        status = 200
        payload = message
        is_success = False
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success = 400, False
            workspace_db = mydb["workspace"]
            all_data_db = mydb["all_data_db"]
            widget_db = mydb["widget"]
            data_db = mydb["data"]
            res = {}        
            workspace_id = str(request.args.get('workspaceId'))
            workspace_data = workspace_db.find_one({"_id":ObjectId(f"{workspace_id}")})
            widget_data = widget_db.find({'workspaceId' : workspace_id})
            all_data = data_db.find_one({"workspaceId" :workspace_id})   
            if 'currencies' in workspace_data:
                user_currency = workspace_data.get('currencies', "$")
            if 'currencies' not in workspace_data:
                user_currency = "$"
            currency = user_currency
            # widget_data = list(widget_data)
            #print layout in cmd
            # print("======layout===")
            # pprint.pprint(all_data['layout'])
            if not widget_data:
                message = "No data found"
                status = 400
                payload = message
            else:
                parsed_data, parameter_data = [], []
                dataSet = {}
                combined_dataSet = all_data['data']['dataSet']
                single_chart_details = all_data['data']['dataDetails']
                for w_data in widget_data:
                    temp, chartData, parameter_dimension = {}, {}, {}
                    temp['userStory'] = w_data.get('userStory', None)
                    temp['widgetName'] = w_data.get('widgetName',None)
                    temp['widgetId'] = str(w_data.get('_id',None))
                    chartData = w_data.get("chartData", [])
                    if type(chartData) == list:
                        chartData = chartData[0]
                    else:
                        chartData = chartData
                    charts_suggested = chartData.get("recommendedCharts", [])
                    temp['recommendedCharts'] = charts_suggested
                    temp['theme'] = workspace_data.get('theme',{})
                    parameter_dimension['Suggested_Chart'] = "-"
                    parameter_dimension['widgetId'] = temp['widgetId']
                    if charts_suggested:
                        if 'chartType' in charts_suggested[0]:
                            parameter_dimension['Suggested_Chart'] = charts_suggested[0]['chartType']
                    parameter_dimension['Statement'] = w_data.get('userStory', None)
                    dimension = chartData.get('chartDimension', [])
                    if not dimension:
                        dimension = {"Categorical Dimension": [], "Goal Measure": [], "Measure": [], "Timeline Dimension": []}
                    dimension = {key.replace(" ","_") : ", ".join(value) if value else "-" for key, value in dimension.items()}
                    #pausing temp for dimension usuage starts
                    if chartData.get('categories', []):
                        for cat in chartData['categories']:
                            if cat['elementName'] not in dimension['Categorical_Dimension'].replace("-","").split(", "):
                                cat_dimension = {"Categorical_Dimension" : ", ".join([dimension["Categorical_Dimension"], cat['elementName']]).replace("-, ","")}
                                dimension.update(cat_dimension)
                            else: pass
                    # if chartData.get('variables', []):
                    #     for var in chartData['variables']:
                    #         if var['variableName'] not in (dimension['Measure'].replace("-","") + dimension['Goal_Measure'].replace("-","")).split(", "):
                    #             var_dimension = {"Measure" : ", ".join([dimension["Measure"], var['variableName']]).replace("-, ","")}
                    #             dimension.update(var_dimension)
                    #         else: pass
                    #pausing temp for dimension usuage ends
                    parameter_dimension.update(dimension)
                    parsed_data.append(temp)
                    parameter_data.append(parameter_dimension)
                    # chartTitleData =  "Categorical Dimension : " +  dimension["Categorical_Dimension"], "Goal Measure : " + dimension["Goal_Measure"], "Measure : " + dimension["Measure"], "Timeline Dimension : " + dimension["Timeline_Dimension"]
                    if dimension["Categorical_Dimension"] == "-" :
                        dimension["Categorical_Dimension"] = ""
                    if dimension["Goal_Measure"] == "-" :
                        dimension["Goal_Measure"] = ""
                    if dimension["Measure"] == "-" :
                        dimension["Measure"] = ""
                    if dimension["Timeline_Dimension"] == "-" :
                        dimension["Timeline_Dimension"] = ""
                    if dimension["Timeline_Dimension"] == "Y" :
                        dimension["Timeline_Dimension"] = "Year"
                    if dimension["Timeline_Dimension"] == "M" :
                        dimension["Timeline_Dimension"] = "Month"
                    if dimension["Timeline_Dimension"] == "D" :
                        dimension["Timeline_Dimension"] = "Day"
                    for value in dimension:
                        dimension[value] = list(set(dimension[value].split(', ')))
                        dimension[value] = ', '.join(dimension[value])
                    if len(dimension["Timeline_Dimension"])>=1:
                        chartTitleData =  dimension["Categorical_Dimension"] + dimension["Goal_Measure"] + dimension["Measure"] +" By " +dimension["Timeline_Dimension"]
                    if len(dimension["Timeline_Dimension"]) and len(dimension["Categorical_Dimension"]) == 0:
                        chartTitleData = dimension["Goal_Measure"] + dimension["Measure"]
                    else:
                        chartTitleData =   dimension["Goal_Measure"] + dimension["Measure"] + " By "  +dimension["Categorical_Dimension"] 
                    if chartTitleData.replace(" ", "") == "By":
                        chartTitleData = parameter_dimension["Statement"]
                    # parameter_dimension["chartTitleData"] = ' ,'.join([str(elem) for elem in chartTitleData])
                    parameter_dimension["chartTitleData"] = chartTitleData
                    # parameter_dimension["chartTitleData"] = "Categorical Dimension : " + str(dimension["Categorical_Dimension"]), "Goal Measure : " + str(dimension["Goal_Measure"]), "Measure : " + str(dimension["Measure"]), "Timeline Dimension : " + str(dimension["Timeline_Dimension"])
                filtered_data, res_dict = {}, {}
                ## Adding user currency to dataset
                combined_dataSet["userCurrency"] = user_currency
                ## checking if currency for variables
                variables_list = single_chart_details.get('variables', [])
                isCurrencyList = []
                if variables_list:
                    isCurrencyList = [variable['variableName'] for variable in variables_list if variable["isCurrency"]]
                #isCurrency added in alldata starts
                isCurrencyValue = []
                columnDataType = []
                if variables_list:
                    isCurrencyValue = [{'name' : i['variableName'], 'isCurrency' : i['isCurrency']} for i in variables_list]
                    for i in variables_list:
                        i['variableInnerType'] = i['variableInnerType'] if 'variableInnerType' in i else 'int'
                    columnDataType = [{'name' : i['variableName'], 'variableInnerType' : i['variableInnerType'],'dataType' : i['dataType'], 'isCurrency' : i['isCurrency'], 'symbol' : '%' if i['dataType'] == 'percent' else '$' if i['isCurrency'] == True else '' if i['dataType'] == 'ratio' else False } for i in variables_list]
                #isCurrency added in alldata ends
                combined_dataSet["currencyColumns"] = isCurrencyList
                workspace_categories =  single_chart_details['categories']
                single_dataset = combined_dataSet['tableData']
                #
                #
                category_names = [cat['elementName'] for cat in workspace_categories]
                # if single_chart_details['periodicity']:
                #     category_names.append("Period")
                fil_period_dict = {}
                if single_chart_details['periodicity']:
                    period_freq = single_chart_details['periodicity']['frequency']
                    unique_periods = list({v['Period'] for v in single_dataset})
                    if period_freq == 'M':
                        # unique_periods = list(map(lambda x: datetime.datetime.strptime(x, "%b-%Y").strftime('%d-%m-%Y'), unique_periods))
                        unique_periods = sorted(unique_periods, key = lambda x: datetime.strptime(x, "%b-%Y"))
                    elif period_freq == 'Y':
                        # unique_periods = list(map(lambda x: datetime.datetime.strptime(x, "%Y").strftime('%d-%m-%Y'), unique_periods))
                        unique_periods = sorted(unique_periods, key=lambda x: datetime.strptime(x, "%Y"))
                    elif period_freq == 'D':
                        # unique_periods = list(map(lambda x: datetime.datetime.strptime(x, "%Y").strftime('%d-%m-%Y'), unique_periods))
                        unique_periods = sorted(unique_periods, key=lambda x: datetime.strptime(x, "%Y-%m-%d"))
                    fil_period_dict['Period'] = unique_periods
                    fil_period_dict['PeriodType'] = period_freq
                    fil_period_range = all_data.get('filteredData',{}).get('filteredSubCategories',{}).get('Period',{})
                    if fil_period_range:
                        fil_period_dict['periodRange'] = [fil_period_range[0], fil_period_range[-1]]
                    else:
                        fil_period_dict['periodRange'] = [unique_periods[0], unique_periods[-1]]
                sub_categories = {}
                for catname in category_names:
                    i_set = set()
                    for i in single_dataset:
                        i_set.add(i[catname])
                    sub_categories[catname] = sorted(list(i_set))
                res_dict['categories'] = category_names
                res_dict['subcatagories'] = sub_categories
                res_dict['periodFilters'] = fil_period_dict
                if 'filteredData' in all_data.keys():
                    filtered_data = all_data['filteredData']
                ##########################
                #code for isCurrency starts 
                if len(parsed_data[0]['recommendedCharts']) >=1:
                    if (parsed_data[0]['recommendedCharts'][0]['chartType']) == "numberTile":
                        # isCurreny = bool([pd.Series(charts_suggested[0]['xAxisDataKeys']).isin(combined_dataSet['currencyColumns']).any()])
                        if len(combined_dataSet['currencyColumns']) == 0:
                                isCurrency = bool(False)
                                parsed_data[0]['recommendedCharts'][0]['isCurrency'] = isCurrency
                                combined_dataSet['isCurrency'] = isCurrency
                        if len((charts_suggested))>=1:
                            if len((parsed_data[0]['recommendedCharts'][0]['xAxisDataKeys']))>=1 and len(combined_dataSet['currencyColumns']) >=1:
                                #to change start
                                #if len((charts_suggested[0]['xAxisDataKeys']))>=1 and len(combined_dataSet['currencyColumns']) >=1:
                                #to change ends
                                # isCurrency_ =  all(x in (charts_suggested[0]['xAxisDataKeys']) for x in (combined_dataSet['currencyColumns']))
                                isCurrency_ = set((parsed_data[0]['recommendedCharts'][0]['xAxisDataKeys'])).issubset(combined_dataSet['currencyColumns'])
                                isCurrency_ = str(isCurrency_)
                                if isCurrency_ == "True":
                                    print("Ture in all data")
                                    isCurrency= bool(True)
                                if isCurrency_ == "False":
                                    isCurrency = bool(False)
                                    print("False in all data")
                                for i in parsed_data[0]['recommendedCharts']:
                                    if i['chartType'] == "numberTile":
                                        i['isCurrency'] = isCurrency
                                combined_dataSet['isCurrency'] = isCurrency
                    ######################
                    #Tabular chart sorting starts 
                    if len(parsed_data[0]['recommendedCharts']) >=1:
                        if (parsed_data[0]['recommendedCharts'][0]['chartType']) == "tabularChart":
                            aggregated_dataset = parsed_data[0]['recommendedCharts'][0]['aggregatedDataset']
                            dimension = parameter_data[0]
                            chart_viewer = ChartViewer()
                            #to group by the tabular chart - starts
                            # grup_aggregated_dataset_df = pd.DataFrame(aggregated_dataset)
                            # if 'Period' in grup_aggregated_dataset_df:
                            #     grup_aggregated_dataset_df= grup_aggregated_dataset_df.groupby(['Period']).mean()
                            # if 'Vertical' in grup_aggregated_dataset_df:
                            #     grup_aggregated_dataset_df= grup_aggregated_dataset_df.groupby(['Vertical']).mean()
                            # if 'Account' in grup_aggregated_dataset_df:
                            #     grup_aggregated_dataset_df= grup_aggregated_dataset_df.groupby(['Account']).mean()
                            # if 'Verical' in grup_aggregated_dataset_df:
                            #     grup_aggregated_dataset_df= grup_aggregated_dataset_df.groupby(['Verical']).mean()
                            # if 'Region' in grup_aggregated_dataset_df:
                            #     grup_aggregated_dataset_df= grup_aggregated_dataset_df.groupby(['Region']).mean()
                            # aggregated_dataset = grup_aggregated_dataset_df.to_dict("list")
                            # parsed_data[0]['recommendedCharts'][0]['aggregatedDataset'] = aggregated_dataset
                            # print("================aggregated_dataset after gruping======================")
                            # print(aggregated_dataset)
                            #to group by the tabular chart - ends

                            #new dimension - due to grouping starts
                            text = ' '.join(i for i in aggregated_dataset.keys())
                            parse_story = ParserUserStory()
                            dimension_1 = parse_story.text_to_structure(text, currency)
                    ##############
                    #for loop - tabular chart starts
                    for i in parsed_data:
                        if len(i['recommendedCharts'])>=1 :
                            #Adding percent in each - starts
                            for percent in i['recommendedCharts']:
                                # i['percent'] ="%"
                                percent['percent'] = "%"
                            #Adding percent in each - ends
                            if i['recommendedCharts'][0]['chartType'] == 'tabularChart':
                                aggregated_dataset = i['recommendedCharts'][0]['aggregatedDataset']
                                #rounding aggregated dataset to 2 starts
                                df = pd.DataFrame(i['recommendedCharts'][0]['aggregatedDataset']).round(2)
                                df = df.to_dict("list")
                                i['recommendedCharts'][0]['aggregatedDataset'] = df
                                #rounding aggregated dataset to 2 ends
                                # aggregated_dataset = np.round(aggregated_dataset, decimals = 2)
                                text = ' '.join(i for i in aggregated_dataset.keys())
                                if len(text) == 0:
                                    text = ' '.join(i for i in i['recommendedCharts'][0]['xAxisDataKeys']+ i['recommendedCharts'][0]['yAxisDataKeys'])
                                parse_story = ParserUserStory()

                                currency = "$"
                                dimension_1 = parse_story.text_to_structure(text, currency)
                                # dimension_1 = dimension'
                                if len(dimension.values())>=1:
                                    dimension_1 = {'Categorical_Dimension' : dimension_1[0][0]['chartDimension']['Categorical Dimension'],
                                                    'Goal_Measure' : dimension_1[0][0]['chartDimension']['Goal Measure'],
                                                    'Measure' : dimension_1[0][0]['chartDimension']['Measure'],
                                                    'Timeline_Dimension' : dimension_1[0][0]['chartDimension']['Timeline Dimension']
                                                }
                                if len(dimension.values())==0: 
                                    dimension_1 = {'Categorical_Dimension' : ', '.join(i for i in dimension_1[0][0]['chartDimension']['Categorical Dimension']),
                                                    'Goal_Measure' : ', '.join(i for i in dimension_1[0][0]['chartDimension']['Goal Measure']),
                                                    'Measure' : ', '.join(i for i in dimension_1[0][0]['chartDimension']['Measure']),
                                                    'Timeline_Dimension' : ', '.join(i for i in dimension_1[0][0]['chartDimension']['Timeline Dimension'])
                                                }
                                chart_viewer = ChartViewer()
                                aggregated_dataset_order = chart_viewer.get_tabular_chart_sort(dimension_1, aggregated_dataset)
                                i['recommendedCharts'][0]['aggregated_dataset_order'] = aggregated_dataset_order
                            # print("its tabular chart - yes")
                        else:
                            pass
                            # print("its tabular chart - no")
                    #for loop - tabular chart ends
                    ##############

                    #Tabular chart sorting ends
                    ######################
                ##################################################################
                #code for categories adding a period starts
                if 'Period' in res_dict['periodFilters']:
                    if (len(res_dict['periodFilters']['Period'])) >= 1:
                        res_dict['categories' ].append('Period')
                #code for categories adding a period starts
                ##################################################################
                ########
                #for arranging copy paste data - starts
                for i in parsed_data:
                    if "duplicateWidgetIndex" in i["recommendedCharts"]:
                        if "duplicateWidgetIndex" in i["recommendedCharts"][0] and "duplicateWidgetIndex" is not None:
                            duplicateWidgetIndex = (i["recommendedCharts"][0]["duplicateWidgetIndex"])
                            duplicateWidgetId= i["widgetId"]
                            parsed_data[duplicateWidgetIndex], parsed_data[-1] = parsed_data[-1], parsed_data[duplicateWidgetIndex]
                            duplicateWidgetIndex = None
                            widget_db.update_many({"widgetId" : duplicateWidgetId}, {"$set" : {'duplicateWidgetIndex' : i["recommendedCharts"][0]["duplicateWidgetIndex"]}}) 
                #for chartTitleData in chartData starts
                for i in parameter_data:
                    for j in parsed_data:
                        if j["widgetId"] == i["widgetId"]:
                            j["chartTitleData"] = i["chartTitleData"] 
                #for chartTitleData in chartData ends
                #for arranging copy paste data - ends
                ########
                ##for arranging parameter data start

                parameter_data_1 = {"paramsData" : []}
                for i in parameter_data:
                    parameter_data_2 = { "Statement" : parameter_data[0]["Statement"], "Categorical Dimension"  : parameter_data[0]["Categorical_Dimension"],  "Suggested Chart" : parameter_data[0]["Suggested_Chart"], 
                                  "Measure" : parameter_data[0]["Measure"], "Goal Measure" : parameter_data[0]["Goal_Measure"],
                                  "Timeline Dimension" : parameter_data[0]["Timeline_Dimension"], "chart Title Data" : parameter_data[0]["chartTitleData"],  "widgetId" : parameter_data[0]["widgetId"]  }
                    # parameter_data_2 = OrderedDict.fromkeys(parameter_data_2)
                    parameter_data_1["paramsData"].append(parameter_data_2)
                ##for arranging parameter data ends

                #for NonUniform lenght starts
                tableDataDetails = all_data['data']['dataDetails']
                for i in tableDataDetails['categories']:
                    if 'nonUniformElements' in i: # added for error for NonUniform error and Parameter.
                        if i['nonUniformElements'] != None:
                            if len(i['nonUniformElements'][0]['elements'])>=1:
                                i['nonUniformElements'][0]['nonUniformLength'] = len(i['nonUniformElements'][0]['elements'])
                #for NonUniform lenght ends
                #for suggestions starts
                # suggestions_input = all_data['data']['dataSet']['tableData']
                # if len(suggestions_input)>1:
                #     suggestions_out  = suggestions.Suggestions.get_suggestions(suggestions_input)
                # if len(suggestions_input)<=1:
                #     suggestions_out  = "None"
                #changed in allDataset also / if error remove from there also. 
                #for suggestions ends
                #rounding the value of Dataset window to 4 starts
                combined_dataSet_df = pd.DataFrame(combined_dataSet['tableData'])
                combined_dataSet_df = np.round(combined_dataSet_df, decimals = 4)
                combined_dataSet['tableData'] = combined_dataSet_df.to_dict('records')
                #rounding the value of Dataset window to 4 ends
                #multiple dashboard variable starts
                subDashData = []
                subDashDataList = []
                # subDashDataList.append({"workspaceId" : workspace_id, "workspaceName" : workspace_data["workspaceName"],"workspaceType" : workspace_data["workspaceType"],"currencies" : workspace_data["currencies"] , "theme" : workspace_data["theme"]})
                for q in workspace_db.find({}):
                    q["_id"] = str(q["_id"])
                    q['workspaceId'] = str(q["_id"])
                    subDashData.append(q)
                subDashData.reverse()
                for data in subDashData:
                    if "subDashboard" in data:
                        if data['subDashboard'] == True:
                            if data['inputSubDash']['mainworkspaceID'] == str(workspace_data["_id"]):
                                subDashDataDict = {"workspaceName" :data['workspaceName'],
                                                    "workspaceID" : data['_id'],
                                                    "mainworkspaceName" : workspace_data["workspaceName"],
                                                    "mainworkspaceID" : str(workspace_data["_id"])}
                                subDashDataList.append(subDashDataDict)
                #multiple dashboard variable ends
                #new variable - mainDashDetails -helps only for frontend to get the mainDashDetails starts
                mainDashDetails = {"workspaceId" : workspace_id, "workspaceName" : workspace_data["workspaceName"],"workspaceType" : workspace_data["workspaceType"],"currencies" : workspace_data["currencies"] , "theme" : workspace_data["theme"]}
                #new variable - mainDashDetails -helps only for frontend to get the mainDashDetails ends
                allDataSet = {"workspaceId" : workspace_id, "workspaceName" : workspace_data["workspaceName"],"workspaceType" : workspace_data["workspaceType"],"currencies" : workspace_data["currencies"] , "theme" : workspace_data["theme"], "singleDataSet" : combined_dataSet, "chartData" : parsed_data, "filteredData":filtered_data,
                                "filterOptions" : res_dict, "parameterData" : parameter_data, "layout" : all_data.get('layout',[]), 'isCurrencyValue' : isCurrencyValue, 'columnDataType' : columnDataType, "subDashDataList": subDashDataList, "mainDashDetails"  : mainDashDetails}
                # converted_chartData = ((allDataSet['chartData'])[1:-1])

                #Adding new aggregated dataset for testing the ploty (Will disable this in future) starts
                # allDataSet['aggregatedDatasetTesting'] = []
                for eachChart in parsed_data:
                    x = eachChart['recommendedCharts'][0]['xAxisDataKeys']
                    y = eachChart['recommendedCharts'][0]['yAxisDataKeys']
                    aggregatedDataset = eachChart['recommendedCharts'][0]['aggregatedDataset']
                    print("=====x======")
                    pprint.pprint(x)
                    print("======y=====")
                    pprint.pprint(y)
                    print("======aggregatedDataset=====")
                    pprint.pprint(aggregatedDataset)
                    yValue = [{i : aggregatedDataset[i]} for i in y]
                    xValue = [{i : aggregatedDataset[i]} for i in x]
                    aggregatedDatasetTesting = { 
                                                    "x" : xValue,
                                                    "y" : yValue
                                                }
                    eachChart['recommendedCharts'][0]["aggregatedDatasetTesting"] = aggregatedDatasetTesting
                    # allDataSet['aggregatedDatasetTesting'].append(aggregatedDatasetTesting)
                #Adding new aggregated dataset for testing the ploty (Will disable this in future) ends
                
                all_data_db.insert_one({"workspaceId" : workspace_id, 'allData': allDataSet})
                all_data_db.update_many({"workspaceId" : workspace_id}, {"$set" : {'allData' : allDataSet}})
                payload = allDataSet 
                # print("=====================payload==========================")
                # pprint.pprint(payload)     
                message = "Data Fetched"
                is_success = True
                status = 200
            
            response['message'] = message
            response['payload'] = payload
            response['success'] = is_success
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))
class CopyPaste(Resource):
    # @app.route('/copypaste', methods=['POST'])
    def post(self):
        '''
        It is to get the copy paste details / Duplicate Chart
        
        '''
        message = "No yet started"
        status = 200
        payload = message
        is_success = False
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success = 400, False
            workspace_db = mydb["workspace"]
            all_data_db = mydb["all_data_db"]
            copypaste_db = mydb["copypaste_db"]
            widget_db = mydb["widget"]
            data_db = mydb["data"]
            input_json = request.json
            if not input_json:
                payload = 'No payload found'      
                message = payload
                is_success = False
                status = 400
            else:
                widget_id = input_json.get("widgetId",None)
                index = input_json.get("index", None)
                widget_data = widget_db.find_one({"_id":ObjectId(f"{widget_id}")})
                workspace_id = widget_data['workspaceId']
                all_data_details = all_data_db.find_one({"workspaceId": workspace_id})
                workspace_data = workspace_db.find_one({"_id":ObjectId(f"{workspace_id}")})
                user_story  = workspace_data["userStory"]
                copypaste_data = {}
                for i in all_data_details['allData']['chartData']:
                    if widget_id == i['widgetId']:
                        temp = {}
                        copypaste_data = i
                    else:
                        print("widget id not present")
                #forming the new widget and getting the Id starts
                widget_db.insert_one({'workspaceId': workspace_id,
                                      'widgetName': all_data_details['allData']['chartData'][index[0]]['widgetName'],
                                      'userStory': all_data_details['allData']['chartData'][index[0]]['userStory'], 
                                      'chartData': widget_data['chartData'], 
                                      'currencies': widget_data['currencies']})
                widget_new_Id = widget_db.find({"workspaceId" :workspace_id })
                for w_data in widget_new_Id:
                    temp = {}
                    temp['widgetId'] = str(w_data.get('_id',None))
                #forming the new widget and getting the Id ends 
                
                copypaste_data_ = {'recommendedCharts':copypaste_data['recommendedCharts'],
                                'theme' : copypaste_data['theme'],
                                'userStory': copypaste_data['userStory'],
                                'widgetId': temp['widgetId'],
                                'widgetName' : copypaste_data['widgetName']
                }
                ids = []
                for i in all_data_details['allData']['chartData']:
                    id = i['widgetId']
                    id = ids.append(id)
                widget_data['chartData']['recommendedCharts'][0]['duplicateWidgetIds'] = ids
                duplicateWidgetIndex = index[0]
                widget_data['chartData']['recommendedCharts'][0]['duplicateWidgetIndex'] = duplicateWidgetIndex
                myquery = {"_id" : ObjectId(f"{temp['widgetId']}")}
                newvalues = { "$set": {"chartData": widget_data['chartData']}}
                widget_db.update_one(myquery, newvalues)

                all_data_details['allData']['chartData'].insert(index[0],copypaste_data_)
                # all_data_db.update_one({'workspaceId' : workspace_id}, {"$set" : {"chartData": all_data_details['allData']['chartData'] }})
                all_data_db.update_many({'workspaceId' : workspace_id}, {"$set" : {"chartData": all_data_details['allData']['chartData'] }})
                #  layout starts
                chart_layout_dict, chart_layout_array = {"i": "", "x": 0, "y": 0, "w": 4, "h": 10, "minW": 4, "minH": 9, "maxH": 10}, []
                layout_x = [0,4,8]
                single_dataSet = data_db.find_one({"workspaceId" : workspace_id})
                chart_layout_array = single_dataSet.get('layout', [])
                single_dataSet = single_dataSet['data']['dataSet']
                chart_viewer = ChartViewer()
                currency = "$"
                final_chart_out = []
                chart_dict = {}
                chart_data = all_data_details['allData']['chartData'][index[0]]
                dimension = []
                aggregated_dataset, aggregated_function,dimension = chart_viewer.get_chart_view(chart_data, single_dataSet, dimension)
                chart_data['aggregatedDataset'] = aggregated_dataset
                chart_data['aggregationType'] = aggregated_function
                chart_data['userStory'] = copypaste_data['widgetName']
                chart_data['currencies'] = currency
                final_chart_out.append(chart_data)
                chart_dict['recommendedCharts'] = final_chart_out
                if chart_layout_array:
                    i = len(chart_layout_array)
                    layout = chart_layout_dict.copy()
                    layout["i"] = list(string.ascii_lowercase)[i]
                    layout["x"] = layout_x[i%3]
                    if chart_dict["recommendedCharts"][0]['recommendedCharts'][0]['chartType'] in ["numberTile","kpiTile"] or chart_dict["recommendedCharts"] in ["numberTile","kpiTile"]:
                        print("========yes its numberTile or KPITILE=======")
                        if (chart_dict["recommendedCharts"][0]['recommendedCharts'][0]['chartType'])in ["numberTile","kpiTile"]:
                            layout["h"] = 5
                            layout["minH"] = 5
                    chart_layout_array.append(layout)
                data_db.update_one({'workspaceId' : workspace_id}, {"$set" : {"layout": chart_layout_array}})
                # layout ends
                #######################################################################
                updated_copypaste_data ={ "chartData" : all_data_details['allData']['chartData'], "layout" : chart_layout_array}
            status, is_success = 200, True
            message = "success"
            response['message'] = message
            response['success'] = is_success
            response['payload'] = updated_copypaste_data   
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))
    
class ChartSetting(Resource):
    # @app.route('/chart_settings', methods=['POST'])
    def post(self):
        '''
        It is to store the user chart settings
        
        '''
        message = "No yet started"
        status = 200
        payload = message
        is_success = False
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success = 400, False
            widget_db = mydb["widget"]
            data_db = mydb["data"]
            json_data = request.json
            input_ = json_data.get('payload',{})
            #check for input
            if not input_:
                payload = 'No payload found'      
                message = payload
                is_success = False
                status = 400
            else:
                chart_settings = input_.get("chartSettings",None)
                widget_id = input_.get("widgetId",None)
                #check for settings and widget id
                if not chart_settings or not widget_id:
                    payload = 'Chart settings and widget id'    
                    message = payload
                    is_success = False
                    status = 400 
                else:
                    pass
                    
                # "storedChartSettings":{
                # }
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

    def put(self):
        response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
        try:
            status, is_success = 400, False
            mycol = mydb["workspace"]
            workspace = request.json
            _id = workspace.get('workspaceId', None)
            workspace_theme = workspace.get('theme', None)
            workspace_data = mycol.find_one({"_id":ObjectId(f"{_id}")})
            if not workspace_data:
                message = "Workspace does not exist"
            else:
                mycol.update_one({
                    '_id': ObjectId(f"{_id}")
                        },{"$set": {"theme": workspace_theme}})
                message = "Updated Successfully"
                    
            status, is_success = 200, True
            response['message'] = message
            response['success'] = is_success
            response['payload'] = message
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

class DrillDown(Resource):
    # @app.route('/drillDown', methods=['POST'])
    def post(self):
        '''
        It is to display drill down
        
        '''
        message = "No yet started"
        status = 200
        payload = message
        is_success = False
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success = 400, False
            widget_db = mydb["widget"]
            data_db = mydb["data"]
            all_data_db = mydb["all_data_db"]
            drill_down_db = mydb["drill_down_db"]
            input_ = request.json
            #inputs starts
            widget_id = input_.pop('widgetId')
            workspace_id = input_.get("workspaceId")
            hierarchicalOrder = input_.get('hierarchicalOrder', None)
            drillDownUnique = input_.get('drillDownUnique', None)
            drillDownUniqueXaxis = input_.get('drillDownUniqueXaxis', None)
            all_data = data_db.find_one({"workspaceId" :workspace_id})
            #inputs ends
            if not widget_id:
                    payload = 'widget Id not found'      
                    message = payload
                    is_success = False
                    status = 400  
            else:
                widget_data = widget_db.find_one({"_id" : ObjectId(f"{widget_id}")})
                if not widget_data:
                    payload = 'widget data not found'      
                    message = payload
                    is_success = False
                    status = 400 
                else:
                    workspace_id = widget_data.get('workspaceId', None)
                    chart_data = widget_data.get('chartData',None)
                    periodData = chart_data.get('periodicity', None)
                    currency = '$'
                    combined_dataSet = all_data['data']['dataSet']
                    combined_dataSet = combined_dataSet['tableData']
                    aggregated_dataset = widget_data["chartData"]["recommendedCharts"][0]["aggregatedDataset"]
                    # print("============aggregated_dataset=================")
                    # pprint.pprint(aggregated_dataset)
                    agg_dataset = aggregated_dataset
                    # print("================combined_dataSet=====================")
                    # pprint.pprint(combined_dataSet)
                    #combined dataset df
                    combined_dataSet_df = pd.DataFrame(combined_dataSet)
                    # print("==============combined_dataSet_df===============")
                    # print(combined_dataSet_df)
                    aggregated_dataset_df = pd.DataFrame(aggregated_dataset)
                    # print("==============aggregated_dataset_df===============")
                    # print(aggregated_dataset_df)

###################################################
                    input_["xAxisDataKeys"] = hierarchicalOrder
                    input_["yAxisDataKeys"] = (widget_data["chartData"]["recommendedCharts"][0]["yAxisDataKeys"])
                    # input_["yAxisDataKeys"] = hierarchicalOrder
     
                    #output from chart generator agg dataset starts
                    chart_viewer = ChartViewer()
                    aggregated_function = "sum"
                    #code for drilldown hierarchicalOrder benna and gnana idea starts
                    output = []
                    if len(hierarchicalOrder) >=1:
                        #first level
                        if hierarchicalOrder[0]: 
                            input_["xAxisDataKeys"] = hierarchicalOrder[0]
                            input_["yAxisDataKeys"] = (widget_data["chartData"]["recommendedCharts"][0]["yAxisDataKeys"][0])
                            dataframe = (combined_dataSet_df.loc[:,[ input_["xAxisDataKeys"]  ,  input_["yAxisDataKeys"]  ]  ])
                            agg_out = chart_viewer.get_aggregated_data(dataframe,  hierarchicalOrder[0],  widget_data["chartData"]["recommendedCharts"][0]["yAxisDataKeys"], aggregated_function)
                            agg_out = agg_out.to_dict("list")
                            output_drilldownlevelone = { "drilldownlevelone" : {
                                "drillDownDataset" : agg_out,
                                "xAxisDataKeys" : [input_["xAxisDataKeys"]],
                                "yAxisDataKeys" : [input_["yAxisDataKeys"]]
                                        }
                                    }
                            output.append(output_drilldownlevelone)

                        if len(hierarchicalOrder)>=2: 
                            input_["xAxisDataKeys"] = hierarchicalOrder[1]
                            input_["yAxisDataKeys"] = (widget_data["chartData"]["recommendedCharts"][0]["yAxisDataKeys"][0])
                            dataframe = (combined_dataSet_df.loc[:,[ input_["xAxisDataKeys"]  ,  input_["yAxisDataKeys"]  ]  ])
                            agg_out = chart_viewer.get_aggregated_data(dataframe,  hierarchicalOrder[1],  widget_data["chartData"]["recommendedCharts"][0]["yAxisDataKeys"], aggregated_function)
                            agg_out = agg_out.to_dict("list")
                            output_drilldownleveltwo = { "drilldownleveltwo" : {
                                "drillDownDataset" : agg_out,
                                "xAxisDataKeys" : [input_["xAxisDataKeys"]],
                                "yAxisDataKeys" : [input_["yAxisDataKeys"]]
                                        }       
                                    }
                            output.append(output_drilldownleveltwo)
                        if len(hierarchicalOrder)>=3:
                            input_["xAxisDataKeys"] = hierarchicalOrder[2]
                            input_["yAxisDataKeys"] = (widget_data["chartData"]["recommendedCharts"][0]["yAxisDataKeys"][0])
                            dataframe = (combined_dataSet_df.loc[:,[ input_["xAxisDataKeys"]  ,  input_["yAxisDataKeys"]  ]  ])
                            agg_out = chart_viewer.get_aggregated_data(dataframe,  hierarchicalOrder[2],  widget_data["chartData"]["recommendedCharts"][0]["yAxisDataKeys"], aggregated_function)
                            agg_out = agg_out.to_dict("list")
                            output_drilldownlevelthree = { "drilldownlevelthree" : {
                                "drillDownDataset" : agg_out,
                                "xAxisDataKeys" : [input_["xAxisDataKeys"]],
                                "yAxisDataKeys" : [input_["yAxisDataKeys"]]       
                                    }
                            }
                            output.append(output_drilldownlevelthree)
                            pprint.pprint(output)
                        drill_down_db.insert_one({"workspaceId" : workspace_id, "data" :output })
                        if len(drillDownUnique) >=1 :
                            drill_down_data = drill_down_db.find_one({'workspaceId' : workspace_id})
                            input_["xAxisDataKeys"] = drillDownUniqueXaxis[0]
                            input_["yAxisDataKeys"] = (widget_data["chartData"]["recommendedCharts"][0]["yAxisDataKeys"][0])
                            dataframe = (combined_dataSet_df.loc[:,[ input_["xAxisDataKeys"]  ,  input_["yAxisDataKeys"]  ]  ])
                            string_data = str(drillDownUnique[0])
                            final_out = dataframe.loc[dataframe[hierarchicalOrder[0]] == string_data ]
                            final_out_dict = { "drillDownDatasetUnique" : final_out.to_dict("list"),
                                          "xAxisDataKeys" : [input_["xAxisDataKeys"]],
                                          "yAxisDataKeys" : [input_["yAxisDataKeys"]]
                                        }
                            output.append(final_out_dict)
                            drill_down_data["data"][0]["drillDownDatasetUnique"] = final_out.to_dict("list")
                            drill_down_data["data"][0]["drillDownDatasetUnique"]["xAxisDataKeys"] = input_["xAxisDataKeys"]
                            drill_down_data["data"][0]["drillDownDatasetUnique"]["yAxisDataKeys"] = input_["yAxisDataKeys"]
                            drill_down_db.update_many({'workspaceId' : workspace_id},{"$set" : {"data": drill_down_data["data"]}})
                    #code for drilldown hierarchicalOrder benna and gnana idea ends
                    
                    message  = "success"
                    payload = output
                    is_success = True
                    status = 200
            response['message'] = message
            response['payload'] = payload
            response['success'] = is_success
        except ValueError as exc:
            response['message'] = exc.__str__()
            status = 400
        except:
            print(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))


class NewChart(Resource):
    # @app.route('/newChart', methods=['POST'])
    def post(self):
        '''
        It is to get new chart
        
        '''
        message = "No yet started"
        status = 200
        payload = message
        is_success = False
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success = 400, False
            widget_db = mydb["widget"]
            data_db = mydb["data"]
            all_data_db = mydb["all_data_db"]
            input_ = request.json
            #check for input
            if not input_:
                payload = 'No payload found'      
                message = payload
                is_success = False
                status = 400
            else:
                data_db = mydb["data"]
                widget_id = input_.pop('widgetId')
                # hierchicalOrder = input_.get('hierchicalOrder', None)
                if not widget_id:
                    payload = 'widget Id not found'      
                    message = payload
                    is_success = False
                    status = 400  
                else:
                    widget_data = widget_db.find_one({"_id" : ObjectId(f"{widget_id}")})
                    if not widget_data:
                        payload = 'widget data not found'      
                        message = payload
                        is_success = False
                        status = 400 
                    else:
                        workspace_id = widget_data.get('workspaceId', None)
                        chart_data = widget_data.get('chartData',None)
                        # currency = '$'
                        if chart_data:
                            periodData = chart_data.get('periodicity', None)
                            if chart_data.get('recommendedCharts', None):
                                if 'currencies' in chart_data['recommendedCharts']:
                                    currency = chart_data['recommendedCharts'][0]['currencies']
                                else:
                                    currency = '$'
                        fetched_data = data_db.find_one({"workspaceId" : workspace_id})
                        struct = fetched_data['data']

                        # user_story = struct.get('userStory', None)
                        random_data = struct['dataSet']
                        chart_viewer = ChartViewer()
                        #dimension for get chart view only - used for tabular chart
                        dimension = widget_data['chartData']["chartDimension"]
                        aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(input_, random_data, periodData, dimension)
                        #######################
                        #if aggregated_dataset is none starts
                        all_data = data_db.find_one({"workspaceId" :  workspace_id})
                        combined_dataSet_ = all_data['data']['dataSet']['tableData']
                        combined_dataSet_df_ = pd.DataFrame(combined_dataSet_)
                        if input_["chartType"] == "tabularChart":
                            single_dataSet = data_db.find_one({"workspaceId" : workspace_id})
                            single_dataSet = single_dataSet['data']['dataSet']
                            combined_dataset = single_dataSet["tableData"]
                            if len(aggregated_dataset)>= 1:
                                aggregated_dataset = aggregated_dataset
                            else:
                                combined_dataset_df = pd.DataFrame(combined_dataset)
                                a = (input_["yAxisDataKeys"])+(input_["xAxisDataKeys"])
                                combined_dataset_df = combined_dataset_df[a]
                                aggregated_dataset = combined_dataset_df.to_dict("list")
                        #if aggregated_dataset is none ends
                        #######################
                        #if aggregated_dataset list starts (inside sorting )
                        if type(aggregated_dataset) == list:
                            dframe = pd.DataFrame(aggregated_dataset)
                            one =  {x[0]: [y for y in x[1:] if not pd.isna(y)] for x in dframe.itertuples(index=True) } 
                            aggregated_dataset =  {dframe[column].name: [y for y in dframe[column] if not pd.isna(y)] for column in dframe}
                         #if aggregated_dataset list ends (inside sorting )
                        if input_['chartType'] == 'numberTile':
                            if len(input_['yAxisDataKeys']) >= 1:
                                input_['xAxisDataKeys'] = input_['yAxisDataKeys']
                                input_['yAxisDataKeys'] = []
                            else:
                                 input_['xAxisDataKeys'] = input_['xAxisDataKeys']
                            aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(input_, random_data, periodData, dimension)
                            if input_['xAxisDataKeys'][0] in variable_logos.keys():
                                input_['tileLogo'] = variable_logos[input_['xAxisDataKeys'][0]]
                        if input_["chartType"] == "pieChart":
                            aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(input_, random_data, periodData, dimension)
                        if isinstance(aggregated_dataset, str):
                            raise ValueError(aggregated_dataset)
                        input_['aggregatedDataset'] = aggregated_dataset
                        input_['currencies'] = currency
                        # input_['userStory'] = user_story
                        new_chart = widget_data['chartData']
                        new_chart['recommendedCharts'] = [input_]
                        if input_['chartType'] == 'numberTile':
                            new_chart['recommendedCharts'][0]['isCurrency'] = (new_chart['variables'][0]['isCurrency'])
                        if input_["chartType"] == "tabularChart":
                            print("==========the chart type is tabular chart=========")
                            if len(aggregated_dataset)>= 1:
                                aggregated_dataset = aggregated_dataset
                                aggregated_dataset_df = pd.DataFrame(aggregated_dataset)
                                # if 'Region' in aggregated_dataset_df:
                                #     aggregated_dataset_df= aggregated_dataset_df.groupby(['Region'], as_index=False).mean()
                                # if 'Verical' in aggregated_dataset_df:
                                #     aggregated_dataset_df= aggregated_dataset_df.groupby(['Verical'],  as_index=False).mean()
                                # if 'Vertical' in aggregated_dataset_df:
                                #     aggregated_dataset_df= aggregated_dataset_df.groupby(['Vertical'], as_index=False).mean()
                                # if 'Account' in aggregated_dataset_df:
                                #     aggregated_dataset_df= aggregated_dataset_df.groupby(['Account'], as_index=False).mean()
                                # if 'Period' in aggregated_dataset_df:
                                #     aggregated_dataset_df= aggregated_dataset_df.groupby(['Period'], as_index=False).mean()
                                aggregated_dataset = aggregated_dataset_df.to_dict("list")
                                
                            if len(aggregated_dataset)==0 :
                                combined_dataset = single_dataSet["tableData"]
                                combined_dataset_df = pd.DataFrame(combined_dataset)
                                a = (input_["yAxisDataKeys"])+(input_["xAxisDataKeys"])
                                combined_dataset_df = combined_dataset_df[a]
                                if 'Region' in combined_dataset_df:
                                    combined_dataset_df= combined_dataset_df.groupby(['Region'], as_index=False).mean()
                                if 'Verical' in combined_dataset_df:
                                    combined_dataset_df= combined_dataset_df.groupby(['Verical'], as_index=False).mean()
                                if 'Vertical' in combined_dataset_df:
                                    combined_dataset_df= combined_dataset_df.groupby(['Vertical'], as_index=False).mean()
                                if 'Account' in combined_dataset_df:
                                    combined_dataset_df= combined_dataset_df.groupby(['Account'], as_index=False).mean()
                                if 'Period' in combined_dataset_df:
                                    combined_dataset_df= combined_dataset_df.groupby(['Period'], as_index=False).mean()
                                aggregated_dataset = combined_dataset_df.to_dict("list")
                            aggregated_dataset_df = pd.DataFrame(aggregated_dataset)

                            aggregated_dataset_keys = aggregated_dataset.keys()
                            # print("--------tabular_data_keys---------")
                            # print(tabular_data_keys)
                            # print(str(list(tabular_data_keys)))
                            tabular_data_keys = ((list(aggregated_dataset_keys)))
                            listToStr = ' '.join(map(str, tabular_data_keys))
                            parse_story = ParserUserStory()
                            dimension = parse_story.text_to_structure(listToStr, currency)
                            text = ' '.join(i for i in aggregated_dataset.keys())
                            parse_story = ParserUserStory()
                            currency = "$"
                            dimension_1 = parse_story.text_to_structure(text, currency)
                            dimension_1 = {'Categorical_Dimension' : ', '.join(i for i in dimension_1[0][0]['chartDimension']['Categorical Dimension']),
                                            'Goal_Measure' : ', '.join(i for i in dimension_1[0][0]['chartDimension']['Goal Measure']),
                                            'Measure' : ', '.join(i for i in dimension_1[0][0]['chartDimension']['Measure']),
                                            'Timeline_Dimension' : ', '.join(i for i in dimension_1[0][0]['chartDimension']['Timeline Dimension'])
                                        }
                            #new dimension - due to grouping ends
                            aggregated_dataset_order = chart_viewer.get_tabular_chart_sort(dimension_1, aggregated_dataset)
                            
                            # aggregated_dataset['aggregated_dataset_order'] = (list(final_agg_list.keys()))
                            new_chart['recommendedCharts'][0]['aggregated_dataset_order'] =  aggregated_dataset_order
                            new_chart['recommendedCharts'][0]['aggregatedDataset'] = aggregated_dataset
                        #for sorting the tabular chart ends
                        #update the database
                        #################################
                        myquery = {"_id" : ObjectId(f"{widget_id}")}
                        newvalues = { "$set": {"chartData": new_chart}}
                        widget_db.update_one(myquery, newvalues)
                        message = 'new chart'
                        payload = {}
                        #For sorting adding in query
                        # new_chart['aggregated_dataset_order'] = aggregated_dataset_order
                        fetched_data_ = data_db.find_one({"workspaceId" : workspace_id})
                        # print("==================fetched_data_==================")
                        # pprint.pprint(fetched_data_)
                        payload['data'] = new_chart
                        is_success = True
                        status = 200 
                        
            response['message'] = message
            response['payload'] = payload
            response['success'] = is_success
        except ValueError as exc:
            response['message'] = exc.__str__()
            status = 400
        except:
            print(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

    def get(self):
        '''
        It is to get all possible charts based on widget
        '''
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success = 400, False
            mycol = mydb["widget"]
            res, posCharts = {}, []        
            widget_id = str(request.args.get('widgetId'))
            out = mycol.find_one({'_id' : ObjectId(f"{widget_id}")})
            if not out:
                response['payload'] = {}
            else:
                res['possibleCharts'] = out['chartData'].get('possibleCharts', [])
                posCharts.append(res)
                response['payload'] = {'chartData' : posCharts}
            status = 200
            
            response['message'] = 'success'
            response['success'] = True
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

    def put(self):
        '''
            This api is to replace first index of recomended charts with selected posible chart
        '''
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success, payload = 400, False, {}
            mycol = mydb["widget"]
            widget_id = str(request.json.get('widgetId', None))
            pos_chart_index = (request.json.get('chartIndex', [0]))
            out = mycol.find_one({'_id' : ObjectId(f"{widget_id}")})
            if not out:
                message = "Widget not present"
            else:
                possibleCharts = out['chartData'].get('possibleCharts', [])
                recommendedCharts = out['chartData'].get('recommendedCharts', [])
                if possibleCharts and recommendedCharts:
                    recommendedCharts[0], possibleCharts[pos_chart_index[0]] = possibleCharts[pos_chart_index[0]], recommendedCharts[0]
                    out['chartData']['possibleCharts'] = possibleCharts
                    out['chartData']['recommendedCharts'] = recommendedCharts
                    myquery = {"_id" : ObjectId(f"{widget_id}")}
                    newvalues = { "$set": {"chartData": out['chartData']}}
                    mycol.update_one(myquery, newvalues)
                    status, is_success = 200, True
                    message = payload = "Swaped Successfully"
            response['message'] = message
            response['payload'] = payload
            response['success'] = is_success
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

class ChartMapping(Resource):
    def post(self):
        '''
        It is to store the Chart mapping
        
        '''
        message = "No yet started"
        status = 200
        payload = message
        is_success = False
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success = 400, False
            charts_db = mydb["charts_by_count"] 
            json_data = request.json
            chart_data = json_data.get('chartData',None)
            if 'chart_id' in chart_data.keys():
                chart_data.pop('chart_id')
            #check for input
            if not chart_data:
                payload = 'No chart mapping found'      
                message = payload
                is_success = False
                status = 400
            else:
                ####
                # '*' logic starts
                complete_data = {}
                combination_out = []
                if chart_data['noOfMeasure'] == "*":
                    if chart_data['totalNoOfMeasure'] == 1:
                        combination_out = combination_out
                    if chart_data['totalNoOfMeasure'] == 2:
                        combination_out = [[1,1]]
                    if chart_data['totalNoOfMeasure'] > 2:
                        parse_story = ParserUserStory()
                        combination_out = parse_story.sum_combination(chart_data['totalNoOfMeasure'])
                    combination_out.append([0, chart_data['totalNoOfMeasure']])
                    combination_out.append([chart_data['totalNoOfMeasure'], 0])
                    count = 0

                    for i in combination_out:
                        complete_data[count] = {
                        "actual": chart_data["actual"],
                        "alias": chart_data["alias"],
                        "keywords": chart_data["keywords"],
                        "noOfMeasure": i[0],
                        "noOfGoalMeasure": i[1],
                        "noOfCategoryDimensions": chart_data["noOfCategoryDimensions"],
                        "noOfTimelineDimensions": chart_data["noOfTimelineDimensions"],
                        "totalNoOfMeasure" : chart_data["totalNoOfMeasure"],
                        "totalNoOfDimension" : chart_data["totalNoOfDimension"],
                        "sumOfDimension&Measure": chart_data["sumOfDimension&Measure"]
                    }
                        count +=1

                if chart_data['noOfMeasure'] != "*":
                    complete_data = {0 : chart_data}
                # '*' logic ends
                ####
                chart_data = complete_data
                for data in chart_data:
                    # print("=====data====")
                    # pprint.pprint(chart_data[data])
                    if 'noOfGoalMeasure' not in chart_data[data].keys():
                        chart_data[data]['dimension'] = tuple([int(chart_data[data]['noOfMeasure']), int(chart_data[data]['noOfCategoryDimensions']) ])
                    else:
                        chart_data[data]['dimension'] = tuple([int(chart_data[data]['noOfMeasure']), int(chart_data[data]['noOfGoalMeasure']), int(chart_data[data]['noOfCategoryDimensions']), int(chart_data[data]['noOfTimelineDimensions']) ])
                    charts_db.insert_one({'data' : chart_data[data]})
                    is_success = True
                    status = 200 
                    message = 'New chart mapping inserted'
            response['message'] = message
            response['payload'] = message
            response['success'] = is_success
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))
    
    def get(self):
        '''
        Get all chart mapping
        '''
        message = "No yet started"
        status = 200
        payload = message
        is_success = False
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success = 400, False
            charts_db = mydb["charts_by_count"]
            output=[]
            for chrt in charts_db.find():
                chrt['data'].pop('dimension')
                chrt['data']['chart_id'] = str(chrt.pop('_id'))
                 #for adding new column - total no of measures / dimensions / sum starts
                if 'noOfGoalMeasure' in chrt['data'] and 'noOfTimelineDimensions' in chrt['data']:
                    chrt['data']['totalNoOfMeasure'] = int(chrt['data']['noOfMeasure']) + int(chrt['data']['noOfGoalMeasure'])
                    chrt['data']['totalNoOfDimension'] = int(chrt['data']['noOfCategoryDimensions']) + int(chrt['data']['noOfTimelineDimensions'])
                    chrt['data']['sumOfDimension&Measure'] = chrt['data']['totalNoOfMeasure'] + chrt['data']['totalNoOfDimension']
                else:
                    if 'noOfGoalMeasure' in chrt['data']:
                        chrt['data']['totalNoOfMeasure'] = int(chrt['data']['noOfMeasure']) + int(chrt['data']['noOfGoalMeasure'])
                    if 'noOfGoalMeasure' not  in chrt['data']:
                        chrt['data']['totalNoOfMeasure'] = int(chrt['data']['noOfMeasure'])
                    chrt['data']['totalNoOfDimension'] = int(chrt['data']['noOfCategoryDimensions'])
                    chrt['data']['sumOfDimension&Measure'] = chrt['data']['totalNoOfMeasure'] + chrt['data']['totalNoOfDimension']
                #for adding new column - total no of measures / dimensions / sum ends
                output.append(chrt['data'])
            if output:
                status = 200
                is_success = True
                message = "Chart Mapping fetched"
            else:
                status = 400
                is_success = False
                message = "No charts found"
            response['payload'] = output
            response['success'] = is_success
            response['message'] = message
            print('response is ',jsonify(response['payload']))
            
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

    def put(self):
        '''
        It is to update the Chart mapping
        
        '''
        message = "No yet started"
        status = 200
        payload = message
        is_success = False
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success = 400, False
            charts_db = mydb["charts_by_count"]
            json_data = request.json
            chart_data = json_data.get('chartData',None)
            chart_id = chart_data.pop('chart_id')
            if not chart_id or not chart_data:
                message = "Chart ID not present"
            else:
                myquery = {"_id" : ObjectId(f"{chart_id}")}
                if 'noOfGoalMeasure' not in chart_data.keys():
                    chart_data['dimension'] = tuple([chart_data['noOfMeasure'], chart_data['noOfCategoryDimensions']])
                else:
                    chart_data['dimension'] = tuple([chart_data['noOfMeasure'], chart_data['noOfGoalMeasure'], chart_data['noOfCategoryDimensions'], chart_data['noOfTimelineDimensions']])
                newvalues = { "$set": {"data":chart_data}}
                charts_db.update_one(myquery, newvalues)
                is_success = True
                status = 200 
                message = 'chart mapping updated'
            response['message'] = message
            response['payload'] = message
            response['success'] = is_success
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

    def delete(self):
        try:
            mycol = mydb["charts_by_count"]
            chart_id = request.args.get('chart_id')
            if(list(mycol.find({"_id":ObjectId(f"{chart_id}")}))):
                myquery = { "_id": ObjectId(f"{chart_id}") }
                mycol.delete_one(myquery)
                res = jsonify({'result' : "chart mapping has been deleted"})
                status = 200
            else:
                res = jsonify({'result' : "this chart mapping doesn't exists"})
                status = 200
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            res = jsonify({'result' : "not delted"})
            status = 400
        finally:
            return(make_response(res, status))


class NewChartMapping(Resource):
    def post(self):
        '''
        It is to store the New Chart mapping
        
        '''
        message = "No yet started"
        status = 200
        payload = message
        is_success = False
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success = 400, False
            charts_db = mydb["charts_by_count"] 
            json_data = request.json
            chart_data = json_data.get('chartData',None)
            if 'chart_id' in chart_data.keys():
                chart_data.pop('chart_id')
            #check for input
            if not chart_data:
                payload = 'No chart mapping found'      
                message = payload
                is_success = False
                status = 400
            else:
                ########################
                #logic 1 - with * starts 
                if chart_data["sumOfDimension&Measure"] == "*":
                    df = pd.DataFrame(columns=['noOfMeasure','noOfGoalMeasure', 'noOfCategoryDimensions', 'noOfTimelineDimensions'])
                    dfs = []
                    dfs_1 = []
                    # for i in range(10):
                    #     # df['noOfMeasure'] = [i]
                    #     dfs.append(i)
                    # for i in df:
                    #     df[i] = dfs
                    for i in range(5):
                        dfs = ["%#04d" % num for num in range(0, 10000)]
                        dfs = [dfs[x:x+1] for x in range(0, len(dfs), 1)]
                        for j in dfs:
                            for l in j:
                                dfs = list(l)
                                dfs_1.append(dfs)
                        
                    # chart_data['dimension'] = tuple([int(chart_data['noOfMeasure']), int(chart_data['noOfGoalMeasure']), int(chart_data['noOfCategoryDimensions']), int(chart_data['noOfTimelineDimensions']) ])
                    dfs_1_df = pd.DataFrame(dfs_1)
                    dfs_1_df.columns=['noOfCategoryDimensions', 'noOfGoalMeasure', 'noOfMeasure', 'noOfTimelineDimensions']
                    #####################################
                    #for comparing dimension in db starts
                    new_compare_list = dfs_1_df.values.tolist()
                    dimension_db_list = []
                    for i in charts_db.find():
                        if  chart_data["actual"] == i['data']['actual']: #comparing with "actual chart type " equals the "input chart type"  filtering 
                            dimension_db_list.append(i['data']['dimension'])
                    compared_list = [i for i in new_compare_list if i not in dimension_db_list]
                    #for comparing dimension in db ends
                    #####################################

                    ################################################################################
                    #final dataframe for the Data Insertion and final logic / after filtering starts
                    final_df = pd.DataFrame(compared_list, columns = ['noOfCategoryDimensions','noOfGoalMeasure', 'noOfMeasure', 'noOfTimelineDimensions'])
                    final_df_2 = []
                    for index, row in final_df.iterrows():
                        final_df_3 = row['noOfCategoryDimensions'], row['noOfGoalMeasure'], row['noOfMeasure'], row['noOfTimelineDimensions']
                        final_df_3 = {
                            "dimension" : final_df_3,
                            "actual" : chart_data["actual"],
                            "alias" : chart_data["alias"],
                            "keywords" : chart_data["keywords"],
                            "noOfCategoryDimensions" : final_df_3[0],
                            "noOfGoalMeasure" : final_df_3[1],
                            "noOfMeasure" : final_df_3[2],
                            "noOfTimelineDimensions" : final_df_3[3],
                            "totalNoOfMeasure" : int(final_df_3[1]) + int(final_df_3[2]),
                            "totalNoOfDimension" : int(final_df_3[3]) + int(final_df_3[0]) ,
                            "sumOfDimension&Measure" : int(final_df_3[0]) + int(final_df_3[1]) + int(final_df_3[2]) + int(final_df_3[3])
                        }
                        final_df_2.append(final_df_3)
                        chart_data["dimension"] = i
                    chart_data = final_df_2
                ############################
                #logic 2 - with <, >, * starts
                str_list = ["<", ">", "*"]
                email_contains_service = any(i in chart_data['totalNoOfMeasure'] or  chart_data['totalNoOfDimension']  for i in str_list)
                if email_contains_service == True :
                    for i in chart_data['totalNoOfMeasure']:
                        chart_data['noOfMeasure'] = round(int(chart_data['totalNoOfMeasure'][1]) / 2)
                        chart_data['noOfGoalMeasure'] = round(int(chart_data['totalNoOfMeasure'][1]) / 2)
                        
                        if i == "<":
                            noOfMeasure = []
                            noOfGoalMeasure = []
                            for j in range(0, chart_data['noOfMeasure']):
                                noOfMeasure_append = j
                                print("noOfMeasure_append", noOfMeasure_append)
                                noOfMeasure.append(noOfMeasure_append)
                            for j in range(0, chart_data['noOfGoalMeasure']):
                                noOfGoalMeasure_append = j
                                print("noOfGoalMeasure_append", noOfGoalMeasure_append)
                                noOfGoalMeasure.append(noOfGoalMeasure_append)
                        if i == ">":
                            noOfMeasure = []
                            noOfGoalMeasure = []
                            for j in range(chart_data['noOfMeasure'], 9):
                                noOfMeasure_append = j
                                print("noOfMeasure_append", noOfMeasure_append)
                                noOfMeasure.append(noOfMeasure_append)
                            for j in range(chart_data['noOfGoalMeasure'], 9):
                                noOfGoalMeasure_append = j
                                print("noOfGoalMeasure_append", noOfGoalMeasure_append)
                                noOfGoalMeasure.append(noOfGoalMeasure_append)
                    for i in chart_data['totalNoOfDimension']:
                        chart_data['noOfCategoryDimensions'] = round(int(chart_data['totalNoOfDimension'][1]) / 2)
                        chart_data['noOfTimelineDimensions'] = round(int(chart_data['totalNoOfDimension'][1]) / 2)
                        
                        if i == "<":
                            noOfCategoryDimensions = []
                            noOfTimelineDimensions = []
                            for j in range(0, chart_data['noOfCategoryDimensions']):
                                noOfCategoryDimensions_append = j
                                noOfCategoryDimensions.append(noOfCategoryDimensions_append)
                            for j in range(0, chart_data['noOfTimelineDimensions']):
                                noOfTimelineDimensions_append = j
                                noOfTimelineDimensions.append(noOfTimelineDimensions_append)
                        if i == ">":
                            noOfCategoryDimensions = []
                            noOfTimelineDimensions = []
                            for j in range(chart_data['noOfCategoryDimensions'], 9):
                                noOfCategoryDimensions_append = j
                                noOfCategoryDimensions.append(noOfCategoryDimensions_append)
                            for j in range(0, chart_data['noOfTimelineDimensions']):
                                noOfTimelineDimensions_append = j
                                noOfTimelineDimensions.append(noOfTimelineDimensions_append)
                    df = pd.DataFrame(columns=['noOfMeasure','noOfGoalMeasure'])
                    df['noOfMeasure'] = noOfMeasure
                    df['noOfGoalMeasure'] = noOfGoalMeasure
                    df.insert(0,"noOfCategoryDimensions", (noOfCategoryDimensions + [0]*len(df))[:len(df)], True)
                    df.insert(0,"noOfTimelineDimensions", (noOfTimelineDimensions + [0]*len(df))[:len(df)], True)
                    #####################################
                    #for comparing dimension in db starts
                    new_compare_list = df.values.tolist()
                    dimension_db_list = []
                    for i in charts_db.find():
                        if  chart_data["actual"] == i['data']['actual']: #comparing with "actual chart type " equals the "input chart type"  filtering 
                            dimension_db_list.append(i['data']['dimension'])
                    compared_list = [i for i in new_compare_list if i not in dimension_db_list]
                    #for comparing dimension in db ends
                    #####################################

                    ################################################################################
                    #final dataframe for the Data Insertion and final logic / after filtering starts
                    final_df = pd.DataFrame(compared_list, columns = ['noOfCategoryDimensions','noOfGoalMeasure', 'noOfMeasure', 'noOfTimelineDimensions'])
                    final_df_2 = []
                    for index, row in final_df.iterrows():
                        final_df_3 = row['noOfCategoryDimensions'], row['noOfGoalMeasure'], row['noOfMeasure'], row['noOfTimelineDimensions']
                        final_df_3 = {
                            "dimension" : final_df_3,
                            "actual" : chart_data["actual"],
                            "alias" : chart_data["alias"],
                            "keywords" : chart_data["keywords"],
                            "noOfCategoryDimensions" : final_df_3[0],
                            "noOfGoalMeasure" : final_df_3[1],
                            "noOfMeasure" : final_df_3[2],
                            "noOfTimelineDimensions" : final_df_3[3],
                            "totalNoOfMeasure" : int(final_df_3[1]) + int(final_df_3[2]),
                            "totalNoOfDimension" : int(final_df_3[3]) + int(final_df_3[0]) ,
                            "sumOfDimension&Measure" : int(final_df_3[0]) + int(final_df_3[1]) + int(final_df_3[2]) + int(final_df_3[3])
                        }
                        final_df_2.append(final_df_3)
                        chart_data["dimension"] = i
                    chart_data = final_df_2
                    #final dataframe for the Data Insertion and final logic / after filtering ends
                    ################################################################################

                    ###################################
                    #adding the chart data in DB starts
                    for i in chart_data:
                        # dimension = [int(i["noOfCategoryDimensions"]),  int(i["noOfGoalMeasure"]), int(i["noOfMeasure"]), int(i["noOfTimelineDimensions"]) ]
                        charts_db.insert({'data' : {
                                                        "alias" : i["alias"],"keywords" : i["keywords"],
                                                        "actual" : i["actual"], "noOfCategoryDimensions" : int(i["noOfCategoryDimensions"]), "noOfGoalMeasure" : int(i["noOfGoalMeasure"]),
                                                        "noOfMeasure" :int(i["noOfMeasure"]), "noOfTimelineDimensions" : int(i["noOfTimelineDimensions"]), "totalNoOfMeasure" : int(i["totalNoOfMeasure"]),
                                                        "totalNoOfDimension" : int(i["totalNoOfDimension"]),"sumOfDimension&Measure" : int(i["sumOfDimension&Measure"]),
                                                         "dimension" : [int(i["noOfCategoryDimensions"]),  int(i["noOfGoalMeasure"]), int(i["noOfMeasure"]), int(i["noOfTimelineDimensions"]) ]
                                                        } 
                                                        }
                                                        )
                    #adding the chart data in DB ends
                    #################################

                #logic 2 - with <, >, * ends
                ############################
                    
                else:
                    print("===========No============")
                is_success = True
                status = 200 
                message = 'New chart mapping inserted'
            response['message'] = message
            response['payload'] = message
            response['success'] = is_success
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))
  

class ChartFilter(Resource):
    def post(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            widget = request.json
            mycol = mydb["widget"]
            dataDB = mydb["data"]
            input_ = request.json
            workspace_id = input_.get('workspaceId',None)
            widget_id = input_.get('widgetId',None)
            filter_categories = input_.get('filterCategories',None)
            filteredSyntheticElements = input_.get('filteredSyntheticElements',None)
            filteredPeriods = input_.get('filteredPeriods',None)
            period_freq = input_.get('periodType',None)
            single_dataSet = dataDB.find_one({"workspaceId" : workspace_id})
            new_dataSet = single_dataSet['data']['dataSet']
            sub_category = {}
            if filteredPeriods and period_freq:
                filterStartDate = filteredPeriods[0]
                filterEndDate = filteredPeriods[1]
                if period_freq == 'M':
                    sub_category['Period'] = list(pd.date_range(start = filterStartDate, end = filterEndDate, freq = 'MS').strftime('%b-%Y'))
                elif period_freq == 'Y':
                    sub_category['Period'] = list(pd.date_range(start = filterStartDate, end = filterEndDate, freq = 'YS').strftime('%Y'))
                elif period_freq == 'D':
                    sub_category['Period'] = list(pd.date_range(start = filterStartDate, end = filterEndDate).strftime('%Y-%m-%d'))
            else:
                sub_category = filteredSyntheticElements
            # if single_dataSet['filteredData']:
            #     filter_data = single_dataSet['filteredData']
            if not new_dataSet:
                message = "No DataSet found"
                success = False
                status = 400
            else:
                filtered_dataSet, filterNames, filter_data = [], [], {}
                chart_viewer = ChartViewer()
                sub_category = dict( [(k,v) for k,v in sub_category.items() if len(v)>0])
                # Check for filter options coming else remove flter
                if sub_category:
                    for data in new_dataSet['tableData']:
                        all_present = []
                        for key, value in sub_category.items():
                            if any([ ele in data.values() for ele in value]):
                                all_present.append(True)
                        if len(all_present) == len(sub_category.keys()) and all(all_present):
                            filtered_dataSet.append(data)
                    # filtered_dataSet = list({frozenset(item.items()) : item for item in filtered_dataSet}.values())
                    new_dataSet['tableData'] = filtered_dataSet
                    if not filtered_dataSet:
                        status, success = 400, False
                        message = "Filter options not available"
                        response['message'] = message
                        response['success'] = success
                        response['payload'] = message
                        return(make_response(response, status))
                    # for key, value in sub_category.items():
                    #     filterNames.extend(value)
                    # filterNames = sorted(filterNames)
                # If widget id then apply filter to only slected chart
                if widget_id:
                    opt_widget = mycol.find_one({"_id":ObjectId(f"{widget_id}")})
                    if not opt_widget:
                        status, success = 400, False
                        message = "Widget data not found"
                        response['message'] = message
                        response['success'] = success
                        response['payload'] = message
                        return(make_response(response, status))
                    chartsData = opt_widget['chartData']['recommendedCharts']
                    periodData = opt_widget['chartData']['periodicity']
                    filtered_data = []
                    for chart in chartsData:
                        aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(chart, new_dataSet, periodData, dimension)
                        chart['aggregatedDataset'] = aggregated_dataset
                        chart['aggregationType'] = aggregated_function
                        filtered_data.append(chart)
                    opt_widget['chartData']['recommendedCharts'] = filtered_data
                    widget_updated = opt_widget['chartData']
                    update_data = {"$set" : {"chartData": widget_updated}}
                    mycol.update_one({"_id":ObjectId(f"{widget_id}")},update_data)
                    message = "Filter applied to selected widget"
                    status = 200
                # apply filter to all charts under workspace
                else:
                    all_widgets = mycol.find({"workspaceId" : workspace_id})
                    for widget in all_widgets:
                        _id = str(widget["_id"])
                        periodData = {}
                        
                        # print("==========================widget['chartData']['recommendedCharts']==========================")
                        # pprint.pprint(widget['chartData'][0]['recommendedCharts'])
                        if 'recommendedCharts' in widget['chartData']:
                            chartsData = widget['chartData']['recommendedCharts']
                        else:
                            if 'recommendedCharts' in widget['chartData'][0]:
                                chartsData = widget['chartData'][0]['recommendedCharts']
                        # chartsData = widget['chartData']['recommendedCharts']
                        if 'periodicity' in widget['chartData'].keys():
                            periodData = widget['chartData']['periodicity']
                        filtered_data = []
                        for chart in chartsData:
                            dimension = []
                            aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(chart, new_dataSet, periodData, dimension)
                            chart['aggregatedDataset'] = aggregated_dataset
                            chart['aggregationType'] = aggregated_function
                            filtered_data.append(chart)
                        widget['chartData']['recommendedCharts'] = filtered_data
                        widget_updated = widget['chartData']
                        update_data = {"$set" : {"chartData": widget_updated}}
                        mycol.update_one({"_id":ObjectId(f"{_id}")},update_data)
                    filter_data['filteredCategories'] = filter_categories
                    filter_data['filteredDataSet'] = filtered_dataSet
                    if 'Period' in sub_category.keys():
                        sub_category['Period'] = filteredPeriods
                    filter_data['filteredSubCategories'] = sub_category
                    filter_update = {"$set" : {"filteredData": filter_data}}
                    dataDB.update_one({"workspaceId" : workspace_id}, filter_update)
                    message = "Filter applied to all widgets"
                    status = 200
            response['message'] = message
            response['success'] = True
            response['payload'] = filter_data
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

    def get(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            mycol = mydb["data"]
            workspace_id = str(request.args.get('workspaceId'))
            single_data = mycol.find_one({"workspaceId":workspace_id})
            if not single_data:
                status, success = 400, False
                message = "No data set found"
                response['message'] = message
                response['success'] = success
                response['payload'] = message
                return(make_response(response, status))
            res_dict = {}
            workspace_categories =  single_data['data']['dataDetails']['categories']
            # single_dataset = single_data['data']['dataSet']['tableData']
            category_names = [cat['elementName'] for cat in workspace_categories]
            if single_data['data']['dataDetails']['periodicity']:
                category_names.append("Period")
            # sub_categories = {}
            # for catname in category_names:
            #     i_set = set()
            #     for i in single_dataset:
            #         i_set.add(i[catname])
            #     sub_categories[catname] = sorted(list(i_set))
            res_dict['categories'] = category_names
            # res_dict['subcatagories'] = sub_categories
            status, success = 200, True
            message = "Categories for filter fetched"
            response['message'] = message
            response['success'] = success
            response['payload'] = res_dict
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return (make_response(response, status))

    def put(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            mycol = mydb["data"]
            input_ = request.json
            workspace_id = str(input_.get('workspaceId'))
            cat_names = input_.get('filterCategories',None)
            single_data = mycol.find_one({"workspaceId":workspace_id})
            single_dataset = single_data['data']['dataSet']['tableData']
            if not single_data:
                status, success = 400, False
                message = "No data set found"
                response['message'] = message
                response['success'] = success
                response['payload'] = message
                return(make_response(response, status))
            final_dict, sub_categories = {}, {}
            for catname in cat_names:
                i_set = set()
                for i in single_dataset:
                    i_set.add(i[catname])
                sub_categories[catname] = sorted(list(i_set))
            final_dict['filteredCategories'] = cat_names
            final_dict['withoutFilterSubCategories'] = sub_categories
            filter_update = {"$set" : {"filteredData": final_dict}}
            mycol.update_one({"workspaceId" : workspace_id}, filter_update)
            status, success = 200, True
            message = "Categories and Subcategories for filter added"
            response['message'] = message
            response['success'] = success
            response['payload'] = final_dict
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return (make_response(response, status))

class Uploader(Resource):
    @staticmethod
    def createWorkspace(**workspace):
        print("=======Uploader create workspace is called====")
        try:
            is_success = False
            mycol = mydb["workspace"]
            data_db = mydb['data']
            widget_db = mydb['widget']
            user_db = mydb['users']
            workspace_name = workspace.get('workspaceName', None)
            workspace_type = workspace.get('workspaceType', None)
            # currency = workspace.get('currencies', '$')
            email = workspace.get('email', None)
            user_story = workspace.get('userStory', None)
            currency = workspace.get('currencies', user_db.find_one({"email": email}).get('currencies', "$"))
            workspace_exists = mycol.find_one({"email":email, "workspaceName" : workspace_name})
            if workspace_exists:
                message = 'Workspace Already Exists'
            else:
                parse_story = ParserUserStory()
                widget_payload , user_story, consolidatedData = parse_story.text_to_structure(user_story, currency)
                no_of_widgets = len(user_story)
                user_story_str = " ".join(user_story)
                workspace_id = mycol.insert_one({"email":email, "workspaceName" : workspace_name.capitalize(), "userStory" : user_story_str,
                            "noOfWidgets": no_of_widgets, "workspaceType" : workspace_type, "currencies": currency})
                workspace_id = str(workspace_id.inserted_id)
                data_db.insert_one({'workspaceId' : workspace_id, 'data' : consolidatedData})

                for i in range(0,no_of_widgets):
                    widget_name = f'Widget {i+1}'
                    temp_user_story = user_story[i] if user_story else ""
                    widget_id = widget_db.insert_one({'workspaceId' : workspace_id, 'widgetName' : widget_name.capitalize(),
                                'userStory' : temp_user_story.capitalize(), 'chartData' : widget_payload[i], 'currencies':currency})
                message = 'Workspace Created Successfully'
                status = 200
                is_success = True
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            is_success = False
        finally:
            return is_success
    def post(self):
        print("=======Uploader post is called====")
        response = {'success' : False, 'message' : 'Failure', "payload" : {}}
        try:
            status, success, payload = 400, False, {}
            trainMLmodel = TrainMLModel()
            message = "Error: Not Trained"
            email = request.form.get("email")
            workspace_type = request.form.get("workspaceType", "Finance")
            client_name = request.form.get("client", "Recent").strip()
            train_file = request.files.get('file', [])
            if train_file:
                excel_data = pd.ExcelFile(train_file)
                if 'Sheet1' in excel_data.sheet_names:
                    new_train_data = pd.read_excel(train_file, sheet_name = 'Sheet1')
                    if set(new_train_data.columns) == set(['Keywords', 'Type', 'Classification', 'Min', 'Max']):
                        # train_file.save(os.path.join(app.config['UPLOAD_FOLDER'],*["ModelTrainers",train_file.filename]))
                        # new_train_data = pd.read_excel(os.path.join(app.config['UPLOAD_FOLDER'],*["ModelTrainers",f.filename]))
                        new_train_data = new_train_data.dropna(how = 'all', axis = 1)
                        new_train_data['Client'] = client_name
                        train_dict = trainMLmodel.data_pre_process(new_train_data)
                        if train_dict:
                            trainMLmodel.load_ML_model(train_dict)
                            status, success = 200, True
                            message = 'model trained successfully'
                            payload = message
                    else:
                        message = "Follow Excel format with columns: ['Keywords', 'Type', 'Classification', 'Min', 'Max']"
                else:
                    message = "Use Sheet1 as excel sheet name"
            else:
                message = "Please upload the training excel file"
            response['message'] = message
            response['success'] = success
            response['payload'] = payload
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "file upload failed"
            status = 400
        finally:
            return (make_response(response, status))
    
    def put(self):
        response = {'success' : False, 'message' : 'Failure', "payload" : {}}
        try:
            status, success, payload = 400, False, {}
            trainMLmodel = TrainMLModel()
            message = "Error: Not Trained"
            dimension_data = request.json.get('dimensions', None)
            keywords_db = mydb["training_keywords"]
            if dimension_data:
                train_data = json.loads(json.dumps(dimension_data))
                new_train_data = pd.DataFrame(columns=['Keywords', 'Type', 'Classification', 'numberOfKeywords', 'Min', 'Max', 'Keywords_list', 'Client'])
                if 'measure' in train_data.keys():
                    for measure in train_data['measure']:
                        if measure:
                            #Checking the word if already there - star
                            keywords_data = keywords_db.find_one({"Keywords" : measure})
                            measure_dict = {}
                            #
                            measure_dict['Keywords'] = measure
                            measure_dict['Type'] = "Integer"
                            measure_dict['Classification'] = "Measure"
                            measure_dict['Client'] = "Recent"
                            #
                            measure_dict['numberOfKeywords'] = 0
                            measure_dict['Min'] = 0
                            measure_dict['Max'] = 100
                            measure_dict['Keywords_list'] = []
                            ##

                            ##

                            if keywords_data:
                                measure_dict['Type'] = keywords_data['Type']
                                measure_dict['numberOfKeywords'] = keywords_data['numberOfKeywords'] if 'numberOfKeywords' in keywords_data else 0
                                measure_dict['Min'] = keywords_data['Min'] if 'Min' in keywords_data else 0
                                measure_dict['Max'] = keywords_data['Max'] if 'Max' in keywords_data else 100
                                measure_dict['Keywords_list'] = keywords_data['Keywords_list'] if 'Keywords_list' in keywords_data else []
                            #Checking the word if already there - ends
                            #
                            measure_dict['numberOfKeywords'] = 0 if measure_dict['numberOfKeywords'] == None else measure_dict['numberOfKeywords']
                            measure_dict['Min'] = 0 if measure_dict['Min'] == None else measure_dict['Min']
                            measure_dict['Max'] = 100 if measure_dict['Max'] == None else measure_dict['Max']
                            measure_dict['Keywords_list'] = [] if measure_dict['Keywords_list'] == None else measure_dict['Keywords_list']
                            #
                            new_train_data = new_train_data.append(measure_dict, ignore_index=True)
                if 'categoryDimension' in train_data.keys():
                    for category in train_data['categoryDimension']:
                        if category:
                            category_dict = {}
                            #Checking the word if already there - starts
                            keywords_data = keywords_db.find_one({"Keywords" : category})
                            category_dict = {}
                            #
                            category_dict['Keywords'] = category
                            category_dict['Type'] = "Categorical"
                            category_dict['Classification'] = "Categorical Dimension"
                            category_dict['Client'] = "Recent"
                            #
                            category_dict['numberOfKeywords'] = 0
                            category_dict['Min'] = 0
                            category_dict['Max'] = 100
                            category_dict['Keywords_list'] = []
                            ##
                            
                            ##

                            if keywords_data:
                                category_dict['Type'] = keywords_data['Type']
                                category_dict['numberOfKeywords'] = keywords_data['numberOfKeywords'] if 'numberOfKeywords' in keywords_data else 0
                                category_dict['Min'] = keywords_data['Min'] if 'Min' in keywords_data else 0
                                category_dict['Max'] = keywords_data['Max'] if 'Max' in keywords_data else 100
                                category_dict['Keywords_list'] = keywords_data['Keywords_list'] if 'Keywords_list' in keywords_data else []
                            #Checking the word if already there - ends

                            #
                            category_dict['numberOfKeywords'] = 0 if category_dict['numberOfKeywords'] == None else category_dict['numberOfKeywords']
                            category_dict['Min'] = 0 if category_dict['Min'] == None else category_dict['Min']
                            category_dict['Max'] = 100 if category_dict['Max'] == None else category_dict['Max']
                            category_dict['Keywords_list'] = [] if category_dict['Keywords_list'] == None else category_dict['Keywords_list']
                            #
                            
                            new_train_data = new_train_data.append(category_dict, ignore_index=True)
                if 'goalMeasure' in train_data.keys():
                    for goalmeasure in train_data['goalMeasure']:
                        if goalmeasure:
                            goalmeasure_dict = {}
                            #Checking the word if already there - starts
                            keywords_data = keywords_db.find_one({"Keywords" : goalmeasure})
                            goalmeasure_dict = {}
                            #
                            goalmeasure_dict['Keywords'] = goalmeasure
                            goalmeasure_dict['Type'] = "Currency"
                            goalmeasure_dict['Classification'] = "Goal Measure"
                            goalmeasure_dict['Client'] = "Recent"
                            #
                            ##
                            goalmeasure_dict['numberOfKeywords'] = 0
                            goalmeasure_dict['Min'] = 0
                            goalmeasure_dict['Max'] = 100
                            goalmeasure_dict['Keywords_list'] = []
                            ##

                            if keywords_data:
                                goalmeasure_dict['Type'] = keywords_data['Type']
                                goalmeasure_dict['numberOfKeywords'] = keywords_data['numberOfKeywords'] if 'numberOfKeywords' in keywords_data else 0
                                goalmeasure_dict['Min'] = keywords_data['Min'] if 'Min' in keywords_data else 0
                                goalmeasure_dict['Max'] = keywords_data['Max'] if 'Max' in keywords_data else 100
                                goalmeasure_dict['Keywords_list'] = keywords_data['Keywords_list'] if 'Keywords_list' in keywords_data else []
                            #Checking the word if already there - ends

                            #
                            goalmeasure_dict['numberOfKeywords'] = 0 if goalmeasure_dict['numberOfKeywords'] == None else goalmeasure_dict['numberOfKeywords']
                            goalmeasure_dict['Min'] = 0 if goalmeasure_dict['Min'] == None else goalmeasure_dict['Min']
                            goalmeasure_dict['Max'] = 100 if goalmeasure_dict['Max'] == None else goalmeasure_dict['Max']
                            goalmeasure_dict['Keywords_list'] = [] if goalmeasure_dict['Keywords_list'] == None else goalmeasure_dict['Keywords_list']
                            #
                            
                            new_train_data = new_train_data.append(goalmeasure_dict, ignore_index=True)
                if set(new_train_data.columns) == set(['Keywords', 'Type', 'Classification', 'numberOfKeywords', 'Min', 'Max', 'Keywords_list', 'Client']):
                    # train_file.save(os.path.join(app.config['UPLOAD_FOLDER'],*["ModelTrainers",train_file.filename]))
                    # new_train_data = pd.read_excel(os.path.join(app.config['UPLOAD_FOLDER'],*["ModelTrainers",f.filename]))
                    new_train_data = new_train_data.dropna(how = 'all', axis = 1)
                    train_dict = trainMLmodel.data_pre_process(new_train_data)
                    if train_dict:
                        trainMLmodel.load_ML_model(train_dict)
                        # user_story = ", ".join(new_train_data['Keywords'].tolist())
                        # data = {"workspaceName": train_file.filename.split(".")[0], "workspaceType": workspace_type, "email":email, "userStory":user_story.lower()}
                        # workspace_req = self.createWorkspace(**data)
                        # if workspace_req == True:
                        #     status, success = 200, True
                        #     message = 'model trained successfully and workspace created'
                        #     payload = new_train_data.to_dict(orient='records')
                        # else:
                        #     status, success = 200, True
                        #     message = 'model trained successfully and workspace not created'
                        #     payload = new_train_data.to_dict(orient='records')
                        status, success = 200, True
                        message = 'new data keywords trained'
                        payload = message
                else:
                    message = "Follow Excel format with columns: ['Keywords', 'Type', 'Classification', 'numberOfKeywords', 'Min', 'Max', 'Keywords_list', 'Client']"
            response['message'] = message
            response['success'] = success
            response['payload'] = payload
            print("========End of Uploader========")
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "new keywords training failed"
            status = 400
        finally:
            return (make_response(response, status))

class Layout(Resource):
    def put(self):
        response = {'success' : False, 'message' : 'Failure', "payload" : {}}
        try:
            status, success, payload = 400, False, {}
            data_db = mydb['data']
            workspace = request.json
            workspaceId = workspace.get('workspaceId', None)
            layout = workspace.get('layout', [])
            workspace_data = data_db.find_one({"workspaceId": workspaceId})
            if not workspace_data:
                message = "Workspace does not exist"
            elif layout:
                myquery = {"workspaceId" : workspaceId}
                newvalues = { "$set": {"layout": layout}}
                data_db.update_one(myquery, newvalues)
                message = "Layout Updated Success"
            else:
                message = "Layout is Empty"
            status, is_success = 200, True
            response['message'] = message
            response['success'] = is_success
            response['payload'] = message
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

class Training_Keywords(Resource):

    #post api starts
    def post(self):
        '''
        Creating a new keyword -  chart mapping
        '''
        message = "No yet started"
        status = 200
        payload = message
        is_success = False
        response = {'success' : False, 'message' : 'Failure', "payload" : {}}
        try:
            status, is_success = 400, False
            keywords_db = mydb["training_keywords"]
            keywords_new_data = request.json
            keywords_file_data = request.form
            keywords_old_data = list(keywords_db.find({}))
            type = keywords_new_data.get('Type', 'Unit')
            classification = keywords_new_data.get('Classification', None)
            min = keywords_new_data.get('Min', None)
            max = keywords_new_data.get('Max', None)
            keywords = keywords_new_data.get('Keywords', None)
            client = keywords_new_data.get('Client', None)
            upload_file = request.files.get('upload_file', [])
            ##### -- adding mainKeywords, keywords for forming Categorical Dimension with Sub Category starts 1.
            numberOfKeywords = keywords_new_data.get('numberOfKeywords', 0)
            keywords_list = keywords_new_data.get('Keywords_list', [])
            ##### -- adding mainKeywords, keywords for forming Categorical Dimension with Sub Category ends 1.
            numberOfKeywords = 0 if numberOfKeywords == 'None' else numberOfKeywords == numberOfKeywords
            if keywords is not None and client is not None:
                data = {
                    "numberOfKeywords" : int(numberOfKeywords),
                    "Keywords_list" : keywords_list,
                    "Keywords" : keywords,
                    "Classification" : classification,
                    "Type" : type,
                    "Min" : min,
                    "Max" : max,
                    "Client" : client
                }
                out = keywords_new_data
                keywords_db.insert_one(data)
            if upload_file:
                upload_file_df = pd.read_excel(upload_file,sheet_name="Logistics Keywords") 
                dd = defaultdict(list)
                #
                upload_file_df = upload_file_df.to_dict('records', into=dd)
                file2 = []
                for i in upload_file_df:
                    file2.append(dict(i))
                for i in file2:
                    if pd.isna(i['Keywords_list']) != True:
                        i['Keywords_list'] = i['Keywords_list'].split(', ')
                        i['numberOfKeywords'] = len(i['Keywords_list'])
                #
                for i in file2:
                    print(i)
                    keywords_db.insert_one(i)
                out = "File Uploaded Susscessfully"
            if keywords_new_data:
                status = 200
                is_success = True
                message = "Keywords Added"
                
            else:
                status = 400
                is_success = False
                message = "No Keywords found"
                out = "Error"
            response['payload'] = out
            response['success'] = is_success
            response['message'] = message
            print('response is ',response['payload'])
            
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return make_response(response, status)
    #post api ends
    def delete(self):
        message = "No yet started"
        status = 200
        payload = message
        is_success = False
        response = {'success' : False, 'message' : 'Failure', "payload" : {}}
        try:
            status, is_success = 400, False
            mycol = mydb["training_keywords"]
            keyword_id = request.args.get('keyword_id', None)
            client = request.args.get('Client', None)
            if (keyword_id):
                if list(mycol.find({"_id": ObjectId(f"{keyword_id}")})):
                    mycol.delete_one({"_id": ObjectId(f"{keyword_id}")})
                    message = "Deleted Based On Id"
                    new_train_data = pd.DataFrame()
                    trainMLmodel = TrainMLModel()
                    train_dict = trainMLmodel.data_pre_process(new_train_data)
                    if train_dict:
                        trainMLmodel.load_ML_model(train_dict)
                    status, is_success = 200, True
                else:
                    message = "Keyword ID not found"
            elif(client):
                if client == "Default":
                    message = "Default data cannot be deleted"
                else:
                    if list(mycol.find({"Client": client})):
                        mycol.delete_many({"Client": client})
                        message = "Deleted Based On Client"
                        new_train_data = pd.DataFrame()
                        trainMLmodel = TrainMLModel()
                        train_dict = trainMLmodel.data_pre_process(new_train_data)
                        if train_dict:
                            trainMLmodel.load_ML_model(train_dict)
                        status, is_success = 200, True
                    else:
                        message = "Client not found"
            else:
                message = "Send Either Client Name or Keyword ID"
            response['message'] = message
            response['success'] = is_success
            response['payload'] = message
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response["message"] = "Error"
            status = 400
        finally:
            return(make_response(response, status))
    
    def get(self):
        '''
        Get all chart mapping
        '''
        message = "No yet started"
        status = 200
        payload = message
        is_success = False
        response = {'success' : False, 'message' : 'Failure', "payload" : {}}
        try:
            status, is_success = 400, False
            keywords_db = mydb["training_keywords"]
            keywords_data = list(keywords_db.find({}))
            keywords = []
            out = {}
            for client in keywords_data:
                client['keyword_id'] = str(client.pop('_id'))
                client.pop('Max')
                client.pop('Min')
                if 'numberOfKeywords' and 'Keywords_list' in client:
                    if type(client['numberOfKeywords']) == float:
                        try:
                            client['numberOfKeywords'] = int(client['numberOfKeywords'])
                        except:
                            client['numberOfKeywords'] = 0
                    if type(client['Keywords_list']) == float:
                        client['Keywords_list'] = []
                keywords.append(client)
            vaas_task_poc_list = pd.DataFrame(keywords)
            vaas_task_poc_list = vaas_task_poc_list.drop_duplicates(subset=['Keywords'], keep="last")
            vaas_task_poc_list = vaas_task_poc_list.reset_index(drop=True)
            # keywords_dict = vaas_task_poc_list.to_dict("records")
            clients_name = list(set(vaas_task_poc_list['Client'].tolist()))
            vaas_task_poc_list = vaas_task_poc_list.to_dict("records")
            grouped_data = {}
            for name in vaas_task_poc_list:
                try:
                    grouped_data[name['Client']].append(name)
                except KeyError:
                    grouped_data[name['Client']] = [name]
            out['data'] = grouped_data
            out['client_names'] = clients_name
            if grouped_data:
                status = 200
                is_success = True
                message = "Keywords fetched based on client"
            else:
                status = 400
                is_success = False
                message = "No Keywords found"
            response['payload'] = out
            response['success'] = is_success
            response['message'] = message
            print('response is ',response['payload'])
            
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return make_response(response, status)

    def put(self):
        '''
        It is to update the Chart mapping
        
        '''
        message = "No yet started"
        status = 200
        payload = message
        is_success = False
        response = {'success' : False, 'message' : 'Failure', "payload" : {}}
        try:
            status, is_success = 400, False
            keywords_db = mydb["training_keywords"]
            keyword_data = request.json
            # keyword_data = keyword_data.get('keywordData',None)
            keyword_id = keyword_data.pop('keyword_id')
            myquery = {"_id" : ObjectId(f"{keyword_id}")}
            keyword_data['Keywords'] = keyword_data['Keywords'].strip()
            newvalues = { "$set": keyword_data}
            keywords_db.update_one(myquery, newvalues)
            if not keyword_id or not keyword_data:
                message = "Keyword ID not present"
            else:
                new_train_data = pd.DataFrame()
                trainMLmodel = TrainMLModel()
                train_dict = trainMLmodel.data_pre_process(new_train_data)
                if train_dict:
                    trainMLmodel.load_ML_model(train_dict)
                    is_success = True
                    status = 200 
                    message = 'Keyword data trained and updated'
            response['message'] = message
            response['payload'] = message
            response['success'] = is_success
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))
class NewKeywords(Resource):
    def post(self):
         try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            input_ = request.json
            keyword_db = mydb["keyword"]
            widget_db = mydb["widget"]
            data_db = mydb["data"]
            all_data_db = mydb["all_data_db"]
            workspace_id = input_.get("workspaceId", None)
            categorical_dimension = input_.get("Categorical_Dimension", [])
            goal_measure = input_.get("Goal_Measure", [])
            measure = input_.get("Measure", [])
            timeline_Dimension = input_.get("Timeline_Dimension", [])
            statement = input_.get("Statement", None)
            chartType = input_.get("chartType", None)
            actualChartType = input_.get("actualChartType", None)
            all_data = all_data_db.find_one({"workspaceId" : workspace_id})
            chartData = all_data['allData']["chartData"]
            currency = "$"
            #######################
            #new random data formation starts
            data = input_.get('data', None)
            widget_data = data
            data_out = data_db.find_one({'workspaceId' : workspace_id})
            whole_data = data_out['data']['dataDetails']
            if data is not None:
                for cats in data['categories']:
                    if cats['categoryId'].lower().strip() not in [whole_cat['categoryId'].lower().strip() for whole_cat in whole_data['categories'] if 'categoryId' in whole_cat.keys()]:
                        whole_data['categories'].append(cats)
                for vars in data['variables']:
                    if vars['variableId'].lower().strip() not in [whole_var['variableId'].lower().strip() for whole_var in whole_data['variables'] if 'variableId' in whole_var.keys()]:
                        whole_data['variables'].append(vars)
                data = whole_data
                random_data = data_generator.get_random_data(copy.deepcopy(data))
                new_variable_list =  [i["variableName"] for i in widget_data["variables"]]
                new_category_list = [i["elementName"] for i in widget_data["categories"]]
                new_category_list = list(dict.fromkeys(new_category_list))
                for i in new_variable_list:
                    if new_variable_list.count(i)>1:
                        response = {'success' : False, 'message' : 'Value Repeated', "payload" : 'Failure'}
                        status, is_success = 400, False
                        return response,status, is_success
                for i in new_category_list:
                    if new_category_list.count(i)>1:
                        response = {'success' : False, 'message' : 'Value Repeated', "payload" : 'Failure'}
                        status, is_success = 400, False
                        return response,status, is_success
                if random_data["tableColumns"] == new_variable_list:
                    print("========fales=========")
                #########
                #update random data in the data db starts
                all_data_random= {}
                temp_random = {}
                all_data_random['dataSet'] = random_data
                categories = data.get('categories',[])
                variables = data.get('variables',[])
                periodicity = data.get('periodicity',[])
                temp_random['categories'] = categories
                temp_random['variables'] = variables
                temp_random['periodicity'] = periodicity
                all_data_random['dataDetails'] = temp_random
                # print("======all_data_random=========")
                # pprint.pprint(all_data_random)
                if(temp_random):
                    data_db.update_one({'workspaceId' : workspace_id}, {"$set" : {"data": all_data_random }})
                #update random data in the data db ends
                #########
            #new random data formation ends
            #######################
            if len(statement) >= 1:
                parse_story = ParserUserStory()
                user_story = statement
                widget_payload , user_story, consolidatedData = parse_story.text_to_structure(user_story, currency)
                consolidatedData_df = pd.DataFrame(consolidatedData)
                #########################
                #new chart logic starts
                #inputs needed for new chart logic
                periodicity = consolidatedData["dataDetails"]["periodicity"]
                dataSet =  consolidatedData["dataSet"]
                chart_viewer = ChartViewer()
                print("=====new chart starts============")
                dimension = []
                # #############
                # #chartInput Starts - get chart view starts
                # widget_payload[0]['recommendedCharts'][0] = {
                #     'actualChartType': ' ',
                #     'aggregationType': 'sum',
                #     'chartTitle': {'Categorical Dimension': len(categorical_dimension),
                #                         'Goal Measure': len(goal_measure),
                #                         'Measure': len(measure),
                #                         'Timeline Dimension': len(timeline_Dimension)},
                #     'currencies': '$',
                #     'userStory': statement,
                #     'xAxisDataKeys': categorical_dimension,
                #     'yAxisDataKeys': goal_measure + measure
                #     }
                # #chartInput Starts - get chart view starts
                # #############
                if len(widget_payload[0]['recommendedCharts'])>=1:
                    aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(widget_payload[0]['recommendedCharts'][0], dataSet, periodicity, dimension)
                if len(widget_payload[0]['recommendedCharts'])<=0:
                    widget_payload[0]['recommendedCharts']= [
                        {
                        
                        'actualChartType': ' ',
                        'aggregationType': 'sum',
                        'chartTitle': {'Categorical Dimension': len(categorical_dimension),
                                        'Goal Measure': len(goal_measure),
                                        'Measure': len(measure),
                                        'Timeline Dimension': len(timeline_Dimension)},
                        'currencies': '$',
                        'userStory': statement,
                        'xAxisDataKeys': categorical_dimension,
                        'yAxisDataKeys': goal_measure + measure
                    }
                    ]
                    aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(widget_payload[0]['recommendedCharts'][0], dataSet, periodicity, dimension)

                new_aggregated_dataset_df  = pd.DataFrame(consolidatedData["dataSet"]["tableData"])
                new_aggregated_dataset = new_aggregated_dataset_df.to_dict("list")
                aggregated_dataset = new_aggregated_dataset
                widget_payload[0]['recommendedCharts'][0]["aggregatedDataset"] = aggregated_dataset
                widget_payload[0]['recommendedCharts'][0]["chartType"] = chartType
                
                #finding the dimension starts
                aggregated_dataset_df = pd.DataFrame(aggregated_dataset)
                aggregated_dataset_keys = aggregated_dataset.keys()  
                tabular_data_keys = ((list(aggregated_dataset_keys)))
                listToStr = ' '.join(map(str, tabular_data_keys))
                parse_story = ParserUserStory()
                dimension = parse_story.text_to_structure(listToStr, currency)
                if len(dimension[0])>=1:
                    dimension = (dimension[0][0]["chartDimension"])
                elif len(dimension[0]) == 0:
                    dimension = {'Categorical Dimension' : [], 'Measure' : [], 'Goal Measure' : [], 'Timeline Dimension' : [] }
                categorical_dimension = dimension['Categorical Dimension']
                measure = dimension['Measure']
                goal_measure= dimension['Goal Measure']
                timeline_Dimension = dimension['Timeline Dimension']
                #finding the dimension ends
                # print("==============widget_payload / recommendedCharts / newkeyword===============")
                # pprint.pprint(widget_payload[0]['recommendedCharts'])
                # print("=====new chart ends============")
                #newChart - Diff Charts - aggregated dataset
                if input_["chartType"] == "pieChart":
                            aggregated_dataset, aggregated_function = chart_viewer.get_chart_view(widget_payload[0]['recommendedCharts'][0], dataSet, periodicity)
                            # df = pd.DataFrame(aggregated_dataset)
                            # aggregated_dataset = df.to_dict("list")
                            widget_payload[0]['recommendedCharts'][0]["aggregatedDataset"] = aggregated_dataset
                            data = { "data" : widget_payload[0]['recommendedCharts'][0] }
                            payload = data
                            message = "success"
                if input_['chartType'] == 'numberTile':
                    if len(widget_payload[0]['recommendedCharts'][0]["yAxisDataKeys"]) >= 1:
                        widget_payload[0]['recommendedCharts'][0]["xAxisDataKeys"] = widget_payload[0]['recommendedCharts'][0]["yAxisDataKeys"] 
                        widget_payload[0]['recommendedCharts'][0]["yAxisDataKeys"] = []
                    else:
                        widget_payload[0]['recommendedCharts'][0]["xAxisDataKeys"] = widget_payload[0]['recommendedCharts'][0]["xAxisDataKeys"]
                    aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(widget_payload[0]['recommendedCharts'][0], dataSet, periodicity, dimension)     
                    widget_payload[0]['recommendedCharts'][0]["aggregatedDataset"] = aggregated_dataset
                    # print("==============recommendedCharts / tabular chart===============")
                    # pprint.pprint(widget_payload[0]['recommendedCharts'])
                    if widget_payload[0]['recommendedCharts'][0]["xAxisDataKeys"][0] in variable_logos.keys():
                                    input_['tileLogo'] = variable_logos[widget_payload[0]['recommendedCharts'][0]["xAxisDataKeys"][0]]
                    df = pd.DataFrame(aggregated_dataset)
                    aggregated_dataset = df.to_dict("list")
                    widget_payload[0]['recommendedCharts'][0]["aggregatedDataset"] = aggregated_dataset
                    data = { "data" : widget_payload[0]['recommendedCharts'][0]
                    }
                    payload = data
                    message = "success"           
                if input_["chartType"] == "tabularChart":
                    if len(widget_payload[0]['recommendedCharts'][0]["aggregatedDataset"]) >=1 :
                        widget_payload[0]['recommendedCharts'][0]["aggregatedDataset"] = widget_payload[0]['recommendedCharts'][0]["aggregatedDataset"]
                    else:
                        a = ( widget_payload[0]['recommendedCharts'][0]["xAxisDataKeys"])+( widget_payload[0]['recommendedCharts'][0]["yAxisDataKeys"])
                        combined_dataset = consolidatedData["dataSet"]["tableData"]
                        combined_dataset_df = pd.DataFrame(combined_dataset)
                        combined_dataset_df = combined_dataset_df[a]
                        aggregated_dataset = combined_dataset_df.to_dict("list")
                        widget_payload[0]['recommendedCharts'][0]["aggregatedDataset"] = aggregated_dataset
                    aggregated_dataset_df = pd.DataFrame(aggregated_dataset)
                    aggregated_dataset_keys = aggregated_dataset.keys()  
                    tabular_data_keys = ((list(aggregated_dataset_keys)))
                    listToStr = ' '.join(map(str, tabular_data_keys))
                    parse_story = ParserUserStory()
                    dimension = parse_story.text_to_structure(listToStr, currency)
                    dimension = (dimension[0][0]["chartDimension"])
                    b = dimension
                    c = b
                    d = list(c.values())
                    # z = list(map(str,str(d)))
                    z = [x for x in d if len(x)>=1] #for getting only the values with not empty list
                    y = list(itertools.chain(*z))
                    y = [y.replace(('Y' or 'M'), 'Period') for y in y]
                    final_agg_list = {}
                    for n, i in enumerate(y):
                        if i == "M":
                            y[n] = "Period"
                        if i == "Y":
                            y[n] = "Period"
                    y = [sub.replace('M', 'Period') for sub in y]
                    y = [sub.replace('Y', 'Period') for sub in y]
                    for i in y:
                        final_agg_list[i] = aggregated_dataset[i]
                        print("=======final_agg_list=======")
                        print(final_agg_list)
                    widget_payload[0]['recommendedCharts'][0]["aggregated_dataset_order"] = (list(final_agg_list.keys()))
                    payload = { "data" : widget_payload[0]['recommendedCharts'][0]}
                    message = "success"
                if input_["chartType"] != "tabularChart" and input_["chartType"] != "pieChart" and input_["chartType"] != "numberTile":
                    aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(widget_payload[0]['recommendedCharts'][0], dataSet, periodicity, dimension)
                    new_aggregated_dataset_df  = pd.DataFrame(aggregated_dataset)
                    new_aggregated_dataset = new_aggregated_dataset_df.to_dict("list")
                    aggregated_dataset = new_aggregated_dataset
                    widget_payload[0]['recommendedCharts'][0]["aggregatedDataset"] = aggregated_dataset
                    payload = { "data" : widget_payload[0]['recommendedCharts'][0]}
                    message = "success"
            if len(statement) == 0:
                print("======len is 0 =======")
                keyword_input = {}
                keyword_input["xAxisDataKeys"] =  categorical_dimension
                keyword_input["yAxisDataKeys"] = goal_measure + measure
                aggregationType = "sum"
                keyword_input["chartType"] = chartType
                keyword_input["actualChartType"] = actualChartType
                if len(timeline_Dimension)>=1:
                    if timeline_Dimension[0] == "Y":
                        timeline_Dimension[0] ="Years"
                    if timeline_Dimension[0] == "M":
                        timeline_Dimension[0] ="Months"
                    if timeline_Dimension[0] == "D":
                        timeline_Dimension[0] ="Days"
                user_story = categorical_dimension + goal_measure + measure + timeline_Dimension
                str1 = " "
                str1 = str1.join(user_story)
                user_story = str1
                parse_story = ParserUserStory()
                widget_payload , user_story, consolidatedData = parse_story.text_to_structure(user_story, currency)
                #############
                #chartInput Starts - get chart view starts
                aggregatedDataset_random_df = pd.DataFrame(random_data['tableData'])
                aggregatedDataset_random = aggregatedDataset_random_df.to_dict("list")
                # NOTE : widget_payload[0]['recommendedCharts'][0] is changed to newKeyword_data_input
                newKeyword_data_input = [{
                    'actualChartType': ' ',
                    'aggregationType': 'sum',
                    'chartType' : chartType,
                    'chartTitle': {'Categorical Dimension': len(categorical_dimension),
                                        'Goal Measure': len(goal_measure),
                                        'Measure': len(measure),
                                        'Timeline Dimension': len(timeline_Dimension)},
                    'aggregatedDataset' : aggregatedDataset_random,
                    'currencies': '$',
                    'userStory': statement,
                    'xAxisDataKeys': categorical_dimension,
                    'yAxisDataKeys': measure + goal_measure
                    }]
                #chartInput Starts - get chart view starts
                #############
                
                consolidatedData_df = pd.DataFrame(consolidatedData)
                periodicity = consolidatedData["dataDetails"]["periodicity"]
                dataSet =  consolidatedData["dataSet"]
                chart_viewer = ChartViewer()
                # print("=====new chart starts============")
                dimension = []
                dataSet = random_data
                aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(newKeyword_data_input[0], random_data, periodicity, dimension)
                # new_aggregated_dataset_df  = pd.DataFrame(consolidatedData["dataSet"]["tableData"])
                # new_aggregated_dataset = new_aggregated_dataset_df.to_dict("list")
                # aggregated_dataset = new_aggregated_dataset
                newKeyword_data_input[0]["aggregatedDataset"] = aggregated_dataset
                newKeyword_data_input[0]["chartType"] = chartType
                if input_["chartType"] == "pieChart":
                            aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(newKeyword_data_input[0], dataSet, periodicity, dimension)
                            # df = pd.DataFrame(aggregated_dataset)
                            # aggregated_dataset = df.to_dict("list")
                            newKeyword_data_input[0]["aggregatedDataset"] = aggregated_dataset
                            data = { "data" : newKeyword_data_input[0]
                            }
                            payload = data
                            message = "success"
                if input_['chartType'] == 'numberTile':
                    if len(newKeyword_data_input[0]["yAxisDataKeys"]) >= 1:
                        newKeyword_data_input[0]["xAxisDataKeys"] = newKeyword_data_input[0]["yAxisDataKeys"] 
                        newKeyword_data_input[0]["yAxisDataKeys"] = []
                    else:
                        newKeyword_data_input[0]["xAxisDataKeys"] = newKeyword_data_input[0]["xAxisDataKeys"]
                    aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(newKeyword_data_input[0], dataSet, periodicity, dimension)     
                    newKeyword_data_input[0]["aggregatedDataset"] = aggregated_dataset
                    if newKeyword_data_input[0]["xAxisDataKeys"][0] in variable_logos.keys():
                                    input_['tileLogo'] = variable_logos[newKeyword_data_input[0]["xAxisDataKeys"][0]]
                    df = pd.DataFrame(aggregated_dataset)
                    aggregated_dataset = df.to_dict("list")
                    newKeyword_data_input[0]["aggregatedDataset"] = aggregated_dataset
                    data = { "data" : newKeyword_data_input[0]
                    }
                    payload = data
                    message = "success"
                if input_["chartType"] == "tabularChart":
                    if len(newKeyword_data_input[0]["aggregatedDataset"]) >=1 :
                        newKeyword_data_input[0]["aggregatedDataset"] = newKeyword_data_input[0]["aggregatedDataset"]
                    else:
                        a = ( newKeyword_data_input[0]["xAxisDataKeys"])+( newKeyword_data_input[0]["yAxisDataKeys"])
                        combined_dataset = consolidatedData["dataSet"]["tableData"]
                        combined_dataset_df = pd.DataFrame(combined_dataset)
                        combined_dataset_df = combined_dataset_df[a]
                        aggregated_dataset = combined_dataset_df.to_dict("list")
                        newKeyword_data_input[0]["aggregatedDataset"] = aggregated_dataset
                    aggregated_dataset_df = pd.DataFrame(aggregated_dataset)
                    aggregated_dataset_keys = aggregated_dataset.keys()  
                    tabular_data_keys = ((list(aggregated_dataset_keys)))
                    listToStr = ' '.join(map(str, tabular_data_keys))
                    parse_story = ParserUserStory()
                    dimension = parse_story.text_to_structure(listToStr, currency)
                    dimension = (dimension[0][0]["chartDimension"])
                    b = dimension
                    c = b
                    d = list(c.values())
                    # z = list(map(str,str(d)))
                    z = [x for x in d if len(x)>=1] #for getting only the values with not empty list
                    y = list(itertools.chain(*z))
                    y = [y.replace(('Y' or 'M'), 'Period') for y in y]
                    final_agg_list = {}
                    for n, i in enumerate(y):
                        if i == "M":
                            y[n] = "Period"
                        if i == "Y":
                            y[n] = "Period"
                    y = [sub.replace('M', 'Period') for sub in y]
                    y = [sub.replace('Y', 'Period') for sub in y]
                    for i in y:
                        final_agg_list[i] = aggregated_dataset[i]
                    newKeyword_data_input[0]["aggregated_dataset_order"] = (list(final_agg_list.keys()))
                    payload = { "data" : newKeyword_data_input[0]}
                    message = "success"
                if input_["chartType"] != "tabularChart" and input_["chartType"] != "pieChart" and input_["chartType"] != "numberTile":
                    newKeyword_data_input[0]["aggregatedDataset"] = aggregated_dataset
                    payload = { "data" : newKeyword_data_input[0]}
                    message = "success"
                if input_["chartType"] == "kpiTile":
                    aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(newKeyword_data_input[0], dataSet, periodicity, dimension)
                    newKeyword_data_input[0]["aggregatedDataset"] = aggregated_dataset
                    payload = { "data" : newKeyword_data_input[0]}
                    message = "success"

            #new chart logic ends
            #########################
            ##############################
            #db updation - all Data db starts
            # print("======final payload / keyword=====")
            # pprint.pprint(payload)
            # print("=============db updation===============")
            # pprint.pprint(({'workspaceId': workspace_id,
            #                       'widgetName': "Keyword Widget",
            #                       'userStory': statement,
            #                       'chartData': {"recommendedCharts" : [payload["data"]],
            #                                     "chartDimension" : { "Measure" : measure, "Goal Measure" : goal_measure, "Categorical Dimension" : categorical_dimension, "Timeline Dimension" : timeline_Dimension }
            #                       },
            #                       'currencies':currency } ))
            widget_db.insert_one({'workspaceId': workspace_id,
                                  'widgetName': "Keyword Widget",
                                  'userStory': statement,
                                  'chartData': {"recommendedCharts" : [payload["data"]],
                                                "categories" : consolidatedData['dataDetails']['categories'],
                                                "variables" :consolidatedData['dataDetails']['variables'],
                                                "periodicity": consolidatedData['dataDetails']['periodicity'],
                                                "chartDimension" : { "Measure" : measure, "Goal Measure" : goal_measure, "Categorical Dimension" : categorical_dimension, "Timeline Dimension" : timeline_Dimension }
                                  },
                                  'currencies':currency } )
            widget_new_Id = widget_db.find({"workspaceId" :workspace_id })
            for w_data in widget_new_Id:
                temp = {}
                temp['widgetId'] = str(w_data.get('_id',None))
            #forming the new widget and getting the Id ends 
            keyword_data = {    'recommendedCharts': [payload["data"]],
                                'userStory': statement,
                                'widgetId': temp['widgetId'],
                                'widgetName' : "Keyword Widget"
                }
            all_data['allData']["chartData"].insert(0, keyword_data)
            # print("=======all_data=======")
            # pprint.pprint(all_data)
            all_data_db.update_one({'workspaceId' : workspace_id}, {"$set" : {"chartData": all_data['allData']['chartData'] }})
            all_data_db.update_many({'workspaceId' : workspace_id}, {"$set" : {"chartData": all_data['allData']['chartData'] }})
            #db updation - all Data db ends
            ##############################
            #  layout starts
            chart_layout_dict, chart_layout_array = {"i": "", "x": 0, "y": 0, "w": 4, "h": 10, "minW": 4, "minH": 9, "maxH": 10}, []
            layout_x = [0,4,8]
            single_dataSet = data_db.find_one({"workspaceId" : workspace_id})
            chart_layout_array = single_dataSet.get('layout', [])
            single_dataSet = single_dataSet['data']['dataSet']
            chart_viewer = ChartViewer()
            currency = "$"
            final_chart_out = []
            chart_dict = {}
            chart_data = all_data['allData']['chartData'][-1]
            aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(chart_data, single_dataSet, dimension)
            chart_data['aggregatedDataset'] = aggregated_dataset
            chart_data['aggregationType'] = aggregated_function
            chart_data['userStory'] = statement
            chart_data['currencies'] = currency
            final_chart_out.append(chart_data)
            chart_dict['recommendedCharts'] = final_chart_out
            if chart_layout_array:
                i = len(chart_layout_array)
                layout = chart_layout_dict.copy()
                layout["i"] = list(string.ascii_lowercase)[i]
                layout["x"] = layout_x[i%3]
                if chart_dict["recommendedCharts"] in ["numberTile","kpiTile"]:
                    if (chart_dict["recommendedCharts"][0]['recommendedCharts'][0]['chartType'])in ["numberTile","kpiTile"]:
                        layout["h"] = 5
                        layout["minH"] = 5
                chart_layout_array.append(layout)
            data_db.update_one({'workspaceId' : workspace_id}, {"$set" : {"layout": chart_layout_array}})
            # layout ends
            ######################################################################
            
            status = 200
            is_success = True
            # output =  {"workspaceId" : workspace_id, "Categorical_Dimension" : categorical_dimension,
                    #    "Goal_Measure" :  goal_measure, "statement" : statement, "Suggested_Chart" : suggested_chart, "Timeline_Dimension" : timeline_dimension }
            response['message'] = message
            response['success'] = True
            response['payload'] = [payload["data"]]
         except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
         finally:
            return(make_response(response, status))
     


#class second Instance starts
class SecondInstance(Resource):
    # @app.route('/second_instance',methods=['POST'])
    def post(self):
        try: 
            ################
            # ruler = nlp.add_pipe("attribute_ruler")
            # patterns = [[{"ORTH": "-"}]]
            # attrs = {"TAG": "HYPH", "POS": "PUNCT"}
            # ruler.add(patterns=patterns, attrs=attrs)
            ################
            response = {'success' : False, 'message' : 'Failure', "payload" : ''}
            second_instance_db = mydb["second_instance"]
            second_instance = request.form['NERtext']
            # r = nlp(second_instance.lower())
            r = None
            l=[{ent.label_: ent.text} for ent in r.ents]
            # l = tuple(l)
            # l = {k:v for t in l for k,v in t.items()}
            output = []
            secondinstance = {}
            if len(l)>0:
                secondinstance=dict( {"predictions" :l, 'query': second_instance})
                output.append(secondinstance)
                print(output)
            output = tuple(output)
            output = {k:v for t in output for k,v in t.items()}
            second_instance_db.insert_one({'data' : output})
            success = False if not output else True
            message = 'Failure' if not output else 'Success'
            response['payload'] = output
            response['success'] = success
            response['message'] = message
            status = 200
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))
    
    # @app.route('/second_instance',methods=['GET'])
    def get(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : {}}
            status, is_success = 400, False
            second_instance_db = mydb["second_instance"]
            res= {}       
            second_instance_id = str(request.args.get('second_instance_Id'))
            out = second_instance_db.find_one({'_id' : ObjectId(f"{second_instance_id}")})
            if not out:
                response['payload'] = {}
            else:
                res = {"second_instance_id" : second_instance_id},out['data']
                response['payload'] = res
            status = 200
            response['message'] = 'success'
            response['success'] = True
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

    def put(self):
        response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
        try:
            status, is_success = 400, False
            second_instance_db = mydb["second_instance"]
            second_instance_datas = request.json
            _id = second_instance_datas.get('second_instance_Id', None)
            data_second_instance = second_instance_datas.get('data', None)
            second_instance_data = second_instance_db.find_one({"_id":ObjectId(f"{_id}")})
            if not second_instance_data:
                message = "Second Instance Id does not exist"
            else:
                second_instance_db.update_one({
                    '_id': ObjectId(f"{_id}")
                        },{"$set": {"data": data_second_instance}})
                message = " Second Instance Updated Successfully"
                    
            status, is_success = 200, True
            response['message'] = message
            response['success'] = is_success
            response['payload'] = message
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

    # @app.route('/second_instance', methods=['DELETE'])
    def delete(self):
        try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            second_instance_db = mydb["second_instance"]
            _id = request.args.get('second_instance_Id')
            if(list(second_instance_db.find({"_id":ObjectId(f"{_id}")}))):
                myquery = {"_id":ObjectId(f"{_id}")}
                second_instance_db.delete_one(myquery)
                message = 'Second Instance ID has been deleted'
                status = 200
                is_success = True
            else:
                message = "Second Instance ID doesn't exists"
                status = 400
            response['message'] = message
            response['success'] = is_success
            response['payload'] = message
                
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

#class second Instance ends


#Class NewDrillDown starts
class NewDrillDown(Resource):
     # @app.route('/newDrillDown', methods=['POST'])
     '''
     This is for drill down.
     Input:
     workspaceId, 
     widgetId
     '''
     def post(self):
         try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            input_ = request.json
            all_data_db = mydb["all_data_db"]
            widget_db = mydb["widget"]
            data_db = mydb["data"]
            workspace_id = input_.get("workspaceId", None)
            widget_id = input_.get("widgetId", None)
            hierarchial_order = input_.get("hierarchial_order", [])
            all_data = data_db.find_one({"workspaceId" :workspace_id})
            widget_data = widget_db.find_one({"_id":ObjectId(f"{widget_id}")})
            tableData = all_data['data']['dataSet']['tableData']
            tableData_df = pd.DataFrame(tableData)
            aggregated_data = tableData_df.to_dict(orient = "list") 
            chartDimension = widget_data['chartData']['chartDimension']
            #dimension only for get_tabular_chart starts
            dimension_values_list = list(aggregated_data.keys())
            dimension_values_list_str = ' '.join(map(str, dimension_values_list))
            parse_story = ParserUserStory()
            currency = '$'
            dimension = parse_story.text_to_structure(dimension_values_list_str, currency)
            dimension = (dimension[0][0]["chartDimension"])
            chartDimension = dimension
            #dimension only for get_tabular_chart ends
            ###adding the period if not created starts
            if chartDimension['Timeline Dimension'] == "M" or "Y" or "D":
                chartDimension['Timeline Dimension'] = ["Period"]
            if 'Period' in aggregated_data.keys() and 'Period' not in chartDimension['Timeline Dimension']:
                chartDimension['Timeline Dimension'] = ["Period"]
            ###adding the period if not created ends 
            if widget_data['chartData']['recommendedCharts'][0]['chartType'] == "tabularChart":
                drilldown_out = drill_down.DrillDown.tabular_drillDown(aggregated_data, hierarchial_order, chartDimension)
            #updating the DB Starts
            widget_data['chartData']['recommendedCharts'][0]['DrillDownValues'] = { 'DrillDownValues' : drilldown_out }
            myquery = {"_id" : ObjectId(f"{widget_id}")}
            newvalues = { "$set": {"chartData": widget_data['chartData']}}
            widget_db.update_many(myquery, newvalues)
            #updating the DB ends
            message = "Success"
            status = 200
            is_success = True
            output =  {"widgetId" : widget_id, 'DrillDownValues' : drilldown_out}
            response['message'] = message
            response['success'] = True
            response['payload'] = output
         except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
         finally:
            return(make_response(response, status))

#Class NewDrillDown ends


#Class DesignDocument starts
class DesignDocument(Resource):
     # @app.route('/designDocument', methods=['POST'])
     '''
     This is for designDocument.
     Input:

     '''
     def post(self):
         try:
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            input_ = request.json
            all_data_db = mydb["all_data_db"]
            widget_db = mydb["widget"]
            data_db = mydb["data"]
            user_db = mydb["users"]
            workspace_db = mydb["workspace"]
            workspace_id = input_.get("workspaceId", None)
            all_data = all_data_db.find_one({"workspaceId": workspace_id})
            workspace_data = workspace_db.find_one({"_id":ObjectId(f"{workspace_id}")})
            #required fileds 
            date = datetime.now().date().strftime("%d - %m - %Y")
            version = 1.0
            workspace_data = workspace_db.find_one({"_id": ObjectId(f"{workspace_id}")})
            email = workspace_data["email"]
            user_data = user_db.find_one({"email" : email})
            user_name  = user_data["name"]
            overview = "Visual Workbench (VW) is a platform to create mockup dashboards rapidly based on the report requirements definition, the input to can be text or voice."
            number_of_dashboards_created =  len(all_data["allData"]["subDashDataList"])
            filter_data = ""
            if len(all_data["allData"]["parameterData"])>=1:
                filter_data = all_data["allData"]["filterOptions"]["categories"]
                # filter_data = [i["Categorical_Dimension"] for i in all_data["allData"]["parameterData"]]
                filter_data = GeneralFunc.removepunc(filter_data)
            allDashboard_data = []
            #updating mainDashboard starts
            allDashboard_data.append({"mainDashboardDetails" : 
                                                            
                                                            {
                                                                "title" : all_data["allData"]["workspaceName"],
                                                                "no_of_charts_created" : len(all_data["allData"]["chartData"]),
                                                                "userStory" : workspace_data["userStory"],
                                                                "chartData" : all_data["allData"]["chartData"],
                                                                "no_of_subDashboard" : len(all_data["allData"]["subDashDataList"]),
                                                                "filter_data" : filter_data
                                                            }
                                                        })
            allDashboard_data[0]["mainDashboardIndividualChartData"] = []
            count = 0
            for i in all_data["allData"]["chartData"]:
                    if len(i["recommendedCharts"])>=1:
                        allDashboard_data[0]["mainDashboardIndividualChartData"].append(
                                {
                                    "mainDashChartNo_" + str(count)  : {
                                        "title" : i["chartTitleData"],
                                        "x_axis" : i["recommendedCharts"][0]["xAxisDataKeys"],
                                        "y_axis" : i["recommendedCharts"][0]["yAxisDataKeys"],
                                        "chartType" : i["recommendedCharts"][0]["chartType"],
                                        "directColumns" : all_data["allData"]["singleDataSet"]["tableColumns"],
                                        "aggregatedDataset" : i["recommendedCharts"][0]["aggregatedDataset"],
                                        "userStory" : i["userStory"]
                                    }
                            }                      
                        )
                        count +=1
            #updating mainDashboard ends

            #updating subDashboard starts
            allDashboard_data[0]["allSubDash"] = []
            count = 1
            for subDashDataList in all_data["allData"]["subDashDataList"]:
                subDashboard_data = all_data_db.find_one({"workspaceId": subDashDataList['workspaceID']})
                count2 = 0
                for chartData in subDashboard_data["allData"]["chartData"]:
                    # for k in range(len(subDashboard_data["allData"]["chartData"])):
                        # print("==aa===")
                        # pprint.pprint(chartData["recommendedCharts"])
                        # print(len(chartData["recommendedCharts"]))
                        if len(chartData["recommendedCharts"])>=1:
                            # print("==bb===")
                            allDashboard_data[0]["allSubDash"].append({
                                                                "subDashNo_" + str(count) : 
                                                                {
                                                                            "chartNo_" + str(count2)  : 
                                                                            {
                                                                            "title" : chartData["chartTitleData"],
                                                                            "x_axis" : chartData["recommendedCharts"][0]["xAxisDataKeys"],
                                                                            "y_axis" : chartData["recommendedCharts"][0]["yAxisDataKeys"],
                                                                            "chartType" : chartData["recommendedCharts"][0]["chartType"],
                                                                            "directColumns" : subDashboard_data["allData"]["singleDataSet"]["tableColumns"],
                                                                            "aggregatedDataset" : chartData["recommendedCharts"][0]["aggregatedDataset"],
                                                                            "userStory" : chartData["userStory"],
                                                                            "workspaceId" : subDashboard_data["allData"]["workspaceId"]
                                                                            }
                                                                        }
                                                                })
                            count2 +=1
                count +=1
                # print("=======allDashboard_data========")
                # pprint.pprint(allDashboard_data[0]["allSubDash"])
                
            #updating subDashboard ends

            message = "Success"
            status = 200
            is_success = True
            output =  {"date" : date, "version" : version, "overview" : overview,"user_name" : user_name, "allDashboard_data" : allDashboard_data}
            print("=====output===")
            pprint.pprint(output)
            response['message'] = message
            response['success'] = True
            response['payload'] = output
         except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
         finally:
            return(make_response(response, status))

#Class DesignDocument ends


#Class SpeechToText starts
class SpeechToText(Resource):
    # @app.route('/speechToText', methods=['POST'])
    def post(self):
        try:
            total_time_start = time.time()
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success, output = 400, False, 'Failure'
            subscription_key = 'b0050fa2fd2a4ed59c01f3bbd915b376'
            #####
            encodedData = request.json
            wav_file = open(RECORDING_FILE_PATH+ RECORDING_OUTPUT_FILE_NAME, "wb")
            baseData = encodedData['data'].split('audio/wav;base64,')
            baseData = baseData[1]
            decoding_time_start = time.time()
            decode_string = base64.b64decode(baseData.strip())
            wavfile_time_start = time.time()
            wav_file.write(decode_string)
            wav_file_stop = time.time() - wavfile_time_start
            decoding_time_stop = time.time() - decoding_time_start
            #####
            def get_token(subscription_key):
                fetch_token_url = 'https://centralindia.api.cognitive.microsoft.com/sts/v1.0/issueToken'
                headers = {
                    'Ocp-Apim-Subscription-Key': subscription_key
                }
                result = requests.post(fetch_token_url, headers=headers)
                access_token = str(result.text)
                return access_token

            # print("transcription id generated successfully!!!"
            #     if get_token(subscription_key) else "error")

            url = "https://centralindia.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1?language=en-US"

            headers = {
            'Content-type': 'audio/wav;codec="audio/pcm";',
            'Ocp-Apim-Subscription-Key': subscription_key,
            }
            # RECORDING_FILE_PATH1 = "D:\\CTO\\SpeechToText\\DebiTeam\\AzureModel\\recording_output\\output_recording.wav"
            with open(RECORDING_FILE_PATH+ RECORDING_OUTPUT_FILE_NAME,'rb') as payload:
                azure_api_time_start = time.time()
                result = requests.request("POST", url, headers=headers, data=payload)
                azure_api_time_stop = time.time() - azure_api_time_start
                output = result.json()["DisplayText"]
                message = 'Success'
                status = 200
                is_success = True
            total_time_stop = time.time() - total_time_start
            time_taken = {'Total time' : total_time_stop, 'Decoding time' : decoding_time_stop,  'Azure API time'  : azure_api_time_stop, "wave file" : wav_file_stop}
            response['message'] = message
            response['time'] = time_taken
            response['success'] = is_success
            response['payload'] = output
            
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            response['message'] = "Error"
            status = 400
        finally:
            return(make_response(response, status))

#Class SpeechToText ends

#Class plotlyData starts
class PlotlyWorkspace(Resource):
    '''
    This is used for Plotly Workspace - Landing Page.
    '''
    # @app.route('/plotlyWorkspace', methods=['GET'])
    def get(self):
        '''
        Get all the workspace details from "ploty_workspace_db" and display it to the landing page of plotly.
        '''
        try:
            print("====workspace get ======")
            response = {'success' : False, 'message' : 'Failure', "payload" : 'Failure'}
            status, is_success = 400, False
            output=[]
            mycol = mydb["ploty_workspace_db"]
            for q in mycol.find({'email' :request.args.get('email')}):
                q["_id"] = str(q["_id"])
                q['workspaceId'] = str(q["_id"])
                output.append(q)
            output.reverse()
            print("========output========")
            pprint.pprint(output)
            response['message'] = 'success'
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

    
#Class plotlyData ends


api.add_resource(Users, '/user')
api.add_resource(Login, '/login')
api.add_resource(Workspaces, '/workspaces')
api.add_resource(Widget, '/widget')
api.add_resource(DataTable, '/data')
api.add_resource(AllData, '/allData')
api.add_resource(ChartSetting, '/chart_settings')
api.add_resource(NewChart, '/newchart')
api.add_resource(ChartMapping, '/chartmapping')
api.add_resource(ChartFilter, '/chartfilter')
api.add_resource(Uploader, '/uploader')
api.add_resource(Layout, '/layout')
api.add_resource(Training_Keywords, '/training_keywords')
api.add_resource(SecondInstance, '/second_instance')
api.add_resource(SellectAll, '/deleteSelected')
api.add_resource(CopyPaste, '/copypaste')
api.add_resource(ClientLogoUploader, '/clientLogoUploader')
api.add_resource(NewKeywords, '/newKeywords')
api.add_resource(DrillDown, '/drillDown')
api.add_resource(NewChartMapping, '/newChartMapping')
api.add_resource(NewDrillDown, '/newDrillDown')
api.add_resource(DesignDocument, '/designDocument')
api.add_resource(SpeechToText, '/speechToText')
api.add_resource(PlotlyWorkspace, '/plotlyWorkspace')



@app.route('/test')
def test():
    return "API Working"

if __name__ == '__main__':  
    logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    logging.Formatter.converter = time.localtime
    # use very short interval for this example, typical 'when' would be 'midnight' and no explicit interval
    handler = logging.handlers.TimedRotatingFileHandler(logs_storage, when="midnight", interval=1, backupCount=10)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    logger.info("Starting Flask")
    app.run(host='0.0.0.0', port = 7000,debug=True)
# , ssl_context=('vs.pem', 'vs_decrypted.pem')