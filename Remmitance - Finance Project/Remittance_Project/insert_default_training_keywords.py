##excel to Mongo table
import pandas as pd
import pymongo
from pymongo import MongoClient

# Making a Connection with MongoClient
client = MongoClient("mongodb://localhost:27017/")
# database
db = client["local_mongo"]
# collection
keywords_db = db["training_keywords"]

# Read keywords 
keywords_db.delete_many({}) 
vaas_task_poc_list = pd.read_excel("Vaas_default_training_keywords.xlsx")
vaas_task_poc_list = vaas_task_poc_list.drop_duplicates(subset=['Keywords'], keep="last")
vaas_task_poc_list = vaas_task_poc_list.reset_index(drop=True)
keywords_dict = vaas_task_poc_list.to_dict("records")
keywords_db.insert_many(keywords_dict)

print("Inserted all default keywords")
