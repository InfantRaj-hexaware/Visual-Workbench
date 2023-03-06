# -*- coding: utf-8 -*-

#Loading required packages
from __future__ import unicode_literals, print_function
from flask import Flask, render_template,request,jsonify
#import plac
import random
from pathlib import Path
import spacy
from tqdm import tqdm 
#from spellchecker import SpellChecker
import re
import os

NER_dir="D:\\CTO\\Second_Instance_V3\\NLP_Vaas_V3\\NLP_Vaas_V3\\output\\model-best"

nlp=spacy.load(NER_dir)
app = Flask(__name__)
#app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'XYZ')

#app.logger.addHandler(logging.StreamHandler(sys.stdout))
#app.logger.setLevel(logging.ERROR)


@app.route('/')
def index():
    print("everything is running fine")

#@app.route('/NER')
#def NER():
#    return render_template('NER.html')     #Show me revenue vs vertical

                                               #revenue-measure,vertical-
@app.route('/answer_ner_v2',methods=['POST'])
def ner_ans():
    rev=request.form['NERtext']

    r=nlp(rev.lower())

    
    l=[{ent.text: ent.label_} for ent in r.ents]
    print("==l===")
    print(l)
    new_list = []
    for i in l:
        dict = {value:key for key, value in i.items()}
        new_list.append(dict)
    
    if len(new_list)>0:
        return jsonify({"data" : {'predictions ':new_list, 'query': rev}})
    else:
        return jsonify({"data" : {'predictions ':new_list, 'query': rev}})
    
    
if __name__ == "__main__":
    app.run()
