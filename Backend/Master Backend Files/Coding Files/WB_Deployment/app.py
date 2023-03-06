# -*- coding: utf-8 -*-

#Loading required packages
from __future__ import unicode_literals, print_function
from flask import Flask, render_template,request,jsonify
import plac
import random
from pathlib import Path
import spacy
from tqdm import tqdm 
#from spellchecker import SpellChecker
import re
import os

NER_dir='E:\\CTO\\Vaas_Instance_two\\Vaas_BI\\backend\\WB_Deployment'

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
@app.route('/answer_ner',methods=['POST'])
def ner_ans():
    rev=request.form['NERtext']

    r=nlp(rev.lower())

    
    l=[{ent.text: ent.label_} for ent in r.ents]

    if len(l)>0:
        return jsonify({'predictions ':l, 'query': rev})
    else:
        return jsonify({'predictions ':l, 'query': rev})
    
    
if __name__ == "__main__":
    app.run()
    
#r.ents    
#import os
#from app import app
#if __name__ == '__main__':
#    app.run(debug = True)
    
#from whitenoise import WhiteNoise

#from app import app

#application = WhiteNoise(app)
#application.add_files('static/', prefix='static/')
    


##@app.route('/bb_cream_ans',methods=['POST'])
"""def bb_cream_ans():
    #rev=request.form['bbtext']

    r=nlp2(rev.lower())

    l=[ent.label_  for ent in r.ents]

    if len(l)>0:
        return render_template('bb_ans.html',answer="This product is a "+str(l[0]))
    else:
        return render_template('bb_ans.html',answer="Could not find product") """