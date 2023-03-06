# -*- coding: utf-8 -*-
### Load the package

#!pip install spellchecker
#from spellchecker import SpellChecker
from __future__ import unicode_literals, print_function
#import plac
import random
from pathlib import Path
import spacy
from tqdm import tqdm 
import spacy
import re
from spacy import displacy



"""### Create a model"""

nlp = spacy.load('en_core_web_md')

"""### Training DATA"""

TRAIN_DATA=[("plot Other income by vertical",{"entities":[(5,17,"Measure"),(20,29,"Categorical Dimension")]}),
("Can you plot my Operating Income for all my verticals ",{"entities":[(16,32,"Measure"),(44,53,"Categorical Dimension")]}),
("Operating Profit vs Vertical ",{"entities":[(0,16,"Measure"),(20,28,"Categorical Dimension")]}),
("How is my Cost of Sales varying across all my verticals ",{"entities":[(10,23,"Measure"),(46,55,"Categorical Dimension")]}),
("Plot my Revenue per day against my verticals ",{"entities":[(8,23,"Measure"),(35,44,"Categorical Dimension")]}),
("I would like to visualize my Net Profit Margin for different verticals ",{"entities":[(29,46,"Measure"),(61,70,"Categorical Dimension")]}),
("Show Gross Margin by vertical ",{"entities":[(5,17,"Measure"),(21,29,"Categorical Dimension")]}),
("Please plot Expense vs verticals ",{"entities":[(12,19,"Measure"),(23,32,"Categorical Dimension")]}),
("I would like to see my revenue for different verticals ",{"entities":[(23,30,"Measure"),(45,54,"Categorical Dimension")]}),
("Show revenue by Turnover",{"entities":[(5,12,"Measure"),(16,24,"Measure")]}),
("Please plot revenue vs Working Capital ",{"entities":[(12,19,"Measure"),(23,38,"Measure")]}),
("I would like to see my revenue for different Burn rate ",{"entities":[(22,30,"Measure"),(45,54,"Measure")]}),
("what is my Depreciation across different Operating Cash Flow ",{"entities":[(11,23,"Measure"),(41,60,"Measure")]}),
("Show me EBIT for all EBIT% ",{"entities":[(8,12,"Measure"),(21,26,"Measure")]}),
("plot Other income by EBITDA% ",{"entities":[(5,17,"Measure"),(21,28,"Measure")]}),
("Can you plot my Operating Income for all my Operating Profit% ",{"entities":[(16,32,"Measure"),(44,61,"Measure")]}),
("Operating Profit vs Gross Margin% ",{"entities":[(0,16,"Measure"),(20,33,"Measure")]}),
("How is my Cost of Sales varying across all my Profit Margin% ",{"entities":[(10,23,"Measure"),(46,60,"Measure")]}),
("Plot my Revenue per day against my Profit% ",{"entities":[(8,24,"Measure"),(35,42,"Measure")]}),
("I would like to visualize my Net Profit Margin for different Profit ",{"entities":[(29,46,"Measure"),(61,67,"Measure")]}),
("Show Gross Margin by Loss% ",{"entities":[(5,17,"Measure"),(21,26,"Measure")]}),
("Please plot Expense vs Loss ",{"entities":[(12,19,"Measure"),(22,27,"Measure")]}),
("I would like to see my revenue for different Tax ",{"entities":[(23,30,"Measure"),(45,48,"Measure")]}),
("Show revenue by Interest rate ",{"entities":[(5,12,"Measure"),(16,29,"Measure")]})
]

"""### Define our variables"""

model = None
output_dir=Path("E:\\CTO\\Vaas_Instance_two\\Vaas_BI\\backend\\WB_Deployment")
#if server add this in output_dir
# output_dir=Path("/home/ec2-user/Vaas_BI_2/backend/WB_Deployment")
n_iter=100

"""### Load the model"""

if model is not None:
    nlp = spacy.load(model)  
    print("Loaded model '%s'" % model)
else:
    nlp = spacy.blank('en')  
    print("Created blank 'en' model")

"""### Set-up the NER pipeline"""

if 'ner' not in nlp.pipe_names:
    ner = nlp.create_pipe('ner')
    nlp.add_pipe(ner, last=True)
else:
    ner = nlp.get_pipe('ner')

"""### Train the recognizer"""

for _, annotations in TRAIN_DATA:
    for ent in annotations.get('entities'):
        ner.add_label(ent[2])

other_pipes = [pipe for pipe in nlp.pipe_names if pipe != 'ner']
with nlp.disable_pipes(*other_pipes):
    optimizer = nlp.begin_training()
    for itn in range(n_iter):
        random.shuffle(TRAIN_DATA)
        losses = {}
        for text, annotations in tqdm(TRAIN_DATA):
            nlp.update(
                [text],  
                [annotations],  
                drop=0.5,  
                sgd=optimizer,
                losses=losses)
        print(losses)

"""### Test the trained model"""

for text, _ in TRAIN_DATA:
    doc = nlp(text)
    print('Entities', [(ent.text, ent.label_) for ent in doc.ents])
    print('Tokens', [(t.text, t.ent_type_, t.ent_iob) for t in doc])

"""### Save the Model"""

if output_dir is not None:
    output_dir = Path(output_dir)
    if not output_dir.exists():
        output_dir.mkdir()
    nlp.to_disk(output_dir)
    print("Saved model to", output_dir)
    
"""### Testing the saved model######"""

nlp=spacy.load(output_dir)


doc = nlp("I would like to see my revenue for different verticals ")
for ent in doc.ents:
    print(ent.text, ent.label_)
#displacy.render(nlp(doc.text),style='ent', jupyter=True)

"""doc = nlp("Show me sales for all Revenue")
for ent in doc.ents:
    print(ent.text, ent.start_char, ent.end_char, ent.label_)
displacy.render(nlp(doc.text),style='ent', jupyter=True)
doc = nlp("How is my Cost of Sales varying across all my verticals")
for ent in doc.ents:
    print(ent.text, ent.start_char, ent.end_char, ent.label_)
displacy.render(nlp(doc.text),style='ent', jupyter=True)
doc = nlp("Show me sales for all Revenue")
for ent in doc.ents:
    print(ent.text, ent.start_char, ent.end_char, ent.label_)
displacy.render(nlp(doc.text),style='ent', jupyter=True)
doc = nlp("Show me sales for all Revenue")
for ent in doc.ents:
    print(ent.text, ent.start_char, ent.end_char, ent.label_)
displacy.render(nlp(doc.text),style='ent', jupyter=True)
doc = nlp("Show me sales for all Revenue")
for ent in doc.ents:
    print(ent.text, ent.start_char, ent.end_char, ent.label_)
displacy.render(nlp(doc.text),style='ent', jupyter=True)
doc = nlp("Show me sales for all Revenue")
for ent in doc.ents:
    print(ent.text, ent.start_char, ent.end_char, ent.label_)
displacy.render(nlp(doc.text),style='ent', jupyter=True)
"""
