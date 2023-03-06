import json
import os
from pyspark.ml import Pipeline
import pyspark.sql.functions as F
import pandas as pd

import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *

#spark = sparknlp.start(spark32=True) #If we are using spark = 3.2.0 and above
spark = sparknlp.start()

#print("Spark NLP version: ", sparknlp.version())
#print("Apache Spark version: ", spark.version)

spark

#userStory = str(input())
#userStory = 'I would like to visualize profit achieved last year and also the loss incurred, What is the targeted revenue and profit for the next year.'


def get_ann_model(userStory):

    sample_data = spark.createDataFrame([[userStory]]).toDF("text")
    document = DocumentAssembler()\
        .setInputCol("text")\
        .setOutputCol("document")

    sentence = SentenceDetector()\
        .setInputCols(['document'])\
        .setOutputCol('sentence')

    token = Tokenizer()\
        .setInputCols(['sentence'])\
        .setOutputCol('token')

    #glove_embeddings = WordEmbeddingsModel.pretrained('glove_100d')\
    glove_embeddings = WordEmbeddingsModel.load('cache_pretrained/glove_100d_en_2.4.0_2.4_1579690104032')\
        .setInputCols(["document", "token"])\
        .setOutputCol("embeddings")
  
# load trained model
    loaded_ner_model = NerDLModel.load("Vwb_NER_glove_e5_b32")\
        .setInputCols(["sentence", "token", "embeddings"])\
        .setOutputCol("ner")

    converter = NerConverter()\
        .setInputCols(["document", "token", "ner"])\
        .setOutputCol("ner_span")

    ner_prediction_pipeline = Pipeline(stages = [
        document,
        sentence,
        token,
        glove_embeddings,
        loaded_ner_model,
        converter
    ])

    empty_data = spark.createDataFrame([['']]).toDF("text")

    prediction_model = ner_prediction_pipeline.fit(empty_data)\

    preds = prediction_model.transform(sample_data)
     
    preds.select(F.explode(F.arrays_zip(preds.ner_span.result,preds.ner_span.metadata)).alias("entities")) \
      .select(F.expr("entities['0']").alias("chunk"),
              F.expr("entities['1'].entity").alias("entity")).show(truncate=False)
    
    pdf = preds.select(F.explode(F.arrays_zip(preds.ner_span.result,preds.ner_span.metadata)).alias("entities")) \
      .select(F.expr("entities['0']").alias("chunk"),
              F.expr("entities['1'].entity").alias("entity")).toPandas()
    
    parameter = [{'Categorical_Dimension' : [],
             'Goal_Measure' : [],
             'Measure' : [],
             'Timeline_Dimension' : []}]
    for i in range(len(pdf)):
        if pdf.loc[i, "entity"] == 'cat':
            parameter[0]['Categorical_Dimension'].append(pdf.loc[i, "chunk"])
        if pdf.loc[i, "entity"] == 'Goal':
            parameter[0]['Goal_Measure'].append(pdf.loc[i, "chunk"])
        if pdf.loc[i, "entity"] == 'Msr':
            parameter[0]['Measure'].append(pdf.loc[i, "chunk"])
        if pdf.loc[i, "entity"] == 'Tmln':
            parameter[0]['Timeline_Dimension'].append(pdf.loc[i, "chunk"])
    print(parameter[0])
    
    #print(pdf)
    
    #jason = pdf.to_json(orient = 'records')
    
    #print(jason)
    
    #print ("Spark NLP NER Entities are created")
    
    return parameter[0]

#get_ann_model()