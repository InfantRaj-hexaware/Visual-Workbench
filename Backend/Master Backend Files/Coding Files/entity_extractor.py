import itertools 
from pickle import TRUE
import spacy
import pandas as pd
import re
from dateutil.relativedelta import relativedelta
import datetime
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize, sent_tokenize
from text2digits import text2digits
from pattern.text.en import singularize
import datetime
import pymongo
import traceback
import os
import copy
from spacy.pipeline import EntityRuler
import shutil
from chart_generator import Charts, ChartViewer
import data_generator
base_path = os.path.abspath(os.path.dirname(__file__))
model_path = os.path.join(base_path, "dictionary_model/")
import logging
import pprint
logger = logging.getLogger()
# nlp = spacy.load(model_path)

base_path = os.path.abspath(os.path.dirname(__file__))
base_train_loc = os.path.join(base_path,*["Uploads","ModelTrainers"])

class DBconn:
    def __init__(self):
        self.myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        self.mydb = self.myclient["local_mongo"]
        self.nlp = spacy.load(model_path)

    def reloadNLP(self):
        self.nlp = spacy.load(model_path)

class ParserUserStory(DBconn):
    
    def __init__(self):
        super(self.__class__, self).__init__()
        self.category_structure = {
                                    "categoryId" : "",
                                    "noOfSyntheticElements": 0,
                                    "syntheticElements": [],
                                    "elementName" : "",
                                    "isLinked": False,
                                    "prefix": "",
                                    "linkedElementName": "",
                                    "nonUniformElements": [
                                        {
                                        "syntheticElement": "",
                                        "elements": ""
                                        }
                                    ],
                                    "isUniform": True
                                    }
        self.period_structure = {
                                "startDate": "",
                                "endDate": "",
                                "frequency": ""
                                }
        self.variable_structure = {
                                "variableId" : "",
                                "precision": "",
                                "variableName":"",
                                "operator":"",
                                "low": "",
                                "high": "",
                                "dataType": "int",
                                "isLinked": False,
                                "dependentType": "",
                                "formula": "",
                                "percent": "%"
                             }
        self.dimension_chart = {'Measure' : 0, 'Goal Measure' : 0, 'Categorical Dimension' : 0, 'Timeline Dimension' : 0}
        self.dimension_template = {'Measure' : [], 'Goal Measure' : [], 'Categorical Dimension' : [], 'Timeline Dimension' : []}

        self.out_dict = {"categories" : [], "variables" : [], "periodicity" : {}, 'recommendedCharts' : []}

        self.variable_logos = {"Revenue" : "RevenueLogo", "Expense" : "ExpenseLogo", "Profit" : "ProfitLogo"}
        
        self.possibleChartList = ['numberTile', 'kpiTile', 'pieChart', 'treeMap', 'lineChart', 'multiLineChart', 'barChart', 'areaChart', 'scatterChart', 'stackedBarChart', 'waterflowChart', 'twoColumnStackedBarChart']
        self.charts_for_dimension_2 = {}
        self.charts_for_dimension_4 = {}

    def detectNumbers(self, text, user_story):
        try:
            combo_exp = '(\d+\s?{0})|({0}\s?\d+)'.format(text)
            search_match = re.search(combo_exp,user_story, re.IGNORECASE)
            if search_match:
                is_digit = re.findall(r'\d+', user_story[search_match.span()[0] : search_match.span()[1]])
                if is_digit:
                    return int(is_digit[0])
            else:
                if text.lower() == "gender":
                    return 3
                else:
                    return 5
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            return 5

    def getRandomStates(self, country_code, no_states):
        try:
            # countries = requests.get('http://api.geonames.org/countryInfoJSON?username=quarantineapp')
            PARAMS = {'username':"quarantineapp", 'geonameId':6255149}
            # states = requests.get('http://api.geonames.org/childrenJSON', params = PARAMS)
            # countries_dict = json.loads(countries.text)
            # for value in list(countries_dict.values())[0]:
            #     if value['countryCode'] == country_code:
            #         geonameId = value['geonameId']
            # PARAMS['geonameId'] = geonameId
            ## states = requests.get('http://api.geonames.org/childrenJSON', params = PARAMS)
            ## states_dict = json.loads(states.text)
            ## all_states = list(states_dict.values())[1]
            major_countries = ["United States", "United Kingdom", "China", "Russia", "Germany", "Spain", "Japan", "India", "France", "Canada"]
            stateNames = [major_countries[i] for i in range(0, no_states)]
            # stateNames = [state['name'] for state in random_states]
        except:
            stateNames = list(map(lambda x: country_code + str(x), range(1,no_states + 1)))
        return stateNames

    def getEYsubCategories(self, ey_cat, no_eys):
        try:
            dictEYvars = {'market': ["G360", "Focus core", "core", "GTER adjustments", "other"],
                        'service line': ["Assurance", "Tax", "Advisory", "TAS", "CBS&Elim", "Unclassified"],
                        'non gps core accounts stratification': ["< 50 K", "50K-100K", "100K-250K", "250K-500K", "500K-1Mil", "1Mil-5Mil", "5Mil-10Mil", "> 10 Mil"],
                        'stratification category': ["Other Core YTD", "Other Core PYTD", "Other Core PY"],
                        'region': ["Americas", "Asia-Pacific", "EMEIA"],
                        'account group': ["Top Down Accounts", "EP Planning exc Portfolio", "EP Portfolio"]}
            if no_eys == 5 or no_eys >= len(dictEYvars[ey_cat]):
                no_eys = len(dictEYvars[ey_cat])
            eySubCats = [dictEYvars[ey_cat][i] for i in range(0, no_eys)]
        except:
            eySubCats = list(map(lambda x: ey_cat + str(x), range(1,no_eys + 1)))
        return eySubCats, no_eys

    def category_maker(self,text, user_story):
        try:
            ##connecting to DB.
            keywords_db = self.mydb["training_keywords"]
            category_structure = self.category_structure.copy()
            no_of_synthetic_ele = self.detectNumbers(text, user_story)

            category_structure['noOfSyntheticElements'] = no_of_synthetic_ele
            if text.lower() in ['geography', 'geographies']:
                # text = 'country'
                category_structure['prefix'] = text
                syntheticElements = self.getRandomStates(text, no_of_synthetic_ele)
                category_structure['syntheticElements'] = syntheticElements
                category_structure['elementName'] = text
                category_structure['categoryId'] = text
            elif text.lower() in ['market', 'service line', 'non gps core accounts stratification', 'stratification category', 'region', 'account group']:
                category_structure['prefix'] = text.capitalize()
                syntheticElements, ey_no_of_synthetic_ele = self.getEYsubCategories(text, no_of_synthetic_ele)
                category_structure['noOfSyntheticElements'] = ey_no_of_synthetic_ele
                category_structure['syntheticElements'] = syntheticElements
                category_structure['elementName'] = text.capitalize()
                category_structure['categoryId'] = text.capitalize()
            else:
                category_structure['prefix'] = text.capitalize()
                keywords_db_data = keywords_db.find_one({"Keywords" : text.lower()})
                if keywords_db_data and type(keywords_db_data['Keywords_list']) != float:
                    if len(keywords_db_data['Keywords_list'])>=1:
                        syntheticElements = keywords_db_data['Keywords_list']
                        category_structure['syntheticElements'] = syntheticElements

                        print("=======keywords_db_data / yeskeywords_db_data=")
                        pprint.pprint(keywords_db_data)
                        print("====category_structure====")
                        pprint.pprint(category_structure)
                        print("=======type(keywords_db_data['numberOfKeywords'])=")
                        print(type(keywords_db_data['numberOfKeywords']))

                        if type(keywords_db_data['numberOfKeywords']) == float:
                            # category_structure['noOfSyntheticElements'] = int(keywords_db_data['numberOfKeywords'])
                            try:
                                category_structure['noOfSyntheticElements'] = int(keywords_db_data['numberOfKeywords'])
                            except:
                                category_structure['noOfSyntheticElements'] = int(0)
                                keywords_db_data['numberOfKeywords'] = int(0)
                                keywords_db_data['Keywords_list'] = []
                    # if pd.isna(keywords_db_data['numberOfKeywords']) == True and type(keywords_db_data['numberOfKeywords']) != float:
                    #     category_structure['noOfSyntheticElements'] = len(keywords_db_data['Keywords_list'])


                else:
                    print("-==no")
                    syntheticElements = list(map(lambda x: text.capitalize() + str(x), range(1,no_of_synthetic_ele + 1)))
                    category_structure['syntheticElements'] = syntheticElements
                category_structure['elementName'] = text.capitalize()
                category_structure['categoryId'] = text.capitalize()
            return category_structure
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())

    @staticmethod
    def unix_time_millis(dt):
            epoch = datetime.datetime.utcfromtimestamp(0)
            return (dt - epoch).total_seconds() * 1000.0

    @staticmethod
    def get_factor(numbers):
        '''
        get factor based on numbers and also check for year
        '''
        start_date_present = False
        if numbers:
            if numbers[0] > 1000:
                factor = 1
                start_date_present = True
            else:
                factor = numbers[0]
        else:
            factor = None
        return factor, start_date_present

    @staticmethod
    def check_sign(text):
        '''
        check the sign of text
        last means negative vice verse
        '''
        negative_terms = ['past','last']
        positive_terms = ['present','next']
        if any([i for i in text.split(' ') if i in negative_terms]):
            sign = '-'
        elif any([i for i in text.split(' ') if i in positive_terms]):
            sign = '+'
        else:
            sign = ''
        return sign

    @staticmethod
    def set_start_end_date(date1, date2):
        '''
        sort start date and end date
        '''
        date_lst = [date1, date2]
        date_lst = sorted(date_lst)
        return date_lst[0], date_lst[1]

    @staticmethod
    def check_for_a(user_story, word):
        '''
        This fuction will check for 'a' and return 1 if present
        
        Parameter
        --------
        user_story : str
            Entire user story
        word : str
            Word before which you need to search 'a' for

        '''
        try:
            is_a = lambda x, index : bool((x[index] == "a") or (x[index] == "an") or (x[index] == "this") or (x[index] == "thi"))
            matched_sequence = re.search(word,user_story)
            if matched_sequence:
                span = matched_sequence.span()
                unmatched = user_story[:span[0]] + user_story[span[1]:]
                unmatched_lst = list(filter(lambda x: bool(x.strip()), unmatched.split(" ")))
                if is_a(unmatched_lst, -1):
                    return 1
            word_lst = word.split(" ")
            if is_a(word_lst, 0):
                return 1
            return 0
        except:
            return 0
    ###############################################################
    #text to digit starts
    @staticmethod
    def text2int (text, numwords={}):
        if not numwords:
            units = [
            "zero", "one", "two", "three", "four", "five", "six", "seven", "eight",
            "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen",
            "sixteen", "seventeen", "eighteen", "nineteen",
            ]

            tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"]

            scales = ["hundred", "thousand", "million", "billion", "trillion"]

            numwords["and"] = (1, 0)
            for idx, word in enumerate(units):  numwords[word] = (1, idx)
            for idx, word in enumerate(tens):       numwords[word] = (1, idx * 10)
            for idx, word in enumerate(scales): numwords[word] = (10 ** (idx * 3 or 2), 0)

        ordinal_words = {'first':1, 'second':2, 'third':3, 'fifth':5, 'eighth':8, 'ninth':9, 'twelfth':12}
        ordinal_endings = [('ieth', 'y'), ('th', '')]

        textnum = text.replace('-', ' ')

        current = result = 0
        curstring = ""
        onnumber = False
        for word in textnum.split():
            if word in ordinal_words:
                scale, increment = (1, ordinal_words[word])
                current = current * scale + increment
                if scale > 100:
                    result += current
                    current = 0
                onnumber = True
            else:
                for ending, replacement in ordinal_endings:
                    if word.endswith(ending):
                        word = "%s%s" % (word[:-len(ending)], replacement)

                if word not in numwords:
                    if onnumber:
                        curstring += repr(result + current) + " "
                    curstring += word + " "
                    result = current = 0
                    onnumber = False
                else:
                    scale, increment = numwords[word]

                    current = current * scale + increment
                    if scale > 100:
                        result += current
                        current = 0
                    onnumber = True

        if onnumber:
            curstring += repr(result + current)

        return curstring
    #text to digit ends
    ###############################################################

    def period_maker(self,text, actual_text):
        
        print("actual_text   :   ",actual_text)
        print('text    :   ', text)
        struct = {
        "startDate": "",
        "endDate": "",
        "frequency": ""
        }
        #detecting a year starts
        # if actual_text:
        #     for ch in ['a','year']:
        #         actual_text = actual_text.replace('a year', '1 year')
        #         print("actual_text after  :   ",actual_text)
        #detecting a yeat ends
        #texttodigits starts - text
        text = self.text2int(text)
        text= text.replace("mon", "month")
        #texttodigits ends
        #texttodigits starts -actual text
        actual_text = self.text2int(actual_text)
        actual_text= actual_text.replace("mon", "month")
         #texttodigits ends -actual text
        epoch = datetime.datetime.utcfromtimestamp(0)

        def unix_time_millis(dt):
            return (dt - epoch).total_seconds() * 1000.0
        
        start_date, end_date = datetime.datetime.now(), datetime.datetime.now()
        type_ = None
        add_factor = {}
        alias = None
        start_date_present = False
        numbers = [int(i) for i in re.findall('\d*',text) if i.strip()]
        factor, start_date_present = self.get_factor(numbers)
        if not factor:
            factor = self.check_for_a(user_story = actual_text, word = text)
        print('res', numbers)
        if "month" in text:
            type_ = 'month'
            add_factor['months'] = 6 if not factor else factor
            alias = "M"
        elif "year" in text or "ytd" in text or "fy" in text:
            numbers = [int(i) for i in re.findall('\d*',actual_text) if i.strip()]
            factor,start_date_present =  self.get_factor(numbers)
            if start_date_present:
                start_date = datetime.datetime(numbers[0],1,1)
            if not factor:
                factor = self.check_for_a(user_story = actual_text, word = text)
                # start_date_present = True
            type_ = "year"
            add_factor['years'] = 5 if not factor else factor
            alias = "Y"
        elif "quarter" in text:
            type_ = "quarter"
            add_factor['months'] = 3 if not factor else factor*3
            alias = "Q"
        elif "day" in text:
            type_ = "day"
            add_factor['days'] = 6 if not factor else factor
            alias = "D"
        elif "week" in text:
            type_ = "day"
            add_factor['week'] = 1 if not factor else factor
            alias = "W"
            
        to_epoch = lambda x: x.timestamp()
        sign = self.check_sign(text)
        print(sign)
        print('years',start_date_present)
        if not start_date_present:
            if sign == '+':
                end_date = start_date + relativedelta(**add_factor)
                print("=========end 1==============")
            else:
                end_date = start_date - relativedelta(**add_factor)
                print("=========end 2==============")
        else:
            end_date = start_date
            print("=========end 3==============")
        start_date, end_date = self.set_start_end_date(start_date, end_date)
        
        struct['startDate'] = unix_time_millis(start_date)
        struct['endDate'] = unix_time_millis(end_date)
        struct['frequency'] = alias

        
        return struct
 
    def variable_maker(self,text, type_):
        # print("========inputs / variable_maker / entity_extractor=======")
        # print("text  :  ",text)
        # print("type_  :  ",type_)
        variable_structure = self.variable_structure.copy()
        type_ = 'Profit' if text == 'profit' else type_
        isCurrency = False
        if type_ == 'Currency':
            low = 1_000_000
            high = 10_000_000
            isCurrency = True
        elif type_ == 'Unit':
            low = 10
            high = 100
        elif type_ == '%':
            low = 0
            high = 1
        elif type_ == 'Profit':
            low = 100_000
            high = 1_000_000
            isCurrency = True
        elif type_ == 'Integer':
            low = 1000
            high = 25000
        elif type_ == 'percent' or type_ == 'Percent':
            low = 0
            high = 100
        elif type_ == 'Ratio' or type_ == 'ratio':
            low = 0
            high = 1
        else:
            low = 10
            high = 100
        variable_structure['low'] = low    
        variable_structure['high'] = high
        variable_structure['variableName'] = text.capitalize()
        variable_structure['variableId'] = text.capitalize()
        variable_structure['isCurrency'] = isCurrency
        variable_structure['variableInnerType'] = type_
        # if type_ == 'percent' or  type_ == '%':
        #     variable_structure['dataType'] = 'percent'
        # if type_ == 'ratio':
        #     variable_structure['dataType'] = 'ratio'
        # print("====variable_structure / period_maker / entity extractor=====")
        # pprint.pprint(variable_structure)
        # print("====inputs / variable_structure / entity extractor=====")
        # print(text, '\n', type_)
        return variable_structure
    
    def get_chart_data(self):
        mycol = self.mydb['charts_by_count']
        all_charts = [i for i in mycol.find({"no_of_dim" : 4})]
        if all_charts:
            all_charts = all_charts[0]
            all_charts = all_charts['data']
            
            for k,v in all_charts.items():
                new_key = eval(k)
                self.charts_for_dimension_4[new_key] = v
        all_charts = [i for i in mycol.find({"no_of_dim" : 4})]
        if all_charts:
            all_charts = all_charts[0]
            all_charts = all_charts['data']
            for k,v in all_charts.items():
                new_key = eval(k)
                self.charts_for_dimension_2[new_key] = v
        
    def consolidatedDataSet(self, lines):
        try:
            single_data_dict = copy.deepcopy(self.out_dict)
            allLinePeriod = []
            print("====lines====")
            print(lines)
            for line in lines:
                dimension_chart = copy.deepcopy(self.dimension_chart)
                line = line.lower()
                line = line.replace(".","")
                # line = " ".join([singularize(l) for l in line.split(" ")])
                doc = self.nlp(line)
                print("==doc====")
                print(doc)
                for ent in doc.ents:
                    ent_text = singularize(ent.text)
                    entity, type_ = ent.label_.split('_')
                    if type_ == "currency":
                        type_ = "Currency"
                    dimension_chart[entity] +=1
                    ##################
                    #Hard coding the words which are incorrectly recognized starts.
                    print("=====ent - i====")
                    print(ent)
                    ent_text = 'loss' if ent_text == 'los' else ent_text
                    ent_text = 'surplus' if ent_text == 'surplu' else ent_text
                    ent_text = 'business plan class' if ent_text == 'business plan clas' else ent_text
                    ent_text = 'gross epi' if ent_text == 'gross epus' else ent_text
                    type_ = "Currency" if ent_text == 'revenue' else type_
                    #Hard coding the words which are incorrectly recognized ends. 
                    # ##################
                    # print("======ent_text========")
                    # print(ent_text)
                    # print("======entity, type_========")
                    # print(entity, type_)
                    # print("======type_========")
                    # print(type_)
                    # print("======dimension_chart========")
                    # print(dimension_chart)
                    
                    if type_ == 'Categorical':
                        cat_dict = self.category_maker(ent_text, line)
                        if cat_dict['elementName'] not in [i_data['elementName'] for i_data in single_data_dict['categories']]:
                            single_data_dict['categories'].append(cat_dict)
                    elif type_ == 'Timeline':
                        period_dict = self.period_maker(ent_text, line)
                        allLinePeriod.append(period_dict)
                        # single_data_dict['periodicity'].update(period_dict)
                    else:
                        variable_dict = self.variable_maker(ent_text, type_)
                        if variable_dict['variableName'] not in [i_data['variableName'] for i_data in single_data_dict['variables']]:
                            single_data_dict['variables'].append(variable_dict)
                ###
                #to add profit with prefix and suffix starts.
                for word in lines[0][0:-1].split():
                        if "profit" in (" " + word + " "):
                            print("====word =====")
                            print(word)
                            ent_text = word
                            type_ = "Currency"
                            entity = "Measure"
                            dimension_chart[entity] +=1
                            # print("=====dimension_chart====")
                            # pprint.pprint(dimension_chart)
                            variable_dict = self.variable_maker(ent_text, type_)
                            if variable_dict['variableName'] not in [i_data['variableName'] for i_data in single_data_dict['variables']]:
                                single_data_dict['variables'].append(variable_dict)
                                print("======variable_dict 1=======")
                                pprint.pprint(variable_dict)
                #to add profit with prefix and suffix ends.
                ###
            # print("==========allLinePeriod============")
            # print(allLinePeriod)
            if allLinePeriod:
                new_period_dict = {}
                min_startDate = min([m['startDate'] for m in allLinePeriod])
                max_endDate = max([m['endDate'] for m in allLinePeriod])
                new_freq = "Y"
                freq_list = [period['frequency'] for period in allLinePeriod]
                if 'D' in freq_list:
                    new_freq = 'D'
                elif 'M' in freq_list:
                    new_freq = 'M'
                elif 'Q' in freq_list:
                    new_freq = 'Q'
                elif 'Y' in freq_list:
                    new_freq = 'Y'
                new_period_dict['startDate'] = min_startDate
                new_period_dict['endDate'] = max_endDate
                print("===============min_startDate==============")
                print(min_startDate)
                print("=============max_endDate===============")
                print(max_endDate)
                # for period in allLinePeriod:
                #     if period['startDate'] == min_startDate:
                #         new_period_dict['startDate'] = period['startDate']
                #     if period['endDate'] == max_endDate:
                #         new_period_dict['endDate'] = period['endDate']
                new_period_dict['frequency'] = new_freq
                single_data_dict['periodicity'].update(new_period_dict)
            single_data_set = data_generator.get_random_data(single_data_dict)
            return single_data_set, single_data_dict
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
         
    def text_to_structure(self,text : str, currency = '$'):
        try:
            # print("=======inputs /text_to_structure/ entity extractor=======")
            # print("text : ",text)
            lines = sent_tokenize(text)
            # print("====lines====")
            # print(lines)
            final_out = []
            user_story = lines.copy()
            single_data_set, single_data_dict = self.consolidatedDataSet(lines)
            # print("====single_data_set=====")
            # print(single_data_set)
            # print("=====single_data_dict=====")
            # print(single_data_dict)
            mainChart = []
            for line in lines:
                dimension_chart = copy.deepcopy(self.dimension_chart)
                dimension_template = copy.deepcopy(self.dimension_template)
                out_dict = copy.deepcopy(self.out_dict)
                line = line.lower()
                line = line.replace(".","")
                # line = " ".join([singularize(l) for l in line.split(" ")])
                doc = self.nlp(line.lower())
                # print("==doc==")
                # print(doc)
                # print(type(doc))
                # print("===text===")
                # print(text)
                
                for ent in doc.ents:
                    ent_text = singularize(ent.text)
                    entity, type_ = ent.label_.split('_')
                    dimension_chart[entity] +=1
                    if type_ == "currency":
                        type_ = "Currency"
                    ##################
                    #Hard coding the words which are incorrectly recognized starts.
                    # print("====ent_text 1====")
                    # print(ent_text)
                    # print("=====entity======")
                    # print(entity)
                    # print("====type_===")
                    # print(type_)
                    # print("=====ent - i====")
                    # print(ent)
                    ent_text = 'loss' if ent_text == 'los' else ent_text
                    ent_text = 'surplus' if ent_text == 'surplu' else ent_text
                    ent_text = 'business plan class' if ent_text == 'business plan clas' else ent_text
                    ent_text = 'gross epi' if ent_text == 'gross epus' else ent_text
                    #Hard coding the words which are incorrectly recognized ends. 
                    ##################
                    ######
                    #Prefix and suffix for profit starts.
                    for word in text.split():
                        if "profit" in (" " + word + " "):
                            print("yes")
                    #Prefix and suffix for profit ends.
                    ######
                    if type_ == 'Categorical':
                        cat_dict = self.category_maker(ent_text, line)
                        out_dict['categories'].append(cat_dict)
                        dimension_template[entity].append(cat_dict['elementName'])
                    elif type_ == 'Timeline':
                        period_dict = self.period_maker(ent_text, line)
                        out_dict['periodicity'].update(period_dict)
                        dimension_template[entity].append(period_dict['frequency'])
                    else:
                        variable_dict = self.variable_maker(ent_text, type_)
                        out_dict['variables'].append(variable_dict)
                        dimension_template[entity].append(variable_dict['variableName'])
                ###
                #to add profit with prefix and suffix starts.
                for word in lines[0][0:-1].split():
                        if "profit" in (" " + word + " "):
                            # print("====word =====")
                            # print(word)
                            ent_text = word
                            type_ = "Currency"
                            entity = "Measure"
                            dimension_chart[entity] +=1
                            variable_dict = self.variable_maker(ent_text, type_)
                            out_dict['variables'].append(variable_dict)
                            dimension_template[entity].append(variable_dict['variableName'])
                            # if variable_dict['variableName'] not in [i_data['variableName'] for i_data in single_data_dict['variables']]:
                            #     single_data_dict['variables'].append(variable_dict)
                            #     dimension_template[entity].append(variable_dict['variableName'])
                            # print("======variable_dict 1=======")
                            # pprint.pprint(variable_dict)
                #to add profit with prefix and suffix ends.
                ###
                print("dimension_chart--->   :  ",dimension_chart)
                print("line   :   ",line)
                charts = Charts()
                charts_suggested = charts.find_chart_type(dimension_chart, out_dict, line)
                chart_viewer = ChartViewer()
                random_data = single_data_set
                possible_charts = []
                # all_data = data_generator.get_random_data(out_dict)
                if charts_suggested:
                    if len(charts_suggested[0]['chartAlias'])>1:
                        possibleChartList = charts_suggested[0]['chartAlias']
                    else :
                        possibleChartList = self.possibleChartList
                    for each_chart in possibleChartList:
                        # print("=====each_chart===")
                        # print(each_chart)
                        possibleChartInput = {}
                        possibleChartInput['xAxisDataKeys'] = charts_suggested[0].get('xAxisDataKeys',[])
                        possibleChartInput['yAxisDataKeys'] = charts_suggested[0].get('yAxisDataKeys',[])
                        possibleChartInput['aggregationType'] = 'sum'
                        possibleChartInput['userStory'] = line.capitalize()
                        possibleChartInput['currencies'] = currency
                        possibleChartInput['chartType'] = each_chart
                        if each_chart == 'numberTile':
                            x_axis = possibleChartInput.get('xAxisDataKeys',[])
                            y_axis = possibleChartInput.get('yAxisDataKeys',[])
                            if y_axis:
                                possibleChartInput['xAxisDataKeys'] = y_axis
                                possibleChartInput['yAxisDataKeys'] = []
                            if possibleChartInput['xAxisDataKeys'][0] in self.variable_logos.keys():
                                possibleChartInput['tileLogo'] = self.variable_logos[possibleChartInput['xAxisDataKeys'][0]]
                        dimension = []
                        each_chart_aggregated_dataset, each_chart_aggregated_function, dimension = chart_viewer.get_chart_view(possibleChartInput, random_data, out_dict['periodicity'], dimension)
                        if each_chart_aggregated_dataset and not isinstance(each_chart_aggregated_dataset, str):
                            possibleChartInput['aggregatedDataset'] = each_chart_aggregated_dataset
                            possibleChartInput['aggregationType'] = each_chart_aggregated_function
                            possible_charts.append(possibleChartInput)
                    final_chart_out = []
                    for suggested in charts_suggested:
                        suggested['userStory'] = line.capitalize()
                        suggested['currencies'] = currency
                        chart_type = suggested.get('chartType',None)
                        if chart_type and chart_type == 'numberTile':
                            x_axis = suggested.get('xAxisDataKeys',[])
                            y_axis = suggested.get('yAxisDataKeys',[])
                            if y_axis:
                                suggested['xAxisDataKeys'] = y_axis
                                suggested['yAxisDataKeys'] = []
                            if suggested['xAxisDataKeys'][0] in self.variable_logos.keys():
                                suggested['tileLogo'] = self.variable_logos[suggested['xAxisDataKeys'][0]]
                        dimension = []
                        aggregated_dataset, aggregated_function, dimension = chart_viewer.get_chart_view(suggested, random_data, out_dict['periodicity'], dimension)
                        suggested['aggregatedDataset'] = aggregated_dataset
                        suggested['aggregationType'] = aggregated_function
                        final_chart_out.append(suggested)
                    # To have unique charts for Template view in 1st index
                    # loop_terminator = 0
                    # while final_chart_out[0]['chartType'] in mainChart and final_chart_out[0]['chartType'] not in ['numberTile', 'kpiTile']:
                    #     loop_terminator = loop_terminator+1
                    #     final_chart_out.append(final_chart_out.pop(0))
                    #     if loop_terminator > 8:
                    #         break
                    # mainChart.append(final_chart_out[0]['chartType'])
                    
                    charts_suggested = final_chart_out
                out_dict['recommendedCharts'] = charts_suggested
                out_dict['possibleCharts'] = possible_charts
                out_dict['chartDimension'] = dimension_template
                out_dict['userStory'] = line.capitalize()
                # out_dict['dataset'] = all_data
                final_out.append(out_dict)
            consolidatedData = {"dataSet": single_data_set, "dataDetails": single_data_dict}
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
        # print("*"*200)
        # print(final_out)
        # print("=======consolidatedData=========")
        # pprint.pprint(consolidatedData)

        return final_out, user_story, consolidatedData

    def sum_combination(self, num):
        '''
        This function gets a number and it splits the number into its all the combination of its sum.
        eg: i/p : 10
        o/p : [[5,5], [6,4],[7,3].....]
        '''
        b = []
        sum_list = []
        for i in range(0, num):
            b.append(i)
            for j in b:
                if i+j ==num:
                    final = [i,j]
                    final_reverse = [j,i]
                    sum_list.append(final)
                    sum_list.append(final_reverse)
        sum_list = list(k for k,_ in itertools.groupby(sum_list))
        return sum_list
        

class TrainMLModel(DBconn):
    def __init__(self):
        super(self.__class__, self).__init__()
        self.training_keywords_db = self.mydb['training_keywords']
        # super().__init__()
        # self.base_train_data = pd.read_excel(os.path.join(base_train_loc,"Vaas_task_PoC_v3.xlsx"),sheet_name="Sheet1")
        
        ## Getting training keywords from db
        training_data = list(self.training_keywords_db.find({}))
        self.base_train_data = pd.DataFrame(training_data)
        self.base_train_data.drop('_id', axis =1, inplace = True)
        self.base_train_data = self.base_train_data.dropna(how = 'all', axis = 1)
        self.base_train_data = self.base_train_data.reset_index(drop=True)
        self.train_df = self.base_train_data.copy()

        self.tense = ['last','past','present','next']
        self.period = ['month','months','monthly','year','years','yearly','day','days','daily','quarter','quarters','quarterly','fy']
        self.single = ['month','year','day','quarter','fy']
        self.train = [{'label' : 'Timeline Dimension_Timeline','pattern' : [{"lower": {"IN": self.tense}},
                                                    {"lower" : {'REGEX' : '\d*'}},
                                                    {"lower" : {"IN" : self.period}}]},
                {'label' : 'Timeline Dimension_Timeline','pattern' : [{"lower" : {'REGEX' : '\d*'}},
                                                    {"lower" : {"IN" : self.period}}]},
                {'label' : 'Timeline Dimension_Timeline','pattern' : [{"lower" : {"IN" : self.period}},
                    {"lower" : {'REGEX' : '\d*'}}]},
                {'label' : 'Measure_Unit','pattern' : [{"ORTH" : "training"} ,{"ORTH" : "days"}]},
                {'label' : 'Measure_Unit','pattern' : [{"lower" : "training"} ,{"lower" : "days"}]},
                {'label' : 'Goal Measure_Currency', 'pattern' : [{'lower' : 'budget'},{'lower': {'TAG' : 'NN'}}]},
                {'label' : 'Measure_Currency', 'pattern' : [{"lower" : 'actual'},{"TAG" : 'NN'}]},
                {'label' : 'Measure_Currency', 'pattern' : [{"lower" : 'actual'},{"lower" : 'gm'}]},
                {'label' : 'Categorical Dimension_Categorical', 'pattern' : [{"lower" : 'business'},{"lower" : {'REGEX' : 'group(?:s)?'}}]},
                {'label' : 'Timeline Dimension_Timeline', 'pattern' : [{"lower" : {'IN' : self.single}}, {"lower" : {'REGEX' : '\d*'}}]},
                {'label': 'Measure_Unit','pattern': [{'lower': 'no'},{'lower': 'of'},{'lower': 'working'},{'lower': 'days'}]},
                {'label': 'Measure_Unit','pattern': [{'lower': 'number'},{'lower': 'of'},{'lower': 'working'},{'lower': 'days'}]},
                {'label' : 'Categorical Dimension_Categorical', 'pattern' : "stream"},
                {"label": "Measure_Unit", "pattern": "training days"}]
    
    def data_pre_process(self, new_train_data: pd.DataFrame)->dict:
        train_dict = []
        try:
            if not new_train_data.empty:
                self.train_df = pd.concat([self.train_df, new_train_data], sort=False)
            self.train_df = self.train_df.drop_duplicates(subset=['Keywords'], keep='last') 
            self.train_df.reset_index(drop=True, inplace=True)
            # permit_cmd_base_excel = "sudo chgrp -R root Uploads/ModelTrainers/Vaas_task_PoC_v3.xlsx && sudo chmod -R 777 Uploads/ModelTrainers/Vaas_task_PoC_v3.xlsx"
            # if os.path.exists(os.path.join(base_train_loc,"Vaas_task_PoC_v3.xlsx")):
            #     os.system(permit_cmd_base_excel)
            #     os.remove(os.path.join(base_train_loc,"Vaas_task_PoC_v3.xlsx"))
            #     self.train_df.to_excel(os.path.join(base_train_loc,"Vaas_task_PoC_v3.xlsx"),sheet_name="Sheet1", index=False)
            #     os.system(permit_cmd_base_excel)

            ## Updating the training_keywords table for new entry
            keywords_dict = self.train_df.to_dict("records")
            self.training_keywords_db.delete_many({})
            self.training_keywords_db.insert_many(keywords_dict)
            if 'Client' in self.train_df.columns:
                self.train_df.drop(columns=['Client'])
            self.train_df['tags'] = self.train_df['Classification'] +'_'+ self.train_df['Type']
            regex_string = ''
            for index, word in enumerate(self.single):
                if index == 0:
                    regex_string = regex_string + '('
                regex_string = regex_string + word + '\s*\d*'
                if index == len(self.single)-1:
                    regex_string = regex_string + ')'
                else:
                    regex_string = regex_string + '|'
            # print(regex_string)
            for i in self.period:
                train_dict.append({'label' :'Timeline Dimension_Timeline','pattern' : i})
            train_dict.extend(self.train)
            for index, columns in self.train_df.iterrows():
                keyword = columns['Keywords']
                unit = columns["tags"]
                pattern = [{'LEMMA' : keyword.lower()}]
                train_dict.append({'label' : unit, 'pattern' : pattern})
                train_dict.append({'label' : unit, 'pattern' : keyword.lower()})
                words = [word.lower() for word in keyword.split(" ") if word.strip()]
                if len(words) > 1:
                    pattern = [] 
                    for word in words:
                        pattern.append({'LEMMA' : word.lower().strip()})
                else:
                    pattern = keyword.lower()
                    pattern = [{'LEMMA' : pattern}]
                train_dict.append({'label' : unit, 'pattern' : pattern})
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
        return train_dict
    
    def handler(self, func, path, exc_info): 
        print("Inside handler") 
        print(exc_info)
        logger.error("dictionary_model delete Error:::",exc_info)
    
    def load_ML_model(self, train_dict):
        try:
            self.nlp = spacy.blank('en')
            train_nlp = spacy.load("en_core_web_sm", disable = ['ner'])
            ruler = EntityRuler(train_nlp)
            ruler.add_patterns(train_dict)
            train_nlp.add_pipe(ruler,name="custom_ner")
            nlp_path = os.path.join(base_path, 'dictionary_model')
            permit_cmd_dm = "sudo chgrp -R root dictionary_model && sudo chmod -R 777 dictionary_model"
            if os.path.exists(nlp_path):
                os.system(permit_cmd_dm)
                shutil.rmtree(nlp_path, onerror = self.handler)
                train_nlp.to_disk("dictionary_model")
                os.system(permit_cmd_dm)
                super().reloadNLP()
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())

if __name__ == "__main__":    
    pus = ParserUserStory()

    out = pus.text_to_structure("Show me the conversion ratio for different business classes, brokers and underwriters.".lower())
    print(out)