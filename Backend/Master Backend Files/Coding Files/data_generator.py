# For Uniform And Non Uniform
from functools import singledispatch 
import pprint
import attr
import numpy as np
import traceback
import pymongo
import random
import re
import pandas as pd
import datetime
from functools import singledispatch
from typing import List
import logging
logger = logging.getLogger()

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["local_mongo"]


@attr.s(auto_attribs=True, kw_only=True)
class FloatType(object):
    low: float
    high: float
    count: int
    precision : int
    variableInnerType : str
    def __attrs_post_init__(self):
        object.__setattr__(self, "precision", int(self.precision))
        object.__setattr__(self, "low", float(self.low))
        object.__setattr__(self, "high", float(self.high))
        object.__setattr__(self, "count", int(self.count))
        object.__setattr__(self, "variableInnerType", str(self.variableInnerType))

@attr.s(auto_attribs=True, kw_only=True)
class PercentType(object):
    low: float
    high: float
    count: int
    precision : int
    variableInnerType : str
    def __attrs_post_init__(self):
        object.__setattr__(self, "precision", int(self.precision))
        object.__setattr__(self, "low", float(self.low))
        object.__setattr__(self, "high", float(self.high))
        object.__setattr__(self, "count", int(self.count))
        object.__setattr__(self, "variableInnerType", str(self.variableInnerType))

@attr.s(auto_attribs=True, kw_only=True)
class RatioType(object):
    low: float
    high: float
    count: int
    precision : int
    variableInnerType : str
    def __attrs_post_init__(self):
        object.__setattr__(self, "precision", int(self.precision))
        object.__setattr__(self, "low", float(self.low))
        object.__setattr__(self, "high", float(self.high))
        object.__setattr__(self, "count", int(self.count))
        object.__setattr__(self, "variableInnerType", str(self.variableInnerType))


@attr.s(auto_attribs=True, kw_only=True)
class IntType:
    low: float
    high: float
    count: int
    variableInnerType : str
    def __attrs_post_init__(self):       
        object.__setattr__(self, "low", float(self.low))
        object.__setattr__(self, "high", float(self.high))
        object.__setattr__(self, "count", int(self.count))
        object.__setattr__(self, "variableInnerType", str(self.variableInnerType))
        
@attr.s(auto_attribs=True, kw_only=True)
class StrType:
    name: str
    count : int
    default : list
    def __attrs_post_init__(self):       
        object.__setattr__(self, "name", str(self.name))
        object.__setattr__(self, "count", int(self.count))
        
@attr.s(auto_attribs=True, kw_only=True)
class EnumType:
    name: str
    count : int
    default : list
    def __attrs_post_init__(self):       
        object.__setattr__(self, "name", str(self.name))
        object.__setattr__(self, "count", int(self.count))
        

@attr.s(auto_attribs=True, kw_only=True)
class DateType:
    name: str
    count : int
    startDate : str
    endDate : str = None
    frequency : str
    out_type : str = None

    def __attrs_post_init__(self):       
        object.__setattr__(self, "name", str(self.name))
        object.__setattr__(self, "count", int(self.count))
        

def to_quarter(arr: np.ndarray)->np.ndarray:
    years = arr.year.to_numpy().astype('str')
    quar = np.zeros(len(arr)).astype('str')
    quar[arr.month == 3] = '-Q1'
    quar[arr.month == 6] = '-Q2'
    quar[arr.month == 9] = '-Q3'
    quar[arr.month == 12] = '-Q4'

    return np.core.defchararray.add(years,quar)

        
def gen_date(start_date : datetime.datetime = None, 
             end_date : datetime.datetime = None, 
             freq : str = None, 
             count : int = None, out_type : int = None,
             dtype = "list",
             find_format = False)->List[datetime.datetime]:
    '''
    This function is used to generate list of dates based on frequency
    m for month and 3m for 3 months
    y for year and 3y for 3 years
    '''
    try:
        to_date = lambda t: t.strftime('%Y-%m-%d').astype('str')
        to_month = lambda t: (t.strftime('%b-%Y')).astype('str')
        to_year = lambda t: t.strftime('%Y').astype('str')
        to_timestamp = lambda t: t.strftime('%d-%m-%Y %H:%M:%S').astype('str')
        to_complete_time = lambda t: t.strftime('%d-%m-%Y %H:%M:%S.%f').astype('str')

        default_format = {'L' : to_complete_time, 'S' : to_timestamp, 'T' : to_timestamp,
            'H' : to_timestamp, 'D' : to_date, 'Q' : to_quarter, 'Y' : to_year, 'M' : to_month}


        out_format = {'month' : to_month, 'date' : to_date,
                        'year' : to_year, 'quarter' : to_month,
                        'timestamp' : to_timestamp}

        if find_format:
            func = default_format[freq]
        else:
            func = out_format[out_type]
        if freq == 'Y':
            start_date = start_date.replace(day = 1, month = 1)
            freq = 'YS'
        date_list = pd.date_range(start=start_date, end=end_date, 
                                    freq = freq, periods = count)
            

        date_lst = func(date_list)
        if dtype == 'array':
            return date_lst
        return date_lst.tolist()
    except:
        print(traceback.format_exc())
        logger.error(traceback.format_exc())




def period_generator(data : dict)->dict:
    """
    
    This function is used to generate periodic data
    
    """
    date_list = []
    try:
        start_date = data.get("startDate", None) or None
        start_date = epoch_to_datetime(start_date)
        end_date = data.get("endDate", None) or None
        end_date = epoch_to_datetime(end_date)
        freq = data.get("frequency",None) or None
        count = data.get("count", None) or None
        out_type = data.get("out_type",'month') or 'month'
        date_list = gen_date(start_date = start_date,
                end_date = end_date, count= None, freq =  freq,
                out_type = 'timestamp',find_format = True)

    except:
        print(traceback.format_exc())
        logger.error(traceback.format_exc())

    return date_list


def insert_time(temp, date_lst):
    for i in temp.copy():
        if isinstance(temp[i], dict):
            if not temp[i]:
                temp_ = dict.fromkeys(date_lst, {})
                temp[i] = temp_
            else:
                insert_time(temp[i],date_lst)
        else:
            return temp


def gen_random_gauss(size : int)->np.array:
    '''
    This function is used to generate random numbers with gaussian distribution
    mean is set to zero and standard deviation to 3
    
    '''
    size = int(size)
    # print("====size===")
    # print(size)
    random_gauss = np.array([random.gauss(0,3) for i in range(size)])
    # print("=====random_gauss====")
    # print(random_gauss)
    return random_gauss

def scale(x : np.array, data_range : tuple, axis=None)->np.array:
    '''
    This function is used to rescale the given array between specified range
    x : np.array
        the array to be rescaled
    data_range : tuple
        tuple with min and max values
    
    axis : int, NoneType
        default set to None 
    '''
    domain = np.min(x, axis), np.max(x, axis)
    if domain[0] == domain[1]:
        scaled = np.array([np.random.randint(data_range[0],data_range[1])])
        
    else:
        y = (x - (domain[1] + domain[0]) / 2) / (domain[1] - domain[0])
        scaled = y * (data_range[1] - data_range[0]) + (data_range[1] + data_range[0]) / 2
        np.random.shuffle(scaled)
    return scaled
@singledispatch
def gen_data(data_type):
    raise NotImplementedError('Unsupported type')

@gen_data.register(FloatType)
def _(data_type)->'array of floats':
    # print("=======data_type========")
    # print(data_type)
    # print("========array of floats===========")
    ran_num_unscaled = gen_random_gauss(data_type.count)
    ran_num = scale(ran_num_unscaled, (data_type.low, data_type.high))
    # print("========ran_num 1=========")
    # print(ran_num)
    ran_num = np.round(ran_num, decimals=data_type.precision)
    # print("========ran_num 2=========")
    # print(ran_num)
    return ran_num

@gen_data.register(PercentType)
def _(data_type)->'array of percent':
    # print("========array of percent===========")
    ran_num_unscaled = gen_random_gauss(data_type.count)
    ran_num = scale(ran_num_unscaled, (data_type.low, data_type.high))
    # print("========ran_num 1=========")
    # print(ran_num)
    ran_num = np.round(ran_num, decimals=data_type.precision)
    # print("========ran_num 2=========")
    # print(ran_num)
    #if need of percentage i format of low = 0.0 high = 0.9 starts
    # percent_formula = lambda x: ((x*100/1)*(100/100))
    # ran_num = percent_formula(ran_num)
    #if need of percentage i format of low = 0.0 high = 0.9 ends
    # print("========ran_num 3=========")
    # print(ran_num)
    return ran_num
####
def percentTypeFunc(new_data):
    # print("========new_data / percenttype Func===========")
    # print(new_data)
    ran_num_unscaled = gen_random_gauss(new_data['count'])
    ran_num = scale(ran_num_unscaled, (new_data['low'], new_data['high']))
    # print("========ran_num 1=========")
    # print(ran_num)
    if len(new_data['precision']) == 0:
        new_data['precision'] = 2
    ran_num = np.round(ran_num, decimals=new_data['precision'])
    # print("========ran_num 2=========")
    # print(ran_num)
    #if need of percentage i format of low = 0.0 high = 0.9 starts
    # percent_formula = lambda x: ((x*100/1)*(100/100))
    # ran_num = percent_formula(ran_num)
    #if need of percentage i format of low = 0.0 high = 0.9 ends
    ran_num = ran_num.astype(int)
    # print("========percentTypeFunc / final out=========")
    # print(ran_num)
    return ran_num
####
@gen_data.register(RatioType)
def _(data_type)->'array of ratio':
    # print("========array of ratio===========")
    ran_num_unscaled = gen_random_gauss(data_type.count)
    ran_num = scale(ran_num_unscaled, (data_type.low, data_type.high))
    # print("========ran_num 1=========")
    # print(ran_num)
    ran_num = np.round(ran_num, decimals=data_type.precision)
    ratio_formula = lambda x: x/x.sum(0, keepdims=True)
    ran_num = ratio_formula(ran_num)
    # print("========ran_num 2=========")
    # print(ran_num)
    return ran_num

####
def ratioTypeFunc(new_data):
    # print("========array of ratio===========")
    ran_num_unscaled = gen_random_gauss(new_data['count'])
    ran_num = scale(ran_num_unscaled, (new_data['low'], new_data['high']))
    # print("========ran_num 1=========")
    # print(ran_num)
    if len(new_data['precision']) == 0:
        new_data['precision'] = 2
    ran_num = np.round(ran_num, decimals=new_data['precision'])
    ratio_formula = lambda x: x/x.sum(0, keepdims=True)
    ran_num = ratio_formula(ran_num)
    # print("========ran_num 2=========")
    # print(ran_num)
    ran_num = ran_num.astype(int)
    # print("=====ratioTypeFunc / final out==========")
    # print(ran_num)
    return ran_num
####

@gen_data.register(IntType)
def _(data_type)->'array of ints':
    # print("========array of ints===========")
    # print("========data_type======")
    # print(data_type)
    try:
        ran_num = np.array([])
        ran_num_unscaled = gen_random_gauss(data_type.count)
        # ran_num = scale(ran_num_unscaled, (data_type.low, data_type.high))
        # print("=====ran_num_unscaled=========")
        # print(ran_num_unscaled)
        # print("=====ran_num=========")
        # print(ran_num)
        if data_type.variableInnerType == 'ratio' or data_type.variableInnerType == 'percent':
            # ran_num = scale(ran_num_unscaled, (data_type.low*100, data_type.high*100))
            ran_num = scale(ran_num_unscaled, (data_type.low, data_type.high))
            ran_num = ran_num.astype(float)
        else:
            ran_num = scale(ran_num_unscaled, (data_type.low, data_type.high))
            ran_num = ran_num.astype(int)
        # print("=====ran_num / final int=========")
        # print(ran_num)
    except:
        print(traceback.format_exc())
    return ran_num

@gen_data.register(StrType)
def _(data_type)->'array of str':
    print("========array of str===========")
    str_lst = []
    size = data_type.count
    name = data_type.name
    default_lst = data_type.default
    if default_lst:
        if len(default_lst) < size :
            count = size - len(default_lst)
            str_lst = default_lst.copy()
        else:
            count = 0
            str_lst =  default_lst[:size] 
    else:
        count = size
    
    for i in range(count):
        name_temp = name.split(" ")[0]
        str_lst.append(name + str(i+1))
        
    return np.array(str_lst)


@gen_data.register(EnumType)
def _(data_type)->'array of Enum': 
    print("========array of Enum===========")
    enum_lst = []
    default_lst = data_type.default
    enum_lst.extend(default_lst)
    count = data_type.count 
    new_count = count - len(enum_lst)
    if count < len(enum_lst):
        return enum_lst[:count]
    elif count == len(enum_lst):
        return np.array(enum_lst)
    else:
        while len(enum_lst) < count:
            new_choice = random.choice(default_lst)
            enum_lst.append(new_choice)
        return np.array(enum_lst)
    
    
def epoch_to_datetime(epoch_time):
    date_ = datetime.datetime.fromtimestamp(float(epoch_time)/1000.)
    return date_

@gen_data.register(DateType)
def _(data_type)->'array of str of dates':
    print("========array of dates===========")
    data_type.startDate = epoch_to_datetime(data_type.startDate)
    date_list = gen_date(start_date = data_type.startDate,
                 count= data_type.count, freq =  data_type.frequency,
                 out_type = 'timestamp', dtype = 'array',
                 find_format = True)
    
    
    
#     if data_type.end_date and data_type.count:
#         date_list = gen_date(start_date = data_type.start_date,
#                  count= data_type.count, freq =  data_type.frequency,
#                  out_type = data_type.out_type, dtype = 'array')
#     else:
#         date_list = gen_date(start_date = data_type.start_date,
#                              end_date = data_type.end_date,
#                  count= data_type.count, freq =  data_type.frequency,
#                  out_type = data_type.out_type, dtype = 'array')
    return date_list

class DataGeneration:
    
    def __init__(self):
        self.default_dict = {}
        self.__name_and_values = {}
        self.dependent_param = set()
        self.independent_param = set()
        self.parameters = {}
    
    
    def gen_variation(self, old_values : np.array, variation : int,
                      size : int, dtype : str, precision : int = 2, operator_new : str = "+")->np.array:
        random_variation = np.random.randint(-variation,variation,size)
        new_values = (old_values - (old_values * (random_variation/100)))
        operator = operator_new
        print("==========operator=========")
        print(operator)
        #############
        #for variable updation with +,-,=&- starts
        if operator == "+":
            print("========gen_variation = +  ============")
            random_variation = np.random.randint(0,variation,size)
            new_values = (old_values + (old_values * (random_variation/100)))
        if operator == "-":
            random_variation = np.random.randint(0,variation,size)
            new_values = (old_values - (old_values * (random_variation/100)))
            print("========gen_variation = -  ============")        
        if operator =="+/-":
            print("========gen_variation = +/-  ============")
            random_value = random.choice([-1, 1])
            if random_value == 1:
                new_values = (old_values + (old_values * (random_variation/100)))
            if random_value ==  -1: 
                new_values = (old_values - (old_values * (random_variation/100)))
        # else: 
        #     print("========gen_variation = else  ============")
        #     new_values = (old_values - (old_values * (random_variation/100)))
        #for variable updation with +,-,=&- ends
        #############
        if dtype == 'float':
            try :
                precision = int(precision)
            except:
                precision = 2
            new_values = np.around(new_values,precision)            
        else:
            new_values = new_values.astype(int)
        # print("=====random_variation=======")
        # print(random_variation)
        # print("======new_values=====")
        # print(new_values)

        return new_values

    @staticmethod  
    def gen_class(data, dtype, variableInnerType):
        # print("====dtype before====")
        # print(dtype)
        # if variableInnerType == 'percent' or variableInnerType == 'ratio':
            # class_and_func = {'int' : IntType, 'float' : FloatType, 'str' : StrType,
                        #  'enum' : EnumType, 'time' : DateType, 'percent' : PercentType, 'ratio' : RatioType}
            # dtype = class_and_func[variableInnerType]
        # print("====dtype after====")
        # print(dtype)
        fields = [i.name for i in attr.fields(dtype)]
        # print("====fields====")
        # print(fields)
        new_data = {}

        for i in data.keys():
            if i in fields:
                new_data[i] = data[i]
        new_data['variableInnerType'] = variableInnerType  
        # print("=====new_data====")
        # print(new_data)
        # if variableInnerType != 'percent' and variableInnerType != 'ratio': 
        data_cls = dtype(**new_data)
        # if variableInnerType == 'percent':
        #     data_cls = percentTypeFunc(new_data)
        # if variableInnerType == 'ratio':
        #     data_cls = ratioTypeFunc(new_data)
        # print("======data_cls / gen_class / data_generator========")
        # print(data_cls)
        return data_cls
    
    
    def parse_formula(self, text:str)->list:
        params = re.findall('\[(.*?)\]',text)
        params = [i.strip() for i in params if i.strip()]
        return params
    
    def replace_percentage(self, formula):
        try:
            percent_re = re.compile(r'(\d+\.?\d*?)\s*?\%')
            get_digit = percent_re.findall(formula)
            get_digit = [i.strip() for i in get_digit if i.strip()]
            if get_digit:
                digit = get_digit[0]
                try:
                    digit = float(digit)
                except:
                    print(traceback.format_exc())
                    digit = 10
            clean_formula = percent_re.sub(str(digit/100) + '*',formula)
            return clean_formula
            
            
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            return formula
            
        
    
    def clean_formula(self, var: list, formula : str):
        temp = formula
        symbols = '+-%*/'

        for i in symbols:
            if i in temp:
                temp = temp.replace(i,f" {i} ")

        temp = temp.split(" ")
        final_temp = []
        for i in temp:
            if i in var or i in symbols or i.isdigit():
                final_temp.append(i)

        cleaned_formula = ' '.join(final_temp)
        is_variation = lambda x : bool(re.findall('(?:\+|\-)\s*?(?:\+|\-)',x))
        
        
        if not is_variation(cleaned_formula) and '%' in cleaned_formula:
            cleaned_formula = self.replace_percentage(cleaned_formula)
            
        
        return cleaned_formula, is_variation(cleaned_formula)
    
    
    def find_variation(self, formula, variable_list):
        precision = self.get_precision(variable_list)
        old_dict_values = self.__name_and_values[variable_list[0]]
        old_dict = self.parameters[variable_list[0]]
        var_re = re.findall(r'(\d+\.?\d*?)\s*?\%',formula)
        var_re = [i.strip() for i in var_re if i.strip()]
        if var_re:
            variation = var_re[0]
            try:
                variation = int(variation)
            except:
                variation = float(variation)
        else:
            variation = 10
        
        dtype = old_dict['dataType']
        size = old_dict['count']   
        operator_new =  variable_list["operator"]       
        new_val = self.gen_variation(old_dict_values, variation,
                                      size,dtype,precision, operator_new)
        return new_val

    def perform_action(self,formula):
        var_list = self.parse_formula(formula)
        formula = re.sub(r'(\[|\])',' ',formula)
        formula = formula.strip()
        formula = formula.replace("  "," ")
        temp = self.__name_and_values
        variable_list = []
        for k,v in temp.items():
            locals()[k] = v
        formula , is_variation = self.clean_formula(var_list, formula)

        if is_variation:
            final_val = self.find_variation(formula, var_list)
        else:
            final_val = eval(formula)

        return final_val
        
    def get_precision(self, param_list):
        for param in param_list:
            temp_dict = self.__name_and_values[param]
            if 'precision' in temp_dict:
                temp_val = temp_dict['precision']
                temp_val = str(temp_val).strip()
                try:
                    temp_val = int(temp_val)
                    return temp_val
                except:
                    pass
        return None
    
    def ind_param_values(self, param_name, values_dict:dict):
        try:
            dep_type = values_dict.get('dependentType',None)
            if dep_type == 'formula':
                formula = values_dict.get('formula','')
                param_req = self.parse_formula(formula)
                if param_req:
                    for param in param_req:
                        if param not in self.__name_and_values:
                            temp_param_dict = self.parameters[param]
                            self.ind_param_values(param,temp_param_dict)
                    dtype = self.parameters[param_req[0]]['dataType']
                    self.parameters[param_name]["dataType"] = dtype
                    final_val = self.perform_action(formula)
                    final_val = final_val.astype(dtype)
                    if isinstance(final_val,np.ndarray) and final_val.dtype == 'float':
                        precision = self.get_precision(param_req)
                        if not precision:
                            precision = 2
                        final_val = np.around(final_val, precision)
                    self.__name_and_values[param_name] = final_val
                else:
                    return
            elif dep_type == 'variation':
                linked_ele = values_dict.get('linkedElementName',None)
                variation =  values_dict.get('variation',0)
                size = values_dict.get('count',0)
                linked_ele_data = self.default_dict[linked_ele]
                dtype = linked_ele_data.get('dataType',"int")
                precision = linked_ele_data.get('precision',2)
                operator_new = values_dict.get('operator',"+")
                if size:
                    old_val = self.__name_and_values[linked_ele]
                    # old_val["opeartor"] = operator
                    final_val = self.gen_variation(old_val, variation, size, dtype, precision, operator_new)
                    self.__name_and_values[param_name] = final_val
                # print("======linked_ele_data=========") 
                # print(linked_ele_data)

                
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            return ''    
    
    def get_variables(self,data:dict, default_size = None):
        '''
        This function is used to get variables and generate data for them
        '''
        param_data = {}
        all_variables = []
        # print("=======data==========")
        # print(data)
        ###################
        #for variable updation starts
        # for i in data:
        #     if i["operator"] == "+":
        #          i['low'] = i['']
        #for variable updation ends
        ###################
        for i in data:
            all_variables.append(i)
            # print("=======all_variables==========")
            # print(all_variables)
            isLinked = False
            #take parameters from json and store it with key and values
            self.parameters[i] = data[i]
            #find dependent param and independent param
            for j in data[i]:
                if j == 'isLinked' and data[i][j]:
                    self.dependent_param.add(i)
                    isLinked = True

            if not isLinked:
                self.independent_param.add(i)
                
        class_and_func = {'int' : IntType, 'float' : FloatType, 'str' : StrType,
                         'enum' : EnumType, 'time' : DateType, 'percent' : PercentType, 'ratio' : RatioType}
        # print("=========dependent_param===========")
        # print(self.dependent_param)
        # print("=========independent_param===========")
        # print(self.independent_param)
        for param in self.independent_param:
            values = self.parameters[param]
            # print("====values=====")
            # pprint.pprint(values)
            dtype = class_and_func[values['dataType']]
            values['name'] = param
            values['variableInnerType'] = values['variableInnerType'] if 'variableInnerType' in values else 'int'
            variableInnerType = values['variableInnerType']

            data_cls = self.gen_class(values,dtype, variableInnerType)
            # data_cls['variableInnerType'] = variableInnerType
            # print("=====data_cls before======")
            # print(data_cls)
            # if variableInnerType != 'percent' and variableInnerType != 'ratio':
            generated_data = gen_data(data_cls)
            # if variableInnerType == 'percent':
            #     generated_data = percentTypeFunc(data_cls) 
            # if variableInnerType == 'ratio':
            #     generated_data = ratioTypeFunc(data_cls)
            # print("=========data_cls after==========")
            # print(data_cls)
            # print("==========generated_data=========")
            # print(generated_data)
            self.__name_and_values[param] = generated_data

        for param in self.dependent_param:

            values = self.parameters[param]
            self.ind_param_values(param, values)
            # print("===========dependent_param============")
            # print("==========values============")
            # print(values)
            # print("========self.ind_param_values==========")
            # print(self.ind_param_values(param, values))

        # print("=================self.__name_and_values, all_variables===================") 
        # print(self.__name_and_values, all_variables)
               
        return self.__name_and_values, all_variables
    
    @staticmethod
    def numpy_to_list(dict_ : dict):
        new_dict = {}
        for k,v in dict_.items():
            if isinstance(v,np.ndarray):
                new_dict[k] = v.tolist()
            else:
                new_dict[k] = v
        return new_dict
    
    def produce_values(self, data : dict)->dict:
        '''
        this function is used to generated random numbers for the given json
        * It will extract variables from the json
        * It will find variables linked to each other and unlinked variables
        * It will use same low high values for linked params and given values for
          unlinked.
        '''
        try:
            linked_pairs = []
            workspace_id = data.get('workspace_id',None)
            widget_id = data.get('_id',None)
            self.default_dict = data
            data_val, all_variables = self.get_variables(data)
            data_val = self.numpy_to_list(data_val)
            return data_val, all_variables
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            
            
def name_generator(name, count, values):
    # print("=====values b4========")
    # print(values)
    if type(values)== str:
        values = values.split(', ')
    # print("==name_generator====")
    # print(name)
    # print(count)
    # print(values)
    for num in range(1,count+1):
        values.append(str(name) + '_' + str(num))
    return values[:count]  



'''
"abc": {
        "noOfSyntheticElements": "",
        "syntheticElements": [],
        "isLinked": True,
        "prefix": "",
        "linkedElementName": "SX",
        "nonUniformElements": [
          {
            "syntheticElement": "2_6",
            "elements": "asx,asxvsax,saxhbvuiasx"
          },
          {
            "syntheticElement": "2_12",
            "elements": "asxas,2sjha"
          }
        ],
'''

def get_cat_names(data_key, data_base, name = None):
    # print("======inputs /get_cat_names / data generator======")
    # print("==data_key==")
    # print(data_key)
    # print("==data_base==")
    # pprint.pprint(data_base)
    # print("==name==")
    # print(name)
    all_val_and_keys = {}
    values = {}
    for i in data_key:
        value_dict = data_base[i]
        count = value_dict.get('noOfSyntheticElements',0)
        #16/6/22 E1- starts
        if count == 0:
            if 'syntheticElements'  in data_base[i]:
                if len(data_base[i]['syntheticElements'])>=1:
                    count = len(data_base[i]['syntheticElements'])
        #16/6/22 E1- ends
        try:
            count = int(count)
        except:
            count = 0
        default_values = value_dict.get('syntheticElements',[])
        nonUniformElements = value_dict.get('nonUniformElements',[])
        is_uniform = value_dict.get("isUniform",False)
        is_linked = value_dict.get("isLinked",False)
        
        if is_linked and not is_uniform and nonUniformElements:
            new_dict = {}
            linked_ele_name = value_dict['linkedElementName']
            linked_ele_data = data_base[linked_ele_name]
            def_syn_ele = linked_ele_data['syntheticElements']
            available_syn_ele = [ele['syntheticElement'] for ele in nonUniformElements]
            missing_ele = set(def_syn_ele) - set(available_syn_ele)
            if missing_ele:
                for mis_ele in missing_ele:
                    nonUniformElements.append({"syntheticElement" : mis_ele , 'elements' : "----"})
            # print("====nonUniformElements / after=====")
            # print(nonUniformElements)
            #non uniform length starts m1
            if 'elements' in nonUniformElements[0]:
                if len(nonUniformElements[0]['elements'])>=1:
                    nonUniformElements[0]['nonUniformLenght'] = len(nonUniformElements[0]['elements'])
                    pprint.pprint(nonUniformElements)
            #non uniform length ends m1
            for ele in nonUniformElements:
                key = ele['syntheticElement']
                val_dict = {}
                # print("===ele / get_cat_names / data generator====")
                # print(ele)
                # print(ele['syntheticElement'])
                # def_list = ele['elements'].split(',')
                def_list = ele['elements']
                # print("===def_list 1 / get_cat_names / data generator====")
                # print(def_list)
                val_dict['syntheticElements'] =def_list
                val_dict['noOfSyntheticElements'] = len(def_list)
                val_dict['nonUniformLenght'] = len(ele['elements'])
                new_dict[key] = val_dict
            # print("====new_dict======")
            # pprint.pprint(new_dict)
            key_val, actual_val = get_cat_names(new_dict.keys(), new_dict, name =i)
            values.update(actual_val)
            
            key_val = dict(zip(key_val.keys(),[i]*len(key_val)))
            all_val_and_keys.update(key_val)
        else:
            def_name = i
            if name:
                def_name = name
            # print("=====default_values=====")
            # print(default_values)
            # print("==def_name, count==")
            # print(def_name, count)
            if default_values != "----":
                cat_names = name_generator(def_name, count, default_values)
            if default_values == '----':
                default_values = ['====']
                # cat_names = name_generator(def_name, count, default_values)           
                cat_names = "----"
            # print("=====cat_names======")
            # print(cat_names)
            temp_dict = {cat : i for cat in cat_names}
            all_val_and_keys.update(temp_dict)
            values[i] = cat_names
        # print("=====all_val_and_keys/ / get_cat_names / data generator=====")
        # pprint.pprint(all_val_and_keys)
        # print("====values / get_cat_names / data generator====")
        # pprint.pprint(values)
        
    return all_val_and_keys, values


def dict_assigner(variable, values):
    final_dict = {}
    for var in variable:
        if var in values:
            temp = {var : dict_assigner(values[var], values)}
            final_dict.update(temp)
        else:
            final_dict.update({var : {}})
    return final_dict


def structure_changer(input_dict, variable, key = None, values = None):
    if not input_dict:
        val = dict_assigner(variable, values)
        return val
    else:
        for k, v in input_dict.copy().items():
            if not v:
                val = dict_assigner(variable, values)
                input_dict[k] = val
            else:
                structure_changer(v, variable, values = values)
    return input_dict

  
def flattenit(pyobj, keystring =''): 
    if type(pyobj) is dict: 
        if pyobj:
            keystring = keystring + "++" if keystring else keystring 
            for k in pyobj:
                yield from flattenit(pyobj[k], keystring + k) 
        else:
            yield keystring, pyobj

    elif (type(pyobj) is list): 
        for lelm in pyobj: 
            yield from flattenit(lelm, keystring) 
    else:
        yield keystring, pyobj

def nested_json_to_list_json(nested_dict : dict , variable_dict : dict):
    '''
    convert the dict of dict to key and list pairs
    for eg:
    "Geo_1": {
            "vertical_1": {},
            "vertical_2": {}
        },
        "Geo_2": {
            "vertical_1": {},
            "vertical_2": {}
        }
    the above will be converted to
    
    {'Geo' : ['Geo1','Geo1','Geo2','Geo2'],
     'vertical' : ['vertical_1','vertical_2','vertical_1','vertical_2']
    }
    
    '''
    if not nested_dict:
        return {}
    res = {k:v for k, v in flattenit(nested_dict)} 
    dict_list = {}
    for i in res.keys():
        temp_lst = i.split('++')
        for ele in temp_lst:
            key = variable_dict[ele]
            dict_list.setdefault(key, []).append(ele) 
    return dict_list

def get_category_structure(category, date_lst):
    dep_lst = []
    indep_lst = []
    values = {}
    all_categories = []
    for k,v in category.items():
        all_categories.append(k)
        if v.get('isLinked',None):
            dep_lst.append(k)
        else:
            indep_lst.append(k)

    all_val_and_keys = {}
    # print("====indep_lst=====")
    # pprint.pprint(indep_lst)
    # print("===category===")
    # pprint.pprint(category)
    temp_all_keys, temp_val = get_cat_names(indep_lst,category)
    # print("=====temp_all_keys=======")
    # print(temp_all_keys)
    # print("====temp_val=====")
    # print(temp_val)
    all_val_and_keys.update(temp_all_keys)
    # print("======all_val_and_keys====")
    # print(all_val_and_keys)
    values.update(temp_val)
    # print("----values1----")
    # pprint.pprint(values)
    # print("=====dep_lst======")
    # print(dep_lst)
    # print("=====category=====")
    # print(category)
    temp_all_keys, temp_val = get_cat_names(dep_lst,category)
    # print("---temp_all_keys-----")
    # print(temp_all_keys)
    # print("----temp_val-----")
    # print(temp_val)
    all_val_and_keys.update(temp_all_keys)
    values.update(temp_val)
    # print("----values2----")
    # pprint.pprint(values)
    structure = {}
    if date_lst:
        all_categories.append('Period')
        temp = dict(zip(date_lst, ['Period']*len(date_lst)))
        all_val_and_keys.update(temp)
        values['Period'] = date_lst
        
    # print("=======all_categories======")
    # pprint.pprint(all_categories)
    # print("----values3----")
    # pprint.pprint(values)
    for cat in all_categories:
        variables = values.get(cat,None)
        if variables:
            # print("=======structure, variables, cat, values / get_category_structure / data generator========")
            # print(structure ,'\n', variables,'\n' ,cat, '\n',values)
            structure = structure_changer(structure, variables, cat, values)
    
    out = nested_json_to_list_json(structure, all_val_and_keys)
    # print("=====out=====")
    # pprint.pprint(out)
    # print("=====all_categories=====")
    # pprint.pprint(all_categories)


    return out, all_categories
    
    
def get_random_data(data):
    # print("=======data/ input / get random data / data generator====")
    # pprint.pprint(data)
    default_cat_struct = {
      "noOfSyntheticElements": 0,
      "syntheticElements": [],
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
    
    category = data['categories']
    # print("======category 1======")
    # pprint.pprint(category)
    new_categories = {}
    
    for i in category:
        new_categories[i['elementName']] = i
    category = new_categories 
    # print("======category 2======")
    # pprint.pprint(category)
    variable_dict = data['variables']
    # print("======variable_dict=======")
    # pprint.pprint(variable_dict)
    new_variables = {}
    for i in variable_dict:
        new_variables[i['variableName']] = i
    variable_dict = new_variables
    # print("========variable_dict=========")
    # pprint.pprint(variable_dict)
            
    
    period = data.get('periodicity', None)
    #create a list of dates 
    if period:
        date_lst = period_generator(period)
    else:
        date_lst = []
    if not category and date_lst:
        default_cat_struct['noOfSyntheticElements'] = len(date_lst)
        default_cat_struct['syntheticElements'] = date_lst.copy()
        category = {"Period" : default_cat_struct}
        date_lst = []
    # print("======default_cat_struct======")
    # print(default_cat_struct)
    # print("======category 3======")
    # pprint.pprint(category)
    #nonUniformElements starts m2
    for i in category:
        if 'nonUniformElements' in category[i]:
            # print("=====category[i]['nonUniformElements']======")
            # print(category[i]['nonUniformElements'])
            # print(type(category[i]['nonUniformElements']))
            if category[i]['nonUniformElements'] != None:
                for j in category[i]['nonUniformElements']:
                    print(j['elements'])
                    if len(j['elements'])>=1:
                        j['nonUniformLength'] = len(j['elements'])
    # print("==category / nonUniformElements ===")
    # pprint.pprint(category)
    #nonUniformElements ends m2
    category_structure , all_categories = get_category_structure(category, date_lst)
    # print("---all_categories---")
    # print(all_categories)
    # print("=====category_structure=====")
    # pprint.pprint(category_structure)
    ##find size by counting number of empty dicts
    if category_structure:
        first_cat = list(category_structure.keys())[0]
        size = len(category_structure[first_cat])  
    else:
        size = 10
    
    working_dict = variable_dict.copy()

    #now append count in all the varible
    for k,_ in variable_dict.items():
        working_dict[k]['count'] = size

    data_gen = DataGeneration()
    
    ##generate data for the variables
    out, all_variables = data_gen.produce_values(working_dict)
    # print
    out_df = pd.DataFrame.from_dict(out)
    # print("=====out_df====")
    # pprint.pprint(out_df)
    ##############
    #error after adding 1st non uniform starts
    # print("========category_structure1========")
    # pprint.pprint(category_structure)
    category_structure_df = pd.DataFrame.from_dict(category_structure, orient='index')
    category_structure_df = category_structure_df.transpose()
    category_structure_df= category_structure_df.dropna()
    category_structure = category_structure_df.to_dict(orient="list")
    #error after adding 1st non uniform ends
    ##############
    # print("=====category_structure2====")
    # pprint.pprint(category_structure)
    df = pd.DataFrame.from_dict(category_structure)
    # print("=====df====")
    # pprint.pprint(df)
    table_columns = all_categories + all_variables
    final_df = pd.concat([df,out_df], axis = 1)
    # print("===table_columns==")
    # print(table_columns)
    # print("====final_df====")
    # print(final_df)
    final_df = final_df[table_columns]
    table_data = final_df.to_dict(orient='records')
    payload = {}
    payload['tableData'] = table_data
    payload['tableColumns'] = table_columns
    # print("===========table_data in data generaator===============")
    # print(table_data)
    return payload

