import copy 
import datetime
import itertools
import logging
import math
import pprint
import traceback
import numpy as np
import pandas as pd
import pymongo
import regex as re
from nltk.tokenize import word_tokenize

logger = logging.getLogger()
class DBconn:
    def __init__(self):
        self.myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        self.mydb = self.myclient["local_mongo"]
     
class Charts(DBconn):
    def __init__(self):
        super(self.__class__, self).__init__()
        
        # self.variable_logos = {"Revenue" : "RevenueLogo", "Expense" : "ExpenseLogo", "Profit" : "ProfitLogo"}
        
    @staticmethod
    def find_keyword_match(text, keywords):
        words = word_tokenize(text)
        words = [word.strip() for word in words]
        keywords = [keys.strip() for keys in keywords.split(",")]
        is_a_match = 0
        for key in keywords:
            if ('-') in key:
                key = key.replace('-','')
                is_a_match = is_a_match +  any([word.strip() for word in words if word.endswith(key)])
            else:
                is_a_match = is_a_match + any([word for word in words if word == key])
        return is_a_match
    
        
    # def get_chart_data(self):
    #     '''
    #     It will return chart by count
    #     '''
    #     mycol = self.mydb['charts_by_count']
    #     all_charts = mycol.find_one()
    #     charts = {}
    #     all_charts = all_charts['data']
    #     for k,v in all_charts.items():
    #         new_key = eval(k)
    #         charts[new_key] = v
        
    #     return charts            
    def get_chart_data(self):
        '''
        It will return chart by count
        '''
        mycol = self.mydb['charts_by_count']
        all_charts = mycol.find()
        charts = {}
        for singleChart in all_charts:
            chart_list = []
            chart_data = singleChart['data']
            chart_data['dimension'] = tuple(chart_data['dimension'])
            # print("=========chart_data starts===========")
            # pprint.pprint(chart_data)
            # print("=========chart_data ends===========")
            if chart_data['dimension'] in charts.keys():
                charts[chart_data.pop('dimension')].append(chart_data)
            else:
                chart_list.append(chart_data)
                charts[chart_data.pop('dimension')] = chart_list
        # print("=====charts========")
        # pprint.pprint(charts)
        return charts
        
    def find_chart_type(self,dimension_chart, out_dict = None, text = None):
        try:
            is_four_dim = False
            four_dim = None
            two_dim = None
            charts_suggested = []
            # print("====out_dict====")
            # pprint.pprint(out_dict)
            periodicity = dimension_chart.get('periodicity', None)
            # print("======dimension_chart=======")
            # pprint.pprint(dimension_chart)
            # print("======periodicity=======")
            # pprint.pprint(periodicity)
            # sum = 0
            if "Measure" in dimension_chart:
                dim = tuple(dimension_chart.values())
                is_four_dim = True
                four_dim = dim
                two_dim = dim[0] + dim[1], dim[2]+dim[3]
                sum = dim[0] + dim[1] + dim[2] + dim[3]
            else:
                x_dim = len(dimension_chart['categories'])
                if periodicity and periodicity.get('startDate', None):
                    x_dim += 1
                y_dim = len(dimension_chart['variables'])
                two_dim = y_dim, x_dim


            all_charts = self.get_chart_data()
            if four_dim:
                charts_suggested = all_charts.get(four_dim, [])
            if not charts_suggested and two_dim:
                charts_suggested = all_charts.get(two_dim, [])
            
            charts_suggested_filtered = []
            print("charts before filter---->")
            pprint.pprint(charts_suggested)
            chartAlias = [i['alias'] for i in charts_suggested]
            chartAlias = list(set(chartAlias))
            # print("======chartAlias=====")
            # pprint.pprint(chartAlias)
            for i in charts_suggested:
                if i not in charts_suggested_filtered:
                    match = 0
                    if text:
                        match = self.find_keyword_match(text.lower(), i['keywords']) 
                        print("match", match)
                         
                        if not match:
                            continue
                    charts_suggested_filtered.append((i, match))
            if charts_suggested_filtered:
                charts_suggested_filtered = sorted(charts_suggested_filtered, key= lambda x: x[1], reverse= True)
                charts_suggested_filtered = [charts_suggested_filtered[0][0]]
                    
            print("charts after filter--->")
            pprint.pprint(charts_suggested_filtered)
            charts_suggested_filtered = charts_suggested_filtered or charts_suggested
            if four_dim:
                data_chart = out_dict.copy()
            else:
                data_chart = dimension_chart.copy()

            period = data_chart.get('periodicity', None)
            period_present = True if period and period.get('startDate', None) else False  
            xAxisDataKey  = [cats.get('elementName', None) for cats in data_chart.get('categories', [])]
            yAxisDataKeys = [var.get('variableName', None) for var in data_chart.get('variables', [])]
            yAxisDataKeys = list(set(yAxisDataKeys))
            # print("=====yAxisDataKeys=====")
            # print(yAxisDataKeys)

            if period_present:
                xAxisDataKey.insert(0,"Period")
            if xAxisDataKey:
                if len(xAxisDataKey) > 1:
                    yAxisDataKeys.extend(xAxisDataKey[1:])
                    xAxisDataKey = xAxisDataKey[:1]
        
            final_out = []
            for charts in charts_suggested_filtered:
                temp = {}
                temp['chartType'] = charts['alias']
                temp['actualChartType'] = charts['actual']
                temp['yAxisDataKeys'] = yAxisDataKeys
                temp['xAxisDataKeys'] = xAxisDataKey
                temp['chartTitle'] = dimension_chart
                final_out.append(temp)
                # print("========final_out / chart_generator / suggested chart1=========")
                # pprint.pprint(final_out)
                # print(f"the sum is {sum}")
            ######################
            #tabular chart if sum is greater than 4 starts
            if sum>=4:
                final_out[0]["chartType"] = "tabularChart"
                final_out[0]["actualChartType"] = "Tabular chart"
                # final_out[0]["yAxisDataKeys"] = final_out[0]["xAxisDataKeys"] + final_out[0]["yAxisDataKeys"] 
                # final_out[0]["xAxisDataKeys"] = []

            #tabular chart if sum is greater than 4 ends 
            ######################
            final_out[0]['chartAlias'] = chartAlias
            # print("=========final_out / find_chart_type / chart_generator2==========")
            # pprint.pprint(final_out)
            return final_out
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
            return []
        
        
class ChartViewer:
    
    '''
    It is to provide manipulate the data and return it in the front end accessable form,
    with data beign aggregated
    
    '''
    @staticmethod
    def millify(n):
        millnames = ['',' Thousand',' Million',' Billion',' Trillion']
        
        n = float(n)
        millidx = max(0,min(len(millnames)-1,
                            int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))

        return '{:.0f}{}'.format(n / 10**(3 * millidx), millnames[millidx])
    def get_aggregated_data(self, dataset : pd.DataFrame, xaxis : list,
                            yaxis : list, aggregated_function : str)->pd.DataFrame:
    
        '''
        This function is used to generate aggregated dataset
        Parameters
        ----------
        dataset : pd.DataFrame
            Dataset to be aggregated
        xaxis : list
            List of x axis keys
        yaxis : list
            List of y axis keys
        aggregated_function : str
            Function to be aggregated
            
        Returns
        -------
        aggregated_dataset
        '''
        # print("====inputs to get_aggregated_data / chart_generator.py===")
        # print("====dataset=====")
        # print(dataset)
        # print("====xaxis=====")
        # print(xaxis)
        # print("====yaxis=====")
        # print(yaxis)
        # print("====aggregated_function=====")
        # print(aggregated_function)
        aggregated_function = "mean"
        aggregated_dataset = pd.DataFrame()
        try:
            if not yaxis and len(yaxis)<=0 :

                aggregated_dataset = dataset[xaxis].agg(aggregated_function)
                # print("===========dataset \ chart_generator \ get_aggregated_data===============")
                # print(dataset)
                # print("===========aggregated_dataset \ chart_generator \ get_aggregated_data===============")
                # print(aggregated_dataset)
                if all(dataset[xaxis] <= 100) and aggregated_function == "mean":
                    aggregated_dataset = round(aggregated_dataset/dataset.shape[0], 2)
            elif not xaxis and len(xaxis)<=0:
                aggregated_dataset = dataset[yaxis].agg(aggregated_function)
                if all(dataset[yaxis] <= 100) and aggregated_function == "mean":
                    aggregated_dataset[yaxis] = round(aggregated_dataset[yaxis]/dataset.shape[0], 2)
            else:
                columns = dataset.columns
                new_yaxis = []
                for col in yaxis:
                    if dataset[col].dtype.kind not in 'if':
                        xaxis.append(col)
                    else:
                        new_yaxis.append(col)
                # print("====new_yaxis===")
                # print(new_yaxis)  
                # print("===x axis after appending===")
                # print(xaxis)   
                # print("===aggregated_function====")
                # print(aggregated_function)
                function = dict(zip(new_yaxis, [aggregated_function] * len(aggregated_function)))
                # print("======================aggregated_dataset 1 \ chart generator \ get_aggregated_data==========================")
                # print(aggregated_dataset)
                aggregated_dataset = dataset.groupby(xaxis).aggregate(function).reset_index()
                aggregated_dataset = np.round(aggregated_dataset, decimals = 2)
                # print("======================aggregated_dataset 2 \ chart generator \ get_aggregated_data==========================")
                # print(aggregated_dataset)
            return aggregated_dataset
        except:
            print(traceback.format_exc())    
            logger.error(traceback.format_exc())

        return aggregated_dataset
    @staticmethod
    def get_pie_chart(aggregated_data : pd.DataFrame,xaxis : list,yaxis : list)->pd.DataFrame:
        '''
        will return data as per pie chart requirements
        
        '''
        try:
            allcolumns = []
            x_columns = None
            y_columns = None
            if xaxis and isinstance(xaxis,list):
                x_columns = xaxis[0]
                allcolumns.append(xaxis[0])
            if yaxis and isinstance(yaxis,list):
                y_columns = (yaxis[0])
                allcolumns.append(yaxis[0])
            # print("=====allcolumns /get_pie_chart /chart generator====")
            # print(allcolumns)
            # print("========aggregated_data /get_pie_chart /chart generator========")
            # print(aggregated_data)
            aggregated_data = aggregated_data[allcolumns]
            if x_columns:
                aggregated_data = aggregated_data.rename(columns = {x_columns : "name"})
                aggregated_data['name'] = aggregated_data['name'].astype("str")
            if y_columns:
                aggregated_data = aggregated_data.rename(columns = {y_columns : "value"})
            
            return aggregated_data.to_dict(orient = "records")
        except:
            print(traceback.format_exc())
            return pd.DataFrame()
    
    def get_numbertile_chart(self,aggregated_data : pd.DataFrame)->pd.DataFrame:
        '''
        This is used to return number tile data
        
        '''
        try:
            # if not aggregated_data.empty:
            #     if isinstance(aggregated_data, pd.DataFrame):
            #         numeric_columns = aggregated_data.select_dtypes(include=np.number).columns.tolist()
            #         if numeric_columns:
            #             for col in numeric_columns:
            #                 aggregated_data[col] = aggregated_data[col].apply(self.millify)
            #     else:
            #         aggregated_data = aggregated_data.apply(self.millify)
            try:
                aggregated_data_dict = aggregated_data.to_dict(orient='records')
            except:
                aggregated_data_dict = [aggregated_data.to_dict()]
            # print("==========aggregated_data_dict \ get_numbertile_chart==========")
            # print(aggregated_data_dict)
            return aggregated_data_dict
            
        except:
            logger.error(traceback.format_exc())
            print(traceback.format_exc())
            return pd.DataFrame()

    @staticmethod
    def get_line_chart(aggregated_data : pd.DataFrame)->pd.DataFrame:
        '''
        This is used to return line chart data
        
        '''
        try:
            # print("=======aggregated_data input / get line chart / chart generator======")
            # print(aggregated_data)
            aggregated_data = aggregated_data.to_dict(orient = 'list')
            # print("===aggregated_data / get line chart / chart generator===")
            # print(aggregated_data)
            return aggregated_data
        except:
            logger.error(traceback.format_exc())
            print("===error block / get line chart / chart generator==")
            print(traceback.format_exc())
            return pd.DataFrame()

    @staticmethod
    def get_tabular_chart(aggregated_data: pd.DataFrame, dimension = [])->pd.DataFrame:
        '''
        This is used to return tabular chart data
        
        '''
        try:
            dataset_list = list(aggregated_data)
            aggregated_data = aggregated_data.to_dict(orient = 'list')
            # print("--------------dimension / chart_generator / get_tabular_chart----------------")
            # print(dimension)
            # print("--------------aggregated_data / chart_generator / get_tabular_chart----------------")
            # print(aggregated_data)
            aggregated_data_df = pd.DataFrame(aggregated_data)
            # print("--------------aggregated_data_df / chart_generator / get_tabular_chart----------------")
            # print(aggregated_data_df)
            #to group aggregated dataset starts
            if len(dimension)>=1:
                del dimension["Goal Measure"]
                del dimension["Measure"]
                if len(dimension["Timeline Dimension"])>=1:
                    if dimension["Timeline Dimension"] == "M" or "D" or "Y":
                        dimension["Timeline Dimension"] = ["Period"]
                dimension_values_list = sum(dimension.values(), [])
                if len(dimension_values_list)>=1:
                    aggregated_data_df= aggregated_data_df.groupby(dimension_values_list, as_index=False).mean()
            if len(dimension)==0:
                # if 'Period' in aggregated_data_df:
                #     aggregated_data_df= aggregated_data_df.groupby(['Period']).mean()
                # if 'Vertical' in aggregated_data_df:
                #     aggregated_data_df= aggregated_data_df.groupby(['Vertical']).mean()
                # if 'Account' in aggregated_data_df:
                #     aggregated_data_df= aggregated_data_df.groupby(['Account']).mean()
                # if 'Verical' in aggregated_data_df:
                #     aggregated_data_df= aggregated_data_df.groupby(['Verical']).mean()
                # if 'Region' in aggregated_data_df:
                #     aggregated_data_df= aggregated_data_df.groupby(['Region']).mean()
                aggregated_data_df =aggregated_data_df
            #to group aggregated dataset ends
            aggregated_data = aggregated_data_df.to_dict(orient = 'list')
            # print("--------------aggregated_data / chart_generator / get_tabular_chart / final----------------")
            # print(aggregated_data)
            return aggregated_data
        except:
            logger.error(traceback.format_exc())
            print(traceback.format_exc())
            return pd.DataFrame()

    def get_tabular_chart_sort(self, dimension, aggregated_dataset):
        '''
        This is used to return the sorting order of the tabular chart

        '''
        try:
            # print("======inputs for tabular chart==========")
            # print("=======dimension=========")
            # pprint.pprint(dimension)
            # print("=========aggregated_dataset==========")
            # pprint.pprint(aggregated_dataset)
            # print("==========================================")
            
            #to get new dimension for sorting - due to grouping - starts
            
            #to get new dimension for sorting - due to grouping - ends 
            a = dimension
            b = {'Categorical Dimension' : a['Categorical_Dimension'],'Timeline Dimension' : a['Timeline_Dimension'],'Measure' : a['Measure'],'Goal Measure' : a['Goal_Measure']}
            print("========b======")
            print(b)
            c = b
            d = list(c.values())
            # z = list(map(str,str(d)))
            z = [x for x in d if len(x)>=1] #for getting only the values with not empty list
            # y = list(itertools.chain(*z))
            # y = [y.replace(('Y' or 'M'), 'Period') for y in y]
            y = z
            y = [item for sublist in y for item in sublist]
            for i in enumerate(y):
                    y = [sub.replace('M', 'Period') for sub in y]
                    y = [sub.replace('Y', 'Period') for sub in y]
                    y = [x for x in y if len(x)>=2]
            # string_ = ' '
            # for x in y:
            #     string_ += ' '+x
            # string_ = string_.replace(",", "")
            # string_ = string_.split()
            # print(string_)
            final_agg_list = {}
            for i in y:
                final_agg_list[i] = aggregated_dataset[i]
                aggregated_dataset_order = ((list(final_agg_list.keys())))
            return aggregated_dataset_order
        except:
            logger.error(traceback.format_exc())
            print(traceback.format_exc())



    def get_bubble_chart(self, aggregated_data : pd.DataFrame,xaxis : list,yaxis : list):
        '''
        This is used to return bubble chart data
        
        '''
        try:
            if xaxis:
                x = xaxis[0]
            list_of_lists = []
            final_list = []
            list_of_lists = aggregated_data.values.tolist()
            for i in range(len(list_of_lists)):
                x_index = list_of_lists[i].pop(0)
                list_of_lists[i].append(x_index)
                list_of_lists[i].append(x_index)
            final_list = [[i] for i in list_of_lists]
            print("bubble res>>>>> ",final_list)
            return final_list
        except:
            logger.error(traceback.format_exc())
            print(traceback.format_exc())

    def get_scatter_chart(self, aggregated_data : pd.DataFrame,xaxis : list,yaxis : list):
        '''
        This is used to return scatter chart data
        
        '''
        new_aggr_data = pd.DataFrame()
        try:
            x = xaxis[0]
            for y in yaxis:
                new_aggr_data[y]= aggregated_data[[x,y]].values.tolist()
            new_aggr_data = new_aggr_data.to_dict(orient = "list")
            return new_aggr_data
        except:
            logger.error(traceback.format_exc())
            print(traceback.format_exc())
    
    def get_stackedBar_chart(self, aggregated_data : pd.DataFrame,xaxis : list,yaxis : list):
        '''
        This is used to return stacked bar chart data
        
        '''
        # print("====Inputs for get_stackedBar_chart \ chart generator===")
        # print("===aggregated datatset==")
        # pprint.pprint(aggregated_data)
        # print("==xaxis==")
        # print(xaxis)
        # print("==yaxis==")
        # print(yaxis)
        try:
            x = xaxis[0]
            grp_a = x
            for y in yaxis:
                print("===aggregated_data[y].dtype.kind===")
                print(aggregated_data[y].dtype.kind)
                if aggregated_data[y].dtype.kind in 'if':
                    grp_b = y
                    print("===grp b==")
                    print(grp_b)
                else:
                    grp_a = y
                    print("===grp a==")
                    print(grp_a)
            # print("===grp a , b 2====")
            # print(grp_a, '\n', grp_b)

            grp_dict = aggregated_data.groupby(grp_a)[grp_b].apply(list).to_dict()
            grp_dict = dict(sorted(grp_dict.items(), key=lambda s: s[0]))
            aggregated_data = aggregated_data.to_dict(orient = "list")
            try:
                aggregated_data[x] = sorted(set(aggregated_data[x]), key=lambda x: datetime.datetime.strptime(x, "%b-%Y"))
            except:
                aggregated_data[x] = sorted(set(aggregated_data[x]))
            aggregated_data[grp_a] = sorted(set(aggregated_data[grp_a]))
            aggregated_data[grp_b] = grp_dict
        except:
            aggregated_data = "Selected axis are not allowed for this chart"
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
        # print("====final aggregated dataset / get_stackedBar_chart / chart generator==")
        # print(aggregated_data)
        return aggregated_data

    def get_2columnStackedBar_chart(self, aggregated_data : pd.DataFrame,xaxis : list,yaxis : list):
        '''
        This is used to return stacked bar chart data
        
        '''
        try:
            x = xaxis[0]
            grp_b = []
            for y in yaxis:
                if aggregated_data[y].dtype.kind in 'if':
                    grp_b.append(y)
                else:
                    grp_a = y
            grp_dict1 = aggregated_data.groupby(grp_a)[grp_b[0]].apply(list).to_dict()
            grp_dict1 = dict(sorted(grp_dict1.items(), key=lambda s: s[0]))
            grp_dict2 = aggregated_data.groupby(grp_a)[grp_b[1]].apply(list).to_dict()
            grp_dict2 = dict(sorted(grp_dict2.items(), key=lambda s: s[0]))
            aggregated_data = aggregated_data.to_dict(orient = "list")
            try:
                aggregated_data[x] = sorted(set(aggregated_data[x]), key=lambda x: datetime.datetime.strptime(x, "%b-%Y"))
            except:
                aggregated_data[x] = sorted(set(aggregated_data[x]))
            aggregated_data[grp_a] = sorted(set(aggregated_data[grp_a]))
            aggregated_data[grp_b[0]] = grp_dict1
            aggregated_data[grp_b[1]] = grp_dict2
        except:
            aggregated_data = "Selected axis are not allowed for this chart"
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
        return aggregated_data

    def get_waterFlow_chart(self, aggregated_data : pd.DataFrame,xaxis : list,yaxis : list):
        '''
        This is used to return stacked bar chart data
        
        '''
        try:
            wtr_x = xaxis[0]
            for y in yaxis:
                if aggregated_data[y].dtype.kind in 'if':
                    wtr_y = y
                else:
                    non_wtr_y = y
            # aggregated_data = aggregated_data.sort_values([wtr_x,non_wtr_y], inplace=True)
            aggregated_data = aggregated_data.to_dict(orient = "list")
            water_list = aggregated_data[wtr_y]
            startPt = [0]
            inc_list, dec_list = [], []
            for i, v in enumerate(water_list):
                if i+1 < len(water_list):
                    if water_list[i+1] < v:
                        startPt.append(water_list[i+1])
                    elif water_list[i+1] > v:
                        startPt.append(v)
                if v > startPt[i]:
                    inc = v-startPt[i]
                    dec = "-"
                elif v <=  startPt[i]:
                    inc = "-"
                    dec = water_list[i-1] - v
                inc_list.append(inc)
                dec_list.append(dec)
            aggregated_data["startPoint"] = startPt
            aggregated_data["incData"] = inc_list
            aggregated_data["decData"] = dec_list
        except:
            aggregated_data = "Selected axis are not allowed for this chart"
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
        return aggregated_data

    def get_kpiTile_chart(self, aggregated_data : pd.DataFrame,xaxis : list,yaxis : list):
        '''
        This is used to return KPI tile chart data
        
        '''
        try:
            # print("========inputs to kpi tile chart=======")
            # print("=======aggregated_data======")
            # pprint.pprint(aggregated_data)
            # print("======yaxis========")
            # pprint.pprint(yaxis)
            # print("======xaxis========")
            # pprint.pprint(xaxis)
            actualdict, targetdict, result, background_color = {}, {}, {}, 'grey'
            color_map = {'grey':'#666666', '#feff89': '#666666', '#fb434f': '#d4d4d4', '#2cb978': '#d4d4d4'}
            aggregated_data = aggregated_data.to_dict()
            # print("===================aggregated_data in chart generator / get_kpiTile_chart========================")
            # print(aggregated_data)
            if len(aggregated_data) > 1:
                actual_num = aggregated_data[yaxis[0]]
                avg_num = aggregated_data[yaxis[1]]
                actualdict = {'name': yaxis[0], 'value': actual_num}
                targetdict = {'name': yaxis[1], 'value': avg_num}
                min_num = round(avg_num * 0.8, 2)
                max_num = round(avg_num * 1.5, 2)
            
                # print("============min and max================")
                # print(min_num,'\n' ,max_num)
                tar_ratio = (actual_num/avg_num) * 100
                percent = round(tar_ratio, 2)
                if actual_num >= avg_num:
                    background_color = '#2cb978'
                elif actual_num >= min_num and actual_num < avg_num:
                    background_color = '#feff89'
                elif actual_num < min_num:
                    background_color = '#fb434f'
                color_code = color_map[background_color]
                result['actual'] = actualdict
                result['target'] = targetdict
                result['percentage'] = percent 
                result['background_color'] = background_color
                result['color_code'] = color_code
            
        except:
            result = "Selected axis are not allowed for this chart"
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
        # print("=======final result / get_kpiTile_chart=======")
        # pprint.pprint(result)
        return result

    def get_treeMap_chart(self, aggregated_data : pd.DataFrame,xaxis : list,yaxis : list):
        '''
        This is used to return TREE MAP chart data
        
        '''
        try:
            base_axis = xaxis[0]
            for y in yaxis:
                if aggregated_data[y].dtype.kind in 'if':
                    valueName = y
                else:
                    keyName = y
            #grouping for xaxis
            x_unique = sorted({data for data in aggregated_data[base_axis]})
            base_group = aggregated_data.groupby([base_axis])[valueName].sum().reset_index(name = 'base_sum')
            base_group = base_group.to_dict('records')

            # Response structure
            treeMap, allTreeMap, result = [], [], {}
            for grp in base_group:
                treeMapDict, allTreeMapDict = {}, {}
                treeMapDict['name'] = allTreeMapDict['name'] = grp[base_axis]
                treeMapDict['value'] = allTreeMapDict['value'] = grp['base_sum']
                treeMapDict['type'] = 'treemap'
                child_df = aggregated_data.loc[aggregated_data[base_axis] == grp[base_axis]].copy(deep=True)
                child_df.drop(base_axis, axis=1, inplace=True)
                child_dict = child_df.to_dict('records')
                treeMapDict['data'] = allTreeMapDict['children'] = [dict(zip(('name', 'value'), child.values())) for child in child_dict]
                treeMap.append(treeMapDict)
                allTreeMap.append(allTreeMapDict)
            result['Data'] = treeMap
            result['AllData'] = allTreeMap
            result[base_axis] = x_unique
        except:
            result = "Selected axis are not allowed for this chart"
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
        return result
    
    def get_googleMap_chart(self, aggregated_data : pd.DataFrame,xaxis : list,yaxis : list):
        '''
        This is used to return GOOGLE MAP chart data
        
        '''
        try:
            base_axis = xaxis[0]
            result = {'data':[], 'options':{'region': "world", 
                                            'colorAxis': { 'colors': ["#fb434f", "#feff89", "#2cb978"] },
                                            'backgroundColor': "#81d4fa", 'datalessRegionColor': "#ccc"}}
            valueList = []
            for y in yaxis:
                if aggregated_data[y].dtype.kind in 'if':
                    valueList.append(y)
            if 'US' in aggregated_data.columns:
                base_axis = 'US'
            valueList.insert(0,base_axis)
            aggregated_data = aggregated_data[valueList]
            data = list(aggregated_data.values.tolist())
            data.insert(0,valueList)
            result['data'] = data
        except:
            result = "Selected axis are not allowed for this chart"
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
        return result
    
    def get_chart_view(self, input_json : dict, input_data : pd.DataFrame, period_dict = {}, dimension = [])->dict:
        '''
        main fuction of chart viewer
        
        Parameters
        ----------
        input_json : dict
            configuration settings like xaxis, yaxis
            
        input_json : pd:DataFrame
            input numeric dataframe
               
        '''
        aggregated_data_dict, aggregated_function = {}, None
        # print("=======input_json========")
        # pprint.pprint(input_json)
        # print("=====input_data======")
        # pprint.pprint(input_data)
        try:
            xaxis = input_json.get('xAxisDataKeys',None)
            yaxis = input_json.get('yAxisDataKeys',None)
            # print("====xaxis, xaxis /get chart view / chart generator=======")
            # print(xaxis)
            # print(yaxis)
            aggregated_function = input_json.get('aggregationType','sum')
            userStory = input_json.get('userStory',None)
            aggr_dict = {"maximum":"max", "max":"max", "minimum":"min", "min":"min", "average":"mean", "avg":"mean"}
            if userStory and not aggregated_function:
                for key in aggr_dict.keys():
                    if key in userStory.lower():
                        aggregated_function = aggr_dict[key]
                        break
            chart_type = input_json.get('chartType',None)
            # print("====chart_type / get chart view / chart generator==")
            # print(chart_type)
            aggregated_function = 'sum' if not aggregated_function else aggregated_function
            #convert json to dataframe
            df = pd.DataFrame(input_data['tableData'])
            # print("=======df / get chart view / chart generator=========")
            # pprint.pprint(df)
            if period_dict:
                freq_type = period_dict.get('frequency', None)
                start_date = period_dict.get('startDate', None)
                end_date = period_dict.get('endDate', None)
                if freq_type == 'Y':
                    intColumns = []
                    nonintColumns = []
                    for col in df.columns:
                        if df[col].dtype.kind in 'if':
                            intColumns.append(col)
                        else:
                            nonintColumns.append(col)
                    start_date = datetime.datetime.fromtimestamp(start_date/1000.0).strftime('%Y')
                    end_date = datetime.datetime.fromtimestamp(end_date/1000.0).strftime('%Y')
                    df['Period'] =  pd.to_datetime(df['Period']).dt.strftime('%Y')
                    df = df[df['Period'].between(start_date, end_date, inclusive=True)]
                    df = df.groupby(nonintColumns, as_index=False)[intColumns].sum()
                elif freq_type == 'M':
                    start_date = datetime.datetime.fromtimestamp(start_date/1000.0).strftime('%b-%Y')
                    end_date = datetime.datetime.fromtimestamp(end_date/1000.0).strftime('%b-%Y')
                    df['Period'] = pd.to_datetime(df['Period'])
                    df = df[df['Period'].between(start_date, end_date, inclusive=True)]
                    # df = df.sort_values(by='Period')
                    df['Period'] = df['Period'].dt.strftime('%b-%Y')
                # %Y-%m-%d
                elif freq_type == 'D':
                    start_date = datetime.datetime.fromtimestamp(start_date/1000.0).strftime('%Y-%m-%d')
                    end_date = datetime.datetime.fromtimestamp(end_date/1000.0).strftime('%Y-%m-%d')
                    df['Period'] = pd.to_datetime(df['Period'])
                    df = df[df['Period'].between(start_date, end_date, inclusive=True)]
                    # df = df.sort_values(by='Period')
                    df['Period'] = df['Period'].dt.strftime('%Y-%m-%d')
            #get aggregated data
            # print("==========xaxis in get_chart_view===========")
            # print(xaxis)
            # print(yaxis)
            # print("=========df / input to get aggr.. /get_chart_view / chart generator===========")
            # print(df)
            # print("===========")
            aggregated_data = self.get_aggregated_data(df, xaxis.copy(), yaxis.copy(), aggregated_function)
            # print("=========aggregated_data / get chart view / chart generator==============")
            # print(aggregated_data)
            # Sort dataframe based on Period
            if chart_type not in ['numberTile', 'kpiTile']: 
                if 'Period' in aggregated_data.columns:
                    if re.findall(r'[A-z+]-[0-9+]',aggregated_data['Period'][0]):
                        aggregated_data['Period'] = pd.to_datetime(aggregated_data['Period'], format="%b-%Y")
                        aggregated_data = aggregated_data.sort_values(by=['Period'])
                        aggregated_data['Period'] = aggregated_data['Period'].apply(lambda x: x.strftime('%b-%Y'))
                    else:
                        aggregated_data.sort_values(by=['Period'])
            if chart_type == 'pieChart':
                aggregated_data_dict = self.get_pie_chart(aggregated_data,xaxis,yaxis)
            elif chart_type == 'numberTile':
                aggregated_data_dict = self.get_numbertile_chart(aggregated_data)
                # print("======aggregated_data_dict \\ get_numbertile_chart=======")
                # print(aggregated_data_dict)
            elif chart_type in ['lineChart', 'multiLineChart', 'barChart', 'areaChart', 'lineBarChart']:
                aggregated_data_dict = self.get_line_chart(aggregated_data)
                # print("========aggregated_data_dict / get chart view / chart type - bar, line/ chart generator===========")
                # print(aggregated_data_dict)
            elif chart_type == 'tabularChart':
                aggregated_data_dict = self.get_tabular_chart(aggregated_data, dimension)
            elif chart_type == 'bubbleChart':
                aggregated_data_dict = self.get_bubble_chart(aggregated_data,xaxis,yaxis)
            elif chart_type == 'scatterChart':
                aggregated_data_dict = self.get_scatter_chart(aggregated_data,xaxis,yaxis)
            elif chart_type == 'stackedBarChart':
                aggregated_data_dict = self.get_stackedBar_chart(aggregated_data,xaxis,yaxis)
            elif chart_type == 'waterflowChart':
                aggregated_data_dict = self.get_waterFlow_chart(aggregated_data,xaxis,yaxis)
            elif chart_type == 'kpiTile':
                aggregated_data_dict = self.get_kpiTile_chart(aggregated_data,xaxis,yaxis)
            elif chart_type == 'twoColumnStackedBarChart':
                aggregated_data_dict = self.get_2columnStackedBar_chart(aggregated_data,xaxis,yaxis)
            elif chart_type == 'treeMap':
                aggregated_data_dict = self.get_treeMap_chart(aggregated_data,xaxis,yaxis)
            elif chart_type == 'googleMap':
                aggregated_data_dict = self.get_googleMap_chart(aggregated_data,xaxis,yaxis)
        except:
            print(traceback.format_exc())
            logger.error(traceback.format_exc())
        # print("===============dimension in get chart view / chart generator====================")
        # print(dimension)
        return aggregated_data_dict, aggregated_function, dimension
