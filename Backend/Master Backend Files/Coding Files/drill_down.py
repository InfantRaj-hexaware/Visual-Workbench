import copy
import datetime
import itertools
import math
import pprint
import traceback
import numpy as np
import pandas as pd
import regex as re
from nltk.tokenize import word_tokenize

class DrillDown:
    def __init__(self, drilldown_out):
        self.drilldown_out = drilldown_out

    @staticmethod
    def tabular_drillDown(aggregated_data, hierarchial_order, chartDimension):
        
        dataset_list = list(aggregated_data)
        aggregated_data = pd.DataFrame(aggregated_data)
        print("=====tabular_drillDown/ input / aggregated_data  / drill down.py=====")
        print(aggregated_data)
        print("=====hierarchial_order/ input / aggregated_data / drill down.py =====")
        print(hierarchial_order)
        aggregatedDataset_DW = aggregated_data.to_dict(orient = 'list')
        tabular_level_dict_inputs = {
            'Measure': chartDimension['Measure'],
            'Goal Measure': chartDimension['Goal Measure'],
            'Timeline Dimension' : chartDimension['Timeline Dimension']}
        tabular_grouping_hOrder_out = []
        def tabular_grouping_hOrder(chartDimension, aggregated_data, hierarchial_order):
            print("===chartDimension===")
            print(chartDimension)
            print("====hierarchial_order1===")
            print(hierarchial_order)
            #if Period needed - hardcode 
            # if "Period" in chartDimension['Timeline Dimension']:
            #     hierarchial_order.append("Period")
            print("====hierarchial_order2===")
            print(hierarchial_order)
            res = []
            [res.append(x) for x in hierarchial_order if x not in res]
            hierarchial_order = res
            #grouping starts
            if len(chartDimension)>=1:
                # h_order_for_agg_DW = hierarchial_order
                # if "Period" in chartDimension['Timeline Dimension'] and "Period" not in h_order_for_agg_DW:
                #     h_order_for_agg_DW.append("Period")
                aggregatedDataset_grouped_DW = aggregated_data.groupby(hierarchial_order, as_index=False).mean()
                print("===aggregatedDataset_grouped_DW / tabular_grouping_hOrder / drill down.py===")
                print(aggregatedDataset_grouped_DW)
                aggregatedDataset_grouped_DW = aggregatedDataset_grouped_DW.to_dict(orient = 'list')
            #grouping ends

            #to find the hierarchial_order_dw starts
            b = chartDimension
            c = b
            d = list(c.values())
            # z = list(map(str,str(d)))
            z = [x for x in d if len(x)>=1] #for getting only the values with not empty list
            # y = list(itertools.chain(*z))
            # y = [y.replace(('Y' or 'M'), 'Period') for y in y]
            y = [item for sublist in z for item in sublist]
            for i in enumerate(y):
                    y = [sub.replace('M', 'Period') for sub in y]
                    y = [sub.replace('Y', 'Period') for sub in y]
                    y = [x for x in y if len(x)>=2]
            string_ = ' '
            for x in y:
                string_ += ' '+x
            string_ = string_.replace(",", "")
            string_ = string_.split()
            print("===string_=====")
            print(string_)
            if 'Period' in hierarchial_order:
                string_ = string_
            if 'Period' not in hierarchial_order:
                if 'Period' in string_:
                    string_.remove('Period')
            final_agg_list = {}
            for i in string_:
                final_agg_list[i] = aggregatedDataset_grouped_DW[i]
                aggregated_dataset_order = ((list(final_agg_list.keys())))
            #to find the hierarchial_order_dw ends
            tabular_grouping_hOrder_out = {
            "aggregatedDataset_DW" : aggregatedDataset_DW,
            "hierarchialOrder_DW" : aggregated_dataset_order,
            "aggregatedDataset_grouped_DW" : aggregatedDataset_grouped_DW
            }

            return tabular_grouping_hOrder_out

        #level 1 starts
        tabular_level_dict_inputs['level1_ip'] = [hierarchial_order[0]]
        tabular_level_list = [j for i in tabular_level_dict_inputs.values() for j in i]
        chartDimension['Categorical Dimension'] = [hierarchial_order[0]]
        aggregated_data_L1 = aggregated_data[tabular_level_list]
        aggregated_data_L1 = pd.DataFrame(aggregated_data_L1)
        aggregatedDataset_DW_L1 = aggregated_data_L1.to_dict(orient = 'list')
        #tabular l1 inputs
        print("===l1 inputs tabular drill down===")
        print(chartDimension, aggregated_data_L1 , hierarchial_order[0])
        tabular_grouping_hOrder_out_l1 = tabular_grouping_hOrder(chartDimension, aggregated_data_L1, [hierarchial_order[0]]) #aggregated_data is df
        tabular_grouping_hOrder_out_l1 = {'Level1' : tabular_grouping_hOrder_out_l1}
        print("===tabular_grouping_hOrder_out_l1===")
        pprint.pprint(tabular_grouping_hOrder_out_l1)
        tabular_grouping_hOrder_out.append(tabular_grouping_hOrder_out_l1)
        
        #level 1 ends
        
        #level 2 starts
        if len(hierarchial_order)>=2:
            tabular_level_dict_inputs['level2_ip'] = hierarchial_order[0:2]
            tabular_level_list = [j for i in tabular_level_dict_inputs.values() for j in i]
            tabular_level_list = list(set(tabular_level_list))
            chartDimension['Categorical Dimension'] = hierarchial_order[0:2]
            aggregated_data_L2 = aggregated_data[tabular_level_list]
            aggregated_data_L2 = pd.DataFrame(aggregated_data_L2)
            aggregatedDataset_DW_L2 = aggregated_data_L2.to_dict(orient = 'list')
            #tabular l2 inputs
            print("===l2 inputs tabular drill down===")
            print(chartDimension, aggregated_data_L2, hierarchial_order[0:2])
            tabular_grouping_hOrder_out_l2 = tabular_grouping_hOrder(chartDimension, aggregated_data_L2, hierarchial_order[0:2]) #aggregated_data is df
            tabular_grouping_hOrder_out_l2 = {'Level2' : tabular_grouping_hOrder_out_l2}
            print("===tabular_grouping_hOrder_out_l2===")
            pprint.pprint(tabular_grouping_hOrder_out_l2)
            tabular_grouping_hOrder_out.append(tabular_grouping_hOrder_out_l2)
        #level 2 ends
        
        #level 3 starts
        if len(hierarchial_order)>=3:
            tabular_level_dict_inputs['level3_ip'] = hierarchial_order[0:3]
            tabular_level_list = [j for i in tabular_level_dict_inputs.values() for j in i]
            tabular_level_list = list(set(tabular_level_list))
            chartDimension['Categorical Dimension'] = hierarchial_order[0:3]
            aggregated_data_L3 = aggregated_data[tabular_level_list]
            aggregated_data_L3 = pd.DataFrame(aggregated_data_L3)
            aggregatedDataset_DW_L3 = aggregated_data_L3.to_dict(orient = 'list')
            tabular_grouping_hOrder_out_l3 = tabular_grouping_hOrder(chartDimension, aggregated_data_L3, hierarchial_order[0:3]) #aggregated_data is df
            tabular_grouping_hOrder_out_l3 = {'Level3' : tabular_grouping_hOrder_out_l3}
            print("===tabular_grouping_hOrder_out_l3===")
            pprint.pprint(tabular_grouping_hOrder_out_l3)
            tabular_grouping_hOrder_out.append(tabular_grouping_hOrder_out_l3)
        #level 3 ends
        print("======tabular_grouping_hOrder_out======")
        pprint.pprint(tabular_grouping_hOrder_out)
        return tabular_grouping_hOrder_out


