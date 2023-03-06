import pandas as pd
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



class Suggestions:
    def __init__(self, suggestions_out):
        self.suggestions_out = suggestions_out

    @staticmethod
    def get_suggestions(data):
        data_df = pd.DataFrame(data)
        # print((tabulate(data_df, headers = 'keys', tablefmt = 'psql')))
        statement = []
        if "Revenue" in data_df:
            mean_Revenue = int(data_df['Revenue'].mean())
            statement.append(f"The total Revenue of the company is {mean_Revenue}.")
        if "Expense" in data_df:
            mean_Expense = int(data_df['Expense'].mean())
            statement.append(f"The total Expense of the company is {mean_Expense}.")
        elif "Expense" in data_df and "Revenue" in data_df:
            mean_Revenue = int(data_df['Revenue'].mean())
            mean_Expense = int(data_df['Expense'].mean())
            statement.append(f"The total Revenue and Expense of the company is {mean_Revenue}, {mean_Expense}.")
        if "Profit" in data_df:
            mean_Profit = int(data_df['Profit'].mean())
            statement.append(f"With the comparison of Expense and Revenue the Profit of is {mean_Profit}.")
        if "Revenue" in data_df and "Profit" not in data_df:
            mean_Revenue = int(data_df['Revenue'].mean())
            statement.append(f"The total Revenue of the company is {mean_Revenue}. Kindly concentrate on the Company's growth.")  
        statement = ' '.join(statement)
        print(' '.join(statement))
        return statement