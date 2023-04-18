# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 12:44:29 2023

@author: bidzh
"""
import pandas as pd
import requests as re
import csv



def outputData(inputs, request):
    """
        Converts the input data format into pandas dataframe
        Input:
            inputs: type of data - either csv or json
            request: request to the server
        output:
            data: pandas dataframe
    """
    if inputs == 'csv':
        with re.Session() as s:
            download = s.get(request.url)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
        
        data = pd.DataFrame(my_list, columns = my_list[0]).drop(index=[0])
        return data
    else:
        df_json = request.json()
        data = pd.DataFrame.from_dict([df_json]).T   
        return data
