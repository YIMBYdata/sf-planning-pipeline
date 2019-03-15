'''
database_creator
Author: Tristan Miller
This module contains functions to create a relational database of the SF Planning records.
The eventual goal is that this can be run periodically to update the database.
'''

import pandas as pd
import numpy as np
import sqlite3 as lite
import re
import time

#loads data from csv file
def load_data():
    return pd.read_csv('planning-department-records-2018/PPTS_Records_data.csv',
    #               parse_dates=['date_opened','date_closed',],
                   infer_datetime_format=True)

#creates tables, cleans up columns, prints out progress
def prepare_data(data):
    comp_timer = timer()
    print('Generating record_type table')
    record_type = record_type_table(data)
    comp_timer.printreport()
    comp_timer.restart()
    print('Generating record_rel table')
    record_rel = record_rel_table(data)
    comp_timer.printreport()
    comp_timer.restart()
    print('Generating location table')
    data, location = location_table(data)
    comp_timer.printreport()
    comp_timer.restart()
    
    print('Cleaning data')
    data = clean_record_type(data,record_type)
    print('Computation time (min): %s' % timer.report())
    comp_timer.restart()
    
#and creates a new dataframe record_type, which will eventually be made into a SQL table.
def record_type_table(data):
    record_type = pd.DataFrame({'original_type':data['record_type_category'].unique()})
    
    #Go through each original_type, standardize the format and find associated info
    for i, og_type in enumerate(record_type['original_type'].unique()):
        #find first row with this record type
        flag = False
        for j, r_type in enumerate(data['record_type_category']):
            if(r_type == og_type):
                record_type.loc[i,'record_type_name'] = data.loc[j,'record_type']
                record_type.loc[i,'record_type_cat'] = data.loc[j,'record_type_type']
                record_type.loc[i,'record_type_subcat'] = data.loc[j,'record_type_subtype']
                #check if format is standardized
                m = re.match('^(...)$',og_type)
                if(m or og_type=="Other"):
                    record_type.loc[i,'record_type'] = og_type
                    flag = True
                    break
                else:
                    #if format is not standardized, try to find the acronym
                    m = re.match('^.* \((...)\)',data.loc[j,'record_type'])
                    if(m):
                        record_type.loc[i,'record_type'] = m.groups()[0]
                        flag = True
                        break
                    else:
                        record_type.loc[i,'record_type'] = og_type
                        #if no acronym is found, better keep looking by continuing the for loop
        #if no acronym is ever found, then we have a real problem
        if not flag:
            print('Error in record_type_table(): Could not determine acronym for %s' % og_type)
                
    return record_type

#clean the record_type_category column through use of the record_type table
def clean_record_type(data,record_type):
    record_type_dict = {}
    for row in record_type.index:
        record_type_dict[record_type.loc[row,'original_type']] = record_type.loc[row,'record_type']
    data['record_type_category'] = data.apply(lambda row: record_type_dict[row['record_type_category']],axis=1)
    return data

#create a new dataframe for parent/child relationships
#parent_id and child_id go by the index of the dataframe, rather than the record_id
def record_rel_table(data):
    child_list = []
    parent_list = []
    
    #this goes much faster if we turn record_id into the index column
    data_reind = data.copy()
    data_reind['old_index'] = data.index
    data_reind = data_reind.set_index('record_id')
    
    for curr_row,children_str in enumerate(data['children']):
        if not pd.isna(children_str):
            children = children_str.split(',')
            for child in children:
                try:
                    parent_list.append(data_reind.loc[child,'old_index'])
                    child_list.append(curr_row)
                except:
                    pass
    
    record_rel = pd.DataFrame({'child_id':child_list,'parent_id':parent_list})
    return record_rel

#creates a new dataframe for unique locations
#also adds new column location_id
def location_table(data):
    try:
        newcoltimer = timer()
        #dict mapping geometry to location_id
        lid_dict = {}
        #dict matching location_id to list of indices in original data
        index_list = {}

        #populate above two dicts
        lid_counter = 0
        for i,geom in enumerate(data['the_geom']):
            if geom in lid_dict:
                index_list[lid_dict[geom]].append(i)
            else:
                lid_dict[geom] = lid_counter
                index_list[lid_counter] = [i]
                lid_counter += 1
        
        #generate location_id column
        data['location_id'] = data.apply(lambda row: lid_dict[row['the_geom']],axis=1)
        newcoltimer.pause()
        
        tablepoptimer = timer()
        addresstimer = timer(start=False)
        
        #create location dataframe
        location = pd.DataFrame({'the_geom':list(lid_dict.keys())})
        for i,geom in enumerate(location['the_geom']):
            tablepoptimer.start()
            curr_lid = lid_dict[geom]
            location.loc[i,'location_id'] = curr_lid
            ilist = index_list[curr_lid]
            location.loc[i,'shape_length'] = data.loc[ilist[0],'Shape_Length']
            location.loc[i,'shape_area'] = data.loc[ilist[0],'Shape_Area']
            tablepoptimer.pause()
            
            addresstimer.start()
            #sometimes addresses aren't listed.  Find the first one that is
            for index in ilist:
                address = data.loc[index,'address']
                if not pd.isna(address):
                    location.loc[i,'address'] = address
                    break
                #if none is ever found, then address will remain as nan
            addresstimer.pause()
            
        return data, location
    except(KeyboardInterrupt):
        #if you interrupt the function while it's running, then it will produce a computation time report
        print('rows so far: %s' % i)
        print('time generating location_id: %s' % newcoltimer.report())
        print('time populating location table: %s' % tablepoptimer.report())
        print('time finding addresses: %s' % addresstimer.report())

#quick timer class for debugging computation time
class timer():
    def __init__(self,start=True):
        self.paused = True
        self.runtime = 0
        if start:
            self.start()
        
    def pause(self):
        if(not self.paused):
            self.runtime += (time.time() - self.checkpoint)/60
            self.paused = True
    
    def start(self):
        if(self.paused):
            self.checkpoint = time.time()
            self.paused = False
    
    def report(self):
        self.pause()
        return self.runtime
    
    def printreport(self):
        print('Time passed: %.2f min' % self.report())
    
    def restart(self):
        self.pause()
        self.runtime = 0
        self.start()