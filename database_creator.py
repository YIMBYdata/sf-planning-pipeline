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
    data = clean_record_type(data,record_type)
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
    print('Generating planner table')
    data, planner = planner_table(data)
    comp_timer.printreport()
    comp_timer.restart()
    print('Generating prj_desc tables')
    prj_desc, prj_desc_detail = prj_desc_table(data)
    comp_timer.printreport()
    comp_timer.restart()
    print('Generating land_use table')
    land_use = land_use_table(data)
    comp_timer.printreport()
    comp_timer.restart()
    print('Generating prj_feature table')
    data = fix_prj_features(data)
    prj_feature = prj_feature_table(data)
    comp_timer.printreport()
    comp_timer.restart()
    print('Generating dwelling tables')
    dwelling, adu_area = dwelling_table(data)
    comp_timer.printreport()
    comp_timer.restart()
    
    return data, record_type, record_rel, location, planner, prj_desc, prj_desc_detail, land_use, prj_feature, dwelling, adu_area
    
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
        lid_counter = int(0)
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

#creates a dataframe for planners
#also adds column planner_id_int
def planner_table(data):
    try:
        newcoltimer = timer()
        #dict mapping planner_strid to planner_id
        pid_dict = {}
        #dict matching planner_id to first index
        index_dict = {}

        #populate above two dicts
        pid_counter = int(0)
        for i,planner in enumerate(data['planner_id']):
            if ~pd.isna(planner):
                if planner in pid_dict:
                    pass
                else:
                    pid_dict[planner] = pid_counter
                    index_dict[pid_counter] = i
                    pid_counter += 1
        
        #temporarily map nan to nan for exception handling
        pid_dict[np.nan] = np.nan
        
        # change planner_id column to int
        data['planner_id_int'] = data.apply(lambda row: pid_dict[row['planner_id']],axis=1)
                              
        del pid_dict[np.nan]
        newcoltimer.pause()
        
        tablepoptimer = timer()
        
        #create location dataframe
        planner_t = pd.DataFrame({'planner_strid':list(pid_dict.keys())})
        for i,planner in enumerate(planner_t['planner_strid']):
            curr_pid = pid_dict[planner]
            planner_t.loc[i,'planner_id'] = curr_pid
            
            #get e-mail, phone, and name
            index = index_dict[curr_pid]
            planner_t.loc[i,'planner_email'] = data.loc[index,'planner_email']
            planner_t.loc[i,'planner_phone'] = data.loc[index,'planner_phone']
            planner_t.loc[i,'planner_name'] = data.loc[index,'planner_name']
        
        tablepoptimer.pause()
            
        return data, planner_t
    except(KeyboardInterrupt):
        #if you interrupt the function while it's running, then it will produce a computation time report
        print('rows so far: %s' % i)
        print('time generating planner_id: %s' % newcoltimer.report())
        print('time populating planner table: %s' % tablepoptimer.report())
    pass
       
#fixes two columns in the data that are empty
def fix_prj_features(data):
    def get_prop(row,feature_net,feature_exist):
        if pd.isna(row[feature_net]):
            return row[feature_exist]
        elif pd.isna(row[feature_exist]):
            return row[feature_net]
        else:
            return row[feature_exist] + row[feature_net]
    data['PRJ_FEATURE_STORIES_PROP'] = data.apply(lambda row: get_prop(
            row,'PRJ_FEATURE_STORIES_NET', 'PRJ_FEATURE_STORIES_EXIST'),axis=1)
    data['PRJ_FEATURE_LOADING_PROP'] = data.apply(lambda row: get_prop(
            row,'PRJ_FEATURE_LOADING_NET', 'PRJ_FEATURE_LOADING_EXIST'),axis=1)
    return data

#creates the prj_desc table in dataframe form
#also creates prj_desc_detail
def prj_desc_table(data):
    #the column names are hard-coded because if the planning department adds a new column somewhere I don't want this to break.
    prj_desc_cols = ["CHANGE_OF_USE", "ADDITIONS", "NEW_CONSTRUCTION", "LEG_ZONE_CHANGE", "DEMOLITION", "LOT_LINE_ADJUST", "FACADE_ALT", "ROW_IMPROVE", "OTHER_PRJ_DESC", "SPECIAL_NEEDS", "SENIOR", "AFFORDABLE_UNITS", "STUDENT", "INCLUSIONARY", "STATE_DENSITY_BONUS", "ADU", "FORMULA_RETAIL", "MCD", "TOBACCO", "FINANCIAL", "MASSAGE", "OTHER_NON_RES"]
    
    #generate lists, which will be used afterwards to create a dataframe
    record_id = []
    desc_type = []
    desc_id_detail = []
    detail = []
    
    for col in prj_desc_cols:
        indices = np.where(data[col]=="CHECKED")
        if len(indices)>0:
            record_id += list(indices[0])
            desc_type += [col]*len(indices[0])
    
    #handling special cases:
    col = "DEMOLITION"
    indices = np.where(data[col]=="Yes")
    if len(indices)>0:
        record_id += list(indices[0])
        desc_type += [col]*len(indices[0])
    
    for col in ["MCD_REFERRAL","ENVIRONMENTAL_REVIEW_TYPE"]:
        indices = np.where(~pd.isna(data[col]))
        if len(indices)>0:
            start = len(record_id)
            record_id += list(indices[0])
            desc_type += [col]*len(indices[0])
            desc_id_detail += range(start,start+len(indices[0]))
            detail += list(data.loc[indices[0],col])
    
    prj_desc = pd.DataFrame({'record_id':record_id,'desc_type':desc_type})
    prj_desc_detail = pd.DataFrame({'prj_desc_id':desc_id_detail,'detail':detail})
    return prj_desc, prj_desc_detail
    
#creates the land_use table in dataframe form
def land_use_table(data):    
    #the column names are hard-coded because if the planning department adds a new column somewhere I don't want this to break.
    land_use_cols = ["RC", "RESIDENTIAL", "CIE", "PDR", "OFFICE", "MEDICAL", "VISITOR", "PARKING_SPACES"]
    
    #generate lists, which will be used afterwards to create a dataframe
    record_id = []
    land_use_type = []
    land_use_exist = []
    land_use_prop = []
    land_use_net = []
    
    for col in land_use_cols:
        col_exist = 'LAND_USE_' + col + '_EXIST'
        col_prop = 'LAND_USE_' + col + '_PROP'
        col_net = 'LAND_USE_' + col + '_NET'
        indices = np.where( np.logical_or(~pd.isna(data[col_exist]),
                                          np.logical_or(~pd.isna(data[col_prop]) ,
                                          ~pd.isna(data[col_net]) ) ) )
        if len(indices)>0:
            record_id += list(indices[0])
            land_use_type += [col]*len(indices[0])
            land_use_exist += list(data.loc[indices[0],col_exist])
            land_use_prop += list(data.loc[indices[0],col_prop])
            land_use_net += list(data.loc[indices[0],col_net])
    
    land_use = pd.DataFrame({'record_id':record_id,'land_use_type':land_use_type,
                             'land_use_exist':land_use_exist,'land_use_prop':land_use_prop,
                             'land_use_net':land_use_net})
    land_use.fillna(value=0,inplace=True)
    
    return land_use

# creates the prj_feature table in dataframe form
def prj_feature_table(data):    
    prj_feature_cols = ["AFFORDABLE", "HOTEL_ROOMS", "MARKET_RATE", "BUILD", "STORIES", "PARKING", "LOADING", "BIKE", "CAR_SHARE", "USABLE", "PUBLIC", "ART", "ROOF", "SOLAR", "LIVING"]
    
    #generate lists, which will be used afterwards to create a dataframe
    record_id = []
    prj_feature_type = []
    prj_feature_exist = []
    prj_feature_prop = []
    prj_feature_net = []
    
    for col in prj_feature_cols:
        col_exist = 'PRJ_FEATURE_' + col + '_EXIST'
        col_prop = 'PRJ_FEATURE_' + col + '_PROP'
        col_net = 'PRJ_FEATURE_' + col + '_NET'
        indices = np.where( np.logical_or(~pd.isna(data[col_exist]),
                                          np.logical_or(~pd.isna(data[col_prop]) ,
                                          ~pd.isna(data[col_net]) ) ) )
        if len(indices)>0:
            record_id += list(indices[0])
            prj_feature_type += [col]*len(indices[0])
            prj_feature_exist += list(data.loc[indices[0],col_exist])
            prj_feature_prop += list(data.loc[indices[0],col_prop])
            prj_feature_net += list(data.loc[indices[0],col_net])
    
    #special handling for "other" feature
    col = 'OTHER'
    col_name = 'PRJ_FEATURE_' + col
    col_exist = 'PRJ_FEATURE_' + col + '_EXIST'
    col_prop = 'PRJ_FEATURE_' + col + '_PROP'
    col_net = 'PRJ_FEATURE_' + col + '_NET'
    indices = np.where( np.logical_or(~pd.isna(data[col_exist]),
                                      np.logical_or(~pd.isna(data[col_prop]) ,
                                      ~pd.isna(data[col_net]) ) ) )
    if len(indices)>0:
        record_id += list(indices[0])
        names = data.loc[indices[0],col_name]
        names.fillna(value='unknown',inplace=True)
        prj_feature_type += ['OTHER: ' + name for name in list(names)]
        prj_feature_exist += list(data.loc[indices[0],col_exist])
        prj_feature_prop += list(data.loc[indices[0],col_prop])
        prj_feature_net += list(data.loc[indices[0],col_net])
    
    prj_feature = pd.DataFrame({'record_id':record_id,'prj_feature_type':prj_feature_type,
                             'prj_feature_exist':prj_feature_exist,'prj_feature_prop':prj_feature_prop,
                             'prj_feature_net':prj_feature_net})
    prj_feature.fillna(value=0,inplace=True)
    
    return prj_feature

#creates the dwelling table in dataframe form
#also creates adu_area
def dwelling_table(data):    
    #the column names are hard-coded because if the planning department adds a new column somewhere I don't want this to break.
    dwelling_cols = ["STUDIO", "1BR", "2BR", "3BR", "GH_ROOMS", "GH_BEDS", "SRO", "MICRO"]
    adu_cols = ["ADU_STUDIO", "ADU_1BR", "ADU_2BR", "ADU_3BR"]
    
    #generate lists, which will be used afterwards to create a dataframe
    record_id = []
    dwelling_type = []
    dwelling_exist = []
    dwelling_prop = []
    dwelling_net = []
    adu_dwelling_id = []
    adu_area = []
    
    for col in dwelling_cols:
        col_exist = 'RESIDENTIAL_' + col + '_EXIST'
        col_prop = 'RESIDENTIAL_' + col + '_PROP'
        col_net = 'RESIDENTIAL_' + col + '_NET'
        indices = np.where( np.logical_or(~pd.isna(data[col_exist]),
                                          np.logical_or(~pd.isna(data[col_prop]) ,
                                          ~pd.isna(data[col_net]) ) ) )
        if len(indices)>0:
            record_id += list(indices[0])
            dwelling_type += [col]*len(indices[0])
            dwelling_exist += list(data.loc[indices[0],col_exist])
            dwelling_prop += list(data.loc[indices[0],col_prop])
            dwelling_net += list(data.loc[indices[0],col_net])
    
    #additional handling for adu columns
    #tricky because not all of these have an area listed
    for col in adu_cols:
        col_area = 'RESIDENTIAL_' + col + '_AREA'
        col_exist = 'RESIDENTIAL_' + col + '_EXIST'
        col_prop = 'RESIDENTIAL_' + col + '_PROP'
        col_net = 'RESIDENTIAL_' + col + '_NET'
        area_indices = np.where( np.logical_or(~pd.isna(data[col_exist]),
                               np.logical_or(~pd.isna(data[col_prop]),
                               np.logical_or(~pd.isna(data[col_net]),
                                             ~pd.isna(data[col_area]) ) ) ) )
        indices = np.where( np.logical_or(~pd.isna(data[col_exist]),
                               np.logical_or(~pd.isna(data[col_prop]),
                               np.logical_or(~pd.isna(data[col_net]),
                                             pd.isna(data[col_area]) ) ) ) )
        if len(area_indices)>0:
            start = len(record_id)
            record_id += list(area_indices[0])
            dwelling_type += [col]*len(area_indices[0])
            dwelling_exist += list(data.loc[area_indices[0],col_exist])
            dwelling_prop += list(data.loc[area_indices[0],col_prop])
            dwelling_net += list(data.loc[area_indices[0],col_net])
            adu_dwelling_id += range(start,start+len(area_indices[0]))
            adu_area += list(data.loc[area_indices[0],col_area])
        if len(indices)>0:
            record_id += list(indices[0])
            dwelling_type += [col]*len(indices[0])
            dwelling_exist += list(data.loc[indices[0],col_exist])
            dwelling_prop += list(data.loc[indices[0],col_prop])
            dwelling_net += list(data.loc[indices[0],col_net])
            
    dwelling = pd.DataFrame({'record_id':record_id,'dwelling_type':dwelling_type,
                             'dwelling_exist':dwelling_exist,'dwelling_prop':dwelling_prop,
                             'dwelling_net':dwelling_net})
    adu_area = pd.DataFrame({'dwelling_id':adu_dwelling_id,'adu_area':adu_area})
    dwelling.fillna(value=0,inplace=True)
    
    return dwelling, adu_area
    
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