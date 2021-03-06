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

#TODO: Separate out prj_desc_detail table into MCDReferal and EnvironmentalReview tables
#maybe make prj_desc, land_use, and dwelling into many-to-many relationships
#maybe combine dwelling_area with dwelling

#declare names of database columns as constants, so they can be easily adjusted
PLANNER_PK = "id"
PLANNER_ID = "planner_id"
PLANNER_NAME = "name"
PLANNER_EMAIL = "email"
PLANNER_PHONE = "phone"

RECORD_TYPE_PK = "category" #3 letter acronym, ie record_type_category
RECORD_TYPE = "name" #expansion of acronym
RECORD_TYPE_SUBTYPE = "subtype"
RECORD_TYPE_TYPE = "record_type"
RECORD_TYPE_GROUP = "record_group" #can't call it record_group because group is a keyword in sqlite3
RECORD_TYPE_MODULE = "module"

LOCATION_PK = "id"
LOCATION_GEOM = "the_geom"
LOCATION_ADDRESS = "address"
LOCATION_SHAPE_LENGTH = "shape_length"
LOCATION_SHAPE_AREA = "shape_area"

PRJ_DESC_PK = "id"
PRJ_DESC_FK = "record"
PRJ_DESC_TYPE = "desc_type"
PRJ_DESC_DETAIL_PK = "desc_id"
PRJ_DESC_DETAIL = "detail"

LAND_USE_PK = "id"
LAND_USE_FK = "record"
LAND_USE_TYPE = "land_use_type"
LAND_USE_EXIST = "exist"
LAND_USE_PROP = "prop"
LAND_USE_NET = "net"

PRJ_FEATURE_PK = "id"
PRJ_FEATURE_FK = "record"
PRJ_FEATURE_TYPE = "feature_type"
PRJ_FEATURE_EXIST = "exist"
PRJ_FEATURE_PROP = "prop"
PRJ_FEATURE_NET = "net"

DWELLING_PK = "id"
DWELLING_FK = "record"
DWELLING_TYPE = "dwelling_type"
DWELLING_EXIST = "exist"
DWELLING_PROP = "prop"
DWELLING_NET = "net"
ADU_PK = "dwelling_id"
ADU_AREA = "area"

HEARING_PK = "id"
HEARING_FK = "record"
HEARING_TYPE = "hearing_type"
HEARING_DATE = "date"

RECORD_PK = "id"
RECORD_FK_PLANNER = "planner"
RECORD_FK_LOCATION = "location"
RECORD_FK_TYPE = "category"
RECORD_ID = "record_id"
RECORD_OBJECT_ID = "object_id"
RECORD_TEMPLATE_ID = "template_id"
RECORD_NAME = "name"
RECORD_DESCRIPTION = "description"
RECORD_STATUS = "status"
RECORD_CONSTRUCT_COST = "construct_cost"
RECORD_BUILDING_PERMIT = "related_building_permit"
RECORD_ACALINK = "acalink"
RECORD_AALINK = "aalink"
RECORD_YEAR_OPENED = "year_opened"
RECORD_MONTH_OPENED = "month_opened"
RECORD_DAY_OPENED = "day_opened"
RECORD_YEAR_CLOSED = "year_closed"
RECORD_MONTH_CLOSED = "month_closed"
RECORD_DAY_CLOSED = "day_closed"

RECORD_REL_PK = "id"
RECORD_REL_PARENT = "parent"
RECORD_REL_CHILD = "child"

# creates a new database
# to execute this from bash, please use db_create.py
def create(source, destination):
    data = pd.read_csv(source)
    data, record_type, record_rel, location, planner, prj_desc, prj_desc_detail,land_use, prj_feature, dwelling, adu_area, hearing_date = prepare_data(data)
    
    comp_timer = timer()
    print('Generating SQL file')
    init_sql_database(destination, data, record_type, record_rel, location, planner, prj_desc,
                      prj_desc_detail, land_use, prj_feature, dwelling, adu_area, hearing_date)
    comp_timer.printreport()
    comp_timer.restart()

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
    print('Generating year/month/day')
    data = ymd(data)
    comp_timer.printreport()
    comp_timer.restart()
    print('Generating hearing date table')
    hearing_date = hearing_date_table(data)
    comp_timer.printreport()
    comp_timer.restart()
    
    #one last thing: clean nans in constructcost
    data['constructcost'] = data['constructcost'].fillna(value=0)
    
    return data, record_type, record_rel, location, planner, prj_desc, prj_desc_detail, land_use, prj_feature, dwelling, adu_area, hearing_date

#initializes a sql database, given all the appropriate pandas tables, and a target destination
#for the database schema, please see database_structure.xlsx
def init_sql_database(destination, data, record_type, record_rel, location, planner, prj_desc,
                      prj_desc_detail, land_use, prj_feature, dwelling, adu_area, hearing_date):
    con = lite.connect(destination)
    try:
        cur = con.cursor()
        
        ### record
        sqlcmd = '''
        create table record( 
            %s integer primary key autoincrement,
            %s text,
            %s integer, 
            %s integer, 
            %s text, %s integer, %s text,
            %s text, %s text, %s text,
            %s real, %s text, %s text, %s text,
            %s integer, %s integer, %s integer,
            %s integer, %s integer, %s integer
             )''' % (RECORD_PK, RECORD_FK_TYPE, RECORD_FK_PLANNER, RECORD_FK_LOCATION, RECORD_ID,
                     RECORD_OBJECT_ID, RECORD_TEMPLATE_ID, RECORD_NAME, RECORD_DESCRIPTION, RECORD_STATUS,
                     RECORD_CONSTRUCT_COST, RECORD_BUILDING_PERMIT, RECORD_ACALINK, RECORD_AALINK,
                     RECORD_YEAR_OPENED, RECORD_MONTH_OPENED, RECORD_DAY_OPENED,
                     RECORD_YEAR_CLOSED, RECORD_MONTH_CLOSED, RECORD_DAY_CLOSED)
        cur.execute(sqlcmd)
        #create new dataframe with only the desired columns, relabeled as needed
        data_transfer = pd.DataFrame({                
              RECORD_FK_PLANNER:data['planner_id_int'],RECORD_FK_LOCATION:data['location_id'],
              RECORD_FK_TYPE:data['record_type_category'],RECORD_ID:data['record_id'],
              RECORD_NAME:data['record_name'],RECORD_DESCRIPTION:data['description'],
              RECORD_STATUS:data['record_status'],
              RECORD_OBJECT_ID:data['OBJECTID'],RECORD_TEMPLATE_ID:data['templateid'],
              RECORD_CONSTRUCT_COST:data['constructcost'], RECORD_BUILDING_PERMIT:data['RELATED_BUILDING_PERMIT'],
              RECORD_ACALINK:data['acalink'],RECORD_AALINK:data['aalink'],
              RECORD_YEAR_OPENED:data['year_opened'], RECORD_MONTH_OPENED:data['month_opened'], RECORD_DAY_OPENED:data['day_opened'],
              RECORD_YEAR_CLOSED:data['year_closed'], RECORD_MONTH_CLOSED:data['month_closed'], RECORD_DAY_CLOSED:data['day_closed']
            })
        data_transfer.to_sql('record',con,if_exists='append',index_label=RECORD_PK)
        
        #to_sql will do a bunch of this stuff for me,
        #but I think it's better to explicitly create the table to ensure all the types are correct
        
        ### planner
        sqlcmd = '''create table planner(
            %s integer primary key autoincrement,
            %s text, %s text, %s text, %s text)''' % (PLANNER_PK,PLANNER_ID,PLANNER_NAME,PLANNER_EMAIL,PLANNER_PHONE)
        cur.execute(sqlcmd)
        planner.to_sql('planner',con,if_exists='append',index=False)
        
        ### record_type
        sqlcmd = '''create table record_type(
            %s text primary key,
            %s text, %s text, %s text,
            %s text, %s text)''' % (RECORD_TYPE_PK,RECORD_TYPE,RECORD_TYPE_SUBTYPE,RECORD_TYPE_TYPE,RECORD_TYPE_GROUP,RECORD_TYPE_MODULE)
        cur.execute(sqlcmd)
        #there was an extra column that I don't want to write to the db
        record_type = record_type.drop(labels='original_type',axis=1)
        record_type.to_sql('record_type',con,if_exists='append',index=False)
        
        ### location
        sqlcmd = '''create table location(
            %s integer primary key autoincrement,
            %s text, %s text, %s real, %s real)''' % (LOCATION_PK,LOCATION_GEOM,LOCATION_ADDRESS,LOCATION_SHAPE_LENGTH,LOCATION_SHAPE_AREA)
        cur.execute(sqlcmd)
        location.to_sql('location',con,if_exists='append',index=False)
        
        ### prj_desc
        sqlcmd = '''create table prj_desc(
            %s integer primary key autoincrement,
            %s integer, %s text)''' % (PRJ_DESC_PK, PRJ_DESC_FK, PRJ_DESC_TYPE)
        cur.execute(sqlcmd)
        prj_desc.to_sql('prj_desc',con,if_exists='append',index_label=PRJ_DESC_PK)
        
        ### prj_desc_detail
        sqlcmd = '''create table prj_desc_detail(
            %s integer primary key,
            %s text)''' % (PRJ_DESC_DETAIL_PK, PRJ_DESC_DETAIL)
        cur.execute(sqlcmd)
        prj_desc_detail.to_sql('prj_desc_detail',con,if_exists='append',index=False)
        
        ### land_use
        sqlcmd = '''create table land_use(
            %s integer primary key autoincrement,
            %s integer,
            %s text, %s real, %s real,%s real)''' % (LAND_USE_PK, LAND_USE_FK, LAND_USE_TYPE, LAND_USE_EXIST, LAND_USE_PROP, LAND_USE_NET)
        cur.execute(sqlcmd)
        land_use.to_sql('land_use',con,if_exists='append',index_label=LAND_USE_PK)
        
        ### prj_feature
        sqlcmd = '''create table prj_feature(
            %s integer primary key autoincrement,
            %s integer,
            %s text, %s integer, %s integer, %s integer)''' % (PRJ_FEATURE_PK, PRJ_FEATURE_FK, PRJ_FEATURE_TYPE, PRJ_FEATURE_EXIST, PRJ_FEATURE_PROP, PRJ_FEATURE_NET)
        cur.execute(sqlcmd)
        prj_feature.to_sql('prj_feature',con,if_exists='append',index_label=PRJ_FEATURE_PK)
        
        ### dwelling
        sqlcmd = '''create table dwelling(
            %s integer primary key autoincrement,
            %s integer,
            %s text, %s integer, %s integer, %s integer)''' % (DWELLING_PK, DWELLING_FK, DWELLING_TYPE, DWELLING_EXIST, DWELLING_PROP, DWELLING_NET)
        cur.execute(sqlcmd)
        dwelling.to_sql('dwelling',con,if_exists='append',index_label=DWELLING_PK)
                           
        ### adu_area
        sqlcmd = '''create table adu_area(
            %s integer primary key,
            %s real)''' % (ADU_PK, ADU_AREA)
        cur.execute(sqlcmd)
        adu_area.to_sql('adu_area',con,if_exists='append',index=False)
        
        ### record_rel
        sqlcmd = '''create table record_rel(
            %s integer primary key autoincrement,
            %s integer, %s integer)''' % (RECORD_REL_PK, RECORD_REL_PARENT, RECORD_REL_CHILD)
        cur.execute(sqlcmd)
        record_rel.to_sql('record_rel',con,if_exists='append',index_label=RECORD_REL_PK)
        
        ### hearing_date
        sqlcmd = '''create table hearing_date(
            %s integer primary key,
            %s integer, %s text, %s text)''' % (HEARING_PK, HEARING_FK, HEARING_TYPE, HEARING_DATE)
        cur.execute(sqlcmd)
        hearing_date.to_sql('hearing_date',con,if_exists='append',index_label=HEARING_PK)
    finally:    
        con.close()
    
#and creates a new dataframe record_type, which will eventually be made into a SQL table.
def record_type_table(data):
    record_type = pd.DataFrame({'original_type':data['record_type_category'].unique()})
    
    #Go through each original_type, standardize the format and find associated info
    for i, og_type in enumerate(record_type['original_type'].unique()):
        #find first row with this record type
        flag = False
        for j, r_type in enumerate(data['record_type_category']):
            if(r_type == og_type):
                record_type.loc[i,RECORD_TYPE] = data.loc[j,'record_type']
                record_type.loc[i,RECORD_TYPE_TYPE] = data.loc[j,'record_type_type']
                record_type.loc[i,RECORD_TYPE_SUBTYPE] = data.loc[j,'record_type_subtype']
                record_type.loc[i,RECORD_TYPE_GROUP] = data.loc[j,'record_type_group']
                record_type.loc[i,RECORD_TYPE_MODULE] = data.loc[j,'module']
                #check if format is standardized
                m = re.match('^(...)$',og_type)
                if(m or og_type=="Other"):
                    record_type.loc[i,RECORD_TYPE] = og_type
                    flag = True
                    break
                else:
                    #if format is not standardized, try to find the acronym
                    m = re.match('^.* \((...)\)',data.loc[j,'record_type'])
                    if(m):
                        record_type.loc[i,RECORD_TYPE] = m.groups()[0]
                        flag = True
                        break
                    else:
                        record_type.loc[i,RECORD_TYPE] = og_type
                        #if no acronym is found, better keep looking by continuing the for loop
        #if no acronym is ever found, then we have a real problem
        if not flag:
            print('Error in record_type_table(): Could not determine acronym for %s' % og_type)
    
    return record_type

#clean the record_type_category column through use of the record_type table
def clean_record_type(data,record_type):
    record_type_dict = {}
    for row in record_type.index:
        record_type_dict[record_type.loc[row,'original_type']] = record_type.loc[row,RECORD_TYPE]
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
    
    record_rel = pd.DataFrame({RECORD_REL_CHILD:child_list,RECORD_REL_PARENT:parent_list})
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
        location = pd.DataFrame({LOCATION_GEOM:list(lid_dict.keys())})
        for i,geom in enumerate(location[LOCATION_GEOM]):
            tablepoptimer.start()
            curr_lid = lid_dict[geom]
            location.loc[i,LOCATION_PK] = curr_lid
            ilist = index_list[curr_lid]
            location.loc[i,LOCATION_SHAPE_LENGTH] = data.loc[ilist[0],'Shape_Length']
            location.loc[i,LOCATION_SHAPE_AREA] = data.loc[ilist[0],'Shape_Area']
            tablepoptimer.pause()
            
            addresstimer.start()
            #sometimes addresses aren't listed.  Find the first one that is
            for index in ilist:
                address = data.loc[index,'address']
                if not pd.isna(address):
                    location.loc[i,LOCATION_ADDRESS] = address
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
        planner_t = pd.DataFrame({PLANNER_ID:list(pid_dict.keys())})
        for i,planner in enumerate(planner_t[PLANNER_ID]):
            curr_pid = pid_dict[planner]
            planner_t.loc[i,PLANNER_PK] = curr_pid
            
            #get e-mail, phone, and name
            index = index_dict[curr_pid]
            planner_t.loc[i,PLANNER_EMAIL] = data.loc[index,'planner_email']
            planner_t.loc[i,PLANNER_PHONE] = data.loc[index,'planner_phone']
            planner_t.loc[i,PLANNER_NAME] = data.loc[index,'planner_name']
        
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
        #the following line causes a warning when performed on empty columns (such as leg_zone_change)
        #I think the warning can be ignored.
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
    
    prj_desc = pd.DataFrame({PRJ_DESC_FK:record_id,PRJ_DESC_TYPE:desc_type})
    prj_desc_detail = pd.DataFrame({PRJ_DESC_DETAIL_PK:desc_id_detail,PRJ_DESC_DETAIL:detail})
    return prj_desc, prj_desc_detail

#creates the hearing_date table in dataframe form
def hearing_date_table(data):
    #the column names are hard-coded because if the planning department adds a new column somewhere I don't want this to break.
    hearing_date_cols = ["BOS_1ST_READ","BOS_2ND_READ","COM_HEARING","MAYORAL_SIGN","TRANSMIT_DATE_BOS","COM_HEARING_DATE_BOS"]
    
    #generate lists, which will be used afterwards to create a dataframe
    record_id = []
    hearing_type = []
    date = []
    
    for col in hearing_date_cols:
        indices = np.where(~pd.isna(data[col]))
        #the following line causes a warning when performed on empty columns (which is true of several of them)
        #I think the warning can be ignored.
        indices = np.where(data[col]=="CHECKED")
        if len(indices)>0:
            record_id += list(indices[0])
            hearing_type += [col]*len(indices[0])
            date += list(data.loc[indices[0],col])
    
    hearing_date = pd.DataFrame({HEARING_FK:record_id,HEARING_TYPE:hearing_type,HEARING_DATE:date})
    return hearing_date
    
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
    
    land_use = pd.DataFrame({LAND_USE_FK:record_id,LAND_USE_TYPE:land_use_type,
                             LAND_USE_EXIST:land_use_exist,LAND_USE_PROP:land_use_prop,
                             LAND_USE_NET:land_use_net})
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
    
    prj_feature = pd.DataFrame({PRJ_FEATURE_FK:record_id,PRJ_FEATURE_TYPE:prj_feature_type,
                             PRJ_FEATURE_EXIST:prj_feature_exist,PRJ_FEATURE_PROP:prj_feature_prop,
                             PRJ_FEATURE_NET:prj_feature_net})
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
            
    dwelling = pd.DataFrame({DWELLING_FK:record_id,DWELLING_TYPE:dwelling_type,
                             DWELLING_EXIST:dwelling_exist,DWELLING_PROP:dwelling_prop,
                             DWELLING_NET:dwelling_net})
    adu_area = pd.DataFrame({ADU_PK:adu_dwelling_id,ADU_AREA:adu_area})
    dwelling.fillna(value=0,inplace=True)
    
    return dwelling, adu_area

#generates additional columns for year, month, day, for date opened and date closed
def ymd(data):
    #internal function that returns year, month, day, or nans if date is missing
    def parse_date(datestr):
        if not pd.isna(datestr):
            m = re.match('^(\d+)/(\d+)/(\d+)',datestr)
            return m.groups()[2], m.groups()[0], m.groups()[1]
        else:
            return np.nan, np.nan, np.nan
    
    #now apply the function and unpack results
    date_opened = data['date_opened'].apply(lambda dt: parse_date(dt))
    date_closed = data['date_closed'].apply(lambda dt: parse_date(dt))
    
    data['year_opened'] = date_opened.apply(lambda dt: dt[0])
    data['month_opened'] = date_opened.apply(lambda dt: dt[1])
    data['day_opened'] = date_opened.apply(lambda dt: dt[2])
    data['year_closed'] = date_closed.apply(lambda dt: dt[0])
    data['month_closed'] = date_closed.apply(lambda dt: dt[1])
    data['day_closed'] = date_closed.apply(lambda dt: dt[2])
    return data

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