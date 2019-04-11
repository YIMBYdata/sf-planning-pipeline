'''
Author: Tristan Miller
Unit tests for the creation of the ppts database

To run, execute "python -m test_database" from the command line
'''

from unittest import TestCase, main
import pandas as pd
import numpy as np

DATA_SOURCE = "planning-department-records-2018/PPTS_Records_data.csv"
FIELD_SOURCE = "planning-department-records-2018/DataSF_PPTS_Fields.csv"
#may change it later so that the source file is an argument
#import sys
#DATA_SOURCE = sys.argv[1]

class testDataProperties(TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.data = pd.read_csv(DATA_SOURCE)
        cls.fields = pd.read_csv(FIELD_SOURCE)
    
    def test_fields_unchanged(self):
        self.assertEqual(self.fields.shape,(171,2),msg='PPTS_Fields file has changed shape')
        
    def columns_unchanged(self):
        self.assertEqual(self.data.shape[1],172,msg='Number of columns has changed')
    
    
    def decimal_field_tester(self,column,desired_precision,decimal_places,max_digits):
        '''Helper function to be called for testing any DecimalField.
        decimal_places and max_digits are specified in the django model.
        desired_precision is the number of significant digits desired in the smallest value.'''
        min_value = np.min(self.data.loc[np.logical_and(~pd.isna(self.data[column]),
                                                        self.data[column]>0),column])
        max_value = np.max(self.data.loc[np.logical_and(~pd.isna(self.data[column]),
                                                        self.data[column]>0),column])
        if not np.isnan(min_value):
            self.assertTrue(min_value > 10**(-decimal_places-desired_precision+1),
                            msg="not enough decimal places used to model " + column)
            self.assertTrue(max_value < 10**(max_digits-decimal_places),
                            msg="not enough digits used to model " + column)
    
    def char_field_tester(self,column,max_length):
        '''Helper function to be called for testing any CharField.
        max_length is specified in the Django model.'''
        strings = self.data.loc[~pd.isna(self.data[column]),column]
        string_lengths = strings.apply(lambda x: len(x))
        self.assertTrue(np.max(string_lengths) < max_length,msg="Not enough space allocated for string length of " + column)
    
    def test_location_model(self):
        self.decimal_field_tester('Shape_Length',3,8,15)
        self.decimal_field_tester('Shape_Area',3,15,15)
        self.char_field_tester('address',250)
    
    def test_planner_model(self):
        self.char_field_tester('planner_id',100)
        self.char_field_tester('planner_name',100)
        self.char_field_tester('planner_email',100)
        self.char_field_tester('planner_phone',100)
    
    def test_record_type_model(self):
        #note that some of these get additional processing later
        self.char_field_tester('record_type_category',100) 
        self.char_field_tester('record_type',100)
        self.char_field_tester('record_type_subtype',100)
        self.char_field_tester('record_type_type',100)
    
    def test_mcd_referral_model(self):
        num_unique = len(self.data['MCD_REFERRAL'].unique())
        self.assertEqual(num_unique,8,msg="MCD_REFERRAL has new unique values")
    
    def test_env_review_model(self):
        num_unique = len(self.data['ENVIRONMENTAL_REVIEW_TYPE'].unique())
        self.assertEqual(num_unique,22,msg="ENVIRONMENTAL_REVIEW_TYPE has new unique values")
    
    def test_land_use_model(self):
        land_use_cols = ["RC", "RESIDENTIAL", "CIE", "PDR", "OFFICE", "MEDICAL", "VISITOR", "PARKING_SPACES"]
        for col in land_use_cols:
            col_exist = 'LAND_USE_' + col + '_EXIST'
            col_prop = 'LAND_USE_' + col + '_PROP'
            col_net = 'LAND_USE_' + col + '_NET'
            self.decimal_field_tester(col_exist,2,2,15)
            self.decimal_field_tester(col_prop,2,2,15)
            self.decimal_field_tester(col_net,2,2,15)
    
    def test_prj_feature_model(self):
        prj_feature_cols = ["AFFORDABLE", "HOTEL_ROOMS", "MARKET_RATE", "BUILD", "STORIES", "PARKING", "LOADING", "BIKE", "CAR_SHARE", "USABLE", "PUBLIC", "ART", "ROOF", "SOLAR", "LIVING","OTHER"]
        for col in prj_feature_cols:
            col_exist = 'PRJ_FEATURE_' + col + '_EXIST'
            col_prop = 'PRJ_FEATURE_' + col + '_PROP'
            col_net = 'PRJ_FEATURE_' + col + '_NET'
            self.decimal_field_tester(col_exist,2,2,15)
            self.decimal_field_tester(col_prop,2,2,15)
            self.decimal_field_tester(col_net,2,2,15)
        self.char_field_tester('PRJ_FEATURE_OTHER',250)
    
    def test_dwelling_model(self):
        dwelling_cols = ["STUDIO", "1BR", "2BR", "3BR", "GH_ROOMS", "GH_BEDS", "SRO", "MICRO","ADU_STUDIO", "ADU_1BR", "ADU_2BR", "ADU_3BR"]
        adu_cols = ["ADU_STUDIO", "ADU_1BR", "ADU_2BR", "ADU_3BR"]
        for col in dwelling_cols:
            col_exist = 'RESIDENTIAL_' + col + '_EXIST'
            col_prop = 'RESIDENTIAL_' + col + '_PROP'
            col_net = 'RESIDENTIAL_' + col + '_NET'
            self.decimal_field_tester(col_exist,2,2,15)
            self.decimal_field_tester(col_prop,2,2,15)
            self.decimal_field_tester(col_net,2,2,15)
        for col in adu_cols:
            col_area = 'RESIDENTIAL_' + col + '_AREA'
            self.decimal_field_tester(col_area,2,2,15)
            
        
main()