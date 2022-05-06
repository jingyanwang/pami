##############pami_pandas.py###############
import pandas as pd
from pami_re import *

entity_indicator_csv = 'name_male.csv'

def load_entity_from_csv(
	entity_indicator_csv,
	):
	entities = pd.read_csv(
		entity_indicator_csv,
		names = ['indicator'],
		encoding = "ISO-8859-1",
		)
	entities['indicator'] = entities['indicator'].apply(entity2entity_normalized)
	entities = entities.drop_duplicates()
	entities = entities.dropna()
	return entities['indicator'].to_list()

'''
entity_indicator_csv = 'name_male.csv'
load_entity_from_csv(
	entity_indicator_csv,
	)
'''

##############pami_pandas.py###############