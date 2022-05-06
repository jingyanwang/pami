##################pami_spark.py##################
'''
export PYSPARK_PYTHON=/usr/local/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3
'''

import re
import os
import time
import random

from os import listdir
from os import path

from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pami_re import *

'''
sudo rm entities.csv
sudo vi entities.csv
iwant
Jim Smith
al - 
_name_

entity_indicator_csv = 'entities.csv'

load_entity_from_csv(\
	entity_indicator_csv,\
	sqlContext = None).show()
'''

def load_entity_from_csv(\
	entity_indicator_csv,\
	sqlContext = None):
	if sqlContext is None:
		sqlContext = sqlContext_local
	##
	#print('loading indicators from '+entity_indicator_csv)
	sqlContext.read.format("csv")\
		.option("header", "false")\
		.schema(StructType([\
		StructField("indicator", StringType(), True)]))\
		.load(entity_indicator_csv)\
		.withColumn('indicator',\
			udf(entity2entity_normalized, StringType())\
			('indicator'))\
		.registerTempTable('entities')
	entities_df = sqlContext.sql(u"""
		SELECT DISTINCT * 
		FROM entities
		WHERE indicator IS NOT NULL
		""")
	entities_df = entities_df.cache()
	##
	return entities_df

'''
sudo rm input.json
sudo vi input.json
i{"text":"my http://ascii-table.com/ascii.php is not goo"}
{"text":"i will come at 11:11am"}
{"text":"%^& is xgge2245@gmail.com 778"}

input_json = 'input.json'
output_json = 'output.json'
text_json2text_normalized_json(\
	input_json,\
	output_json,\
	save_to_local = True)
'''
def text_json2text_normalized_json(\
	input_json,\
	output_json = None,\
	sqlContext = None):
	start_time = time.time()
	if sqlContext is None:
		sqlContext = sqlContext_local
	print('loading data from '+input_json)
	input_df = sqlContext.read.json(input_json)
	print('extracting url, email, number, and puntuation')
	customSchema = StructType([\
		StructField("text_normalized", StringType(), True),\
		StructField("url", ArrayType(StringType()), True),\
		StructField("email", ArrayType(StringType()), True),\
		StructField("number",ArrayType(StringType()), True),\
		StructField("puntuation", ArrayType(StringType()), True)])
	udf_func = udf(text2text_normalized, customSchema)
	input_df.withColumn('output_temp', udf_func('text'))\
		.registerTempTable('temp')
	output_df = sqlContext.sql(u"""
		SELECT *, output_temp.text_normalized,
		output_temp.url,
		output_temp.email,
		output_temp.number,
		output_temp.puntuation
		FROM temp
		""").drop('output_temp')
	output_df = output_df.cache()
	if output_json is not None:
		print('saving output to '+output_json)
		output_df.write.mode("overwrite").json(output_json)
		print('output saved to '+output_json)
	print('runing time: '+str(time.time()-start_time))
	return output_df

def merge_entities(
	input_df,\
	sub_entities = []):
	udf_comb_enti_int = udf(\
		lambda input: {}, \
		MapType(StringType(),ArrayType(StringType())))
	input_df.drop('entities')
	input_df = input_df.withColumn('entities',\
		udf_comb_enti_int('text'))
	###
	def add_new_entity(entities,
		new_entities,\
		new_entity_type):
		try:
			entities[new_entity_type] = \
				[e['entity'] for e in new_entities]
		except:
			entities[new_entity_type] = new_entities
		return entities
	#combine the sub entities to one
	for sub_entity in sub_entities:
		if sub_entity in input_df.columns:
			input_df = input_df.withColumn(\
				'entities',\
				udf(lambda entities, new_entities:\
				add_new_entity(entities,
				new_entities, sub_entity), \
				MapType(StringType(),ArrayType(StringType())))\
				('entities', sub_entity))
	return input_df

'''
using a lookup tabel and a function 
to extract entities 
from text and text_normalized

sudo rm input.json
sudo vi input.json
i{"text":"this is a text"}
{"text":"i am from Abu Dhabi."}
{"text":"i am from Abu Dhabi. come to building 8"}

text_json2text_normalized_json(\
	input_json = 'input.json',\
	output_json = 'output.json')

sudo rm entities.csv
sudo vi entities.csv
iAbu Dhabi
dubai
build _number_
2-th streat
_name_ house

sudo rm input.json
sudo vi input.json
i{"text":"this is a text","text_normalized":" _start_ this is a text _end_ ","url":[],"email":[],"number":[],"puntuation":[],"name":[]}
{"text":"i am from Abu Dhabi.","text_normalized":" _start_ i am from abu dhabi . _end_ ","url":[],"email":[],"number":[],"puntuation":["."],"name":[]}
{"text":"i am from Abu Dhabi. come to build 8","text_normalized":" _start_ i am from abu dhabi . come to build 8 _end_ ","url":[],"email":[],"number":["8"],"puntuation":["."],"name":[]}
{"text":"i am from Abu Dhabi. come to build 8 in jim house","text_normalized":" _start_ i am from abu dhabi . come to build 8 in jim house _end_ ","url":[],"email":[],"number":["8"],"puntuation":["."],"name":["jim"]}

from pami_spark import *
text_normalized_entity_extraction(\
	input_json = 'input.json',\
	output_json = 'output.json',\
	entity_type = 'location',
	entity_indicator_csv = 'entities.csv',\
	entity_indicator_match_by_word = True,\
	sub_entities = ['number','name'])

from pami_spark import *
def location_extraction(text, \
	text_normalized = None, \
	entities = None):
	output = []
	###
	outputs = text_normalized2text_entity_comb(\
		text_normalized, \
		entities = entities,\
		indicator_re = r'(build _number_)')
	output += [{'entity': e, 'method':'jim1'}\
		for e in outputs]
	###
	outputs = text_normalized2text_entity_comb(\
		text_normalized, \
		entities = entities,\
		indicator_re = r'(abu dhabi|dubai)')
	output += [{'entity': e, 'method':'jim2'}\
		for e in outputs]
	###
	return output

text_normalized_entity_extraction(\
	input_json = 'input.json',\
	output_json = 'output.json',\
	entity_type = 'location',
	entity_indicator_csv = 'entities.csv',\
	entity_extract_func = location_extraction,\
	entity_indicator_match_by_word = True,\
	sub_entities = ['number','name'])

def from_location(text, \
	text_normalized = None, \
	entities = None):
	output = []
	###
	outputs = text_normalized2text_entity_comb(\
		text_normalized, \
		entities = entities,\
		indicator_re = r'from _location_')
	output += [{'entity': e, 'method':'jim3'}\
		for e in outputs]
	try:
		e1 = re.search(r'I am from [A-Z ]+\.', text).group()
		output += [{'entity': \
			text2url_email_number_puntuation(e1)['text_normalized'], \
			'method':'nlp'}]
	except:
		pass
	###
	return output

from_location(\
	text = 'I am from ABU DHABI. come to from ', \
	text_normalized = " _start_ i am from abu dhabi . come to from build 8 _end_ ", \
	entities = {"number":["8"],"puntuation":["."],\
	'location':['abu dhabi','build 8']})

text_normalized_entity_extraction(\
	input_json = 'output.json',\
	output_json = 'output1.json',\
	entity_type = 'fromlocation',
	entity_extract_func = from_location,\
	sub_entities = ['number', 'puntuation', 'location'])

cat output1.json/*
'''
def text_normalized_entity_extraction(\
	input_json,\
	output_json,\
	entity_type,
	entity_extract_func = None,\
	entity_indicator_csv = None,\
	entity_indicator_match_by_word = False,\
	sub_entities = [],\
	show_number_of_matched_texts = True,\
	sqlContext = None,\
	workspace = '.'):
	start_time = time.time()
	if sqlContext is None:
		sqlContext = sqlContext_local
	print('loading data from '+input_json)
	input_df = sqlContext.read.json(input_json)
	print('loaded '+str(input_df.count())+' records')
	##create the entities cloumn for further matching
	input_df = merge_entities(input_df,	sub_entities = sub_entities)
	input_df = input_df.drop(entity_type)
	input_df = input_df.cache()
	output_df1 = input_df
	####using the functions to extract entities
	if entity_extract_func is not None:		
		print('extracting entities by function '\
			+str(entity_extract_func))
		udf_func_entit_extract = udf(lambda \
			text, text_normalized, entities:
			entity_extract_func(text, \
				text_normalized, \
				entities), \
			ArrayType(MapType(StringType(),StringType())))
		output_df1 = output_df1.withColumn(\
			'entity_func', \
			udf_func_entit_extract('text', \
			'text_normalized',\
			'entities'))
		output_df1 = output_df1.cache()
	###using the indicators to extract entities
	output_df2 = output_df1
	entity_matching_start_time = time.time()
	if entity_indicator_csv is not None:
		print('loading indicators from '+entity_indicator_csv)
		entities_df = load_entity_from_csv(\
			entity_indicator_csv,\
			sqlContext = sqlContext)
		#########the first method is based on linear 
		#search over the csv file indicators
		#by putting all indicators as candidats
		if entity_indicator_match_by_word is False:
			entities_list = [row['indicator'] for row in entities_df.collect()]
			output_df2 = output_df2.withColumn(\
				'indicators', \
				udf(lambda input: entities_list,\
				ArrayType(StringType()))('text'))
			output_df2 = output_df2.cache()
		####the seconde method is based on 
		#spark sql join to filter the indicators
		#which only has the overlapping words with
		#text
		else:
			print('spliting the text into words')
			udf_wildcard_text_normalized = \
				udf(lambda input, entities: \
				text_normalized2text_entity_wild(input, entities, wild_entities = sub_entities), \
				StringType())
			output_df3 = output_df2.withColumn(\
				'words', udf_wildcard_text_normalized('text_normalized', 'entities'))
			output_df3 = output_df3.withColumn(\
				'words', udf(text_normalized2words, ArrayType(StringType()))('words'))
			output_df3.registerTempTable('output_df3')
			output_df3 = sqlContext.sql(u"""
				SELECT DISTINCT text_normalized, words
				FROM (
				SELECT text_normalized, EXPLODE(words) AS words
				FROM output_df3
				) AS temp
				WHERE words NOT IN ('_start_', '_end_')
				""")
			#, INT(HASH(words)%100000) AS word_hash_key
			output_df3.write.mode('Overwrite').json('%s/output_df3.json'%(workspace))
			#.repartition(2000).persist(StorageLevel.MEMORY_ONLY_SER)
			sqlContext.read.json('%s/output_df3.json'%(workspace)).repartition('words').registerTempTable('output_df3')
			print('spliting the indicator to words')
			entities_df = entities_df.withColumn('words1', udf(text_normalized2words, \
				ArrayType(StringType()))('indicator'))
			entities_df.registerTempTable('entities_df')
			entities_df = sqlContext.sql(u"""
				SELECT DISTINCT indicator, words1
				FROM (
				SELECT indicator, EXPLODE(words1) AS words1
				FROM entities_df
				) AS temp
				""")
			#, INT(HASH(words1)%100000) AS words1_hash_key
			entities_df.write.mode('Overwrite').json('%s/entities_df.json'%(workspace))
			sqlContext.read.json('%s/entities_df.json'%(workspace)).repartition('words1').registerTempTable('entities_df')
			print('join the text and indicator by words')
			word_matching_start_time = time.time()
			sqlContext.sql(u"""
				SELECT output_df3.*, entities_df.indicator, entities_df.words1
				FROM output_df3
				JOIN entities_df
				ON entities_df.words1 = output_df3.words
				""").write.mode('Overwrite').json('%s/candidate_df1.json'%(workspace))
			sqlContext.read.json('%s/candidate_df1.json'%(workspace)).repartition('text_normalized').repartition('indicator').registerTempTable('candidate_df1')
			sqlContext.sql(u"""
				SELECT text_normalized, COLLECT_SET(indicator) AS indicators
				FROM candidate_df1
				GROUP BY text_normalized
				""").write.mode('Overwrite').json('%s/candidate_df.json'%(workspace))
			sqlContext.read.json('%s/candidate_df.json'%(workspace)).registerTempTable('candidate_df')
			print('word joining of text and indicator completed, running time:\t'+str(time.time()-word_matching_start_time)+' seconds')
			print('joining the candidate indicators to the text')
			output_df2.registerTempTable('output_df2')
			output_df2 = sqlContext.sql(u"""
				SELECT output_df2.*, candidate_df.indicators
				FROM output_df2
				LEFT JOIN candidate_df
				ON candidate_df.text_normalized = output_df2.text_normalized
				""").withColumn('indicators', \
				udf(lambda input: [] if input is None else input,\
				ArrayType(StringType()))('indicators'))
			output_df2 = output_df2.cache()
		####finished the candidate indicators finding
		print('matching the candidate indicators in '+entity_indicator_csv)
		def match_entity_list(input, entities, indicators):
			outputs = []
			for indicator in indicators:
				output = text_normalized2text_entity_comb(\
					input, 
					indicator_text = indicator,\
					entities = entities)
				if len(output) > 0:
					outputs += [{'entity': e, 'method':	'lookup: '+indicator} for e in output]
			return outputs
		##
		udf_func_entit_extract = udf(match_entity_list, \
			ArrayType(MapType(StringType(),StringType())))
		output_df2 = output_df2.drop('entity_indi')
		output_df2 = output_df2.withColumn(\
			'entity_indi', \
			udf_func_entit_extract('text_normalized', 'entities', 'indicators'))
		output_df2 = output_df2.drop('indicators')
		output_df2 = output_df2.cache()
	##afeter using the entities, drop it
	output_df2 = output_df2.drop("entities")
	output_df2.write.mode('Overwrite').json(
		path.join(workspace, 'output_df2.json')
		)
	output_df2 = sqlContext.read.json(path.join(workspace, 'output_df2.json'))
	print('entity matching completed, running time:\t'+str(time.time() - entity_matching_start_time)+' seconds')
	'''
	merget output_df1 and output_df2
	'''
	if 'entity_func' in output_df2.columns \
		and 'entity_indi'  not in output_df2.columns:
		output_df2 = output_df2.withColumnRenamed(\
			'entity_func', entity_type)
	if 'entity_indi' in output_df2.columns \
		and 'entity_func'  not in output_df2.columns:
		output_df2 = output_df2.withColumnRenamed(\
			'entity_indi', entity_type)
	if 'entity_indi' in output_df2.columns \
		and 'entity_func' in output_df2.columns:
		output_df2 = output_df2.withColumn(\
			entity_type,
			udf(lambda entity_func, entity_indi:
			entity_func+entity_indi,\
			ArrayType(MapType(StringType(),StringType())))\
			('entity_indi', 'entity_func'))\
			.drop('entity_indi').drop('entity_func')
	output_df = output_df2
	output_df = output_df.cache()
	###
	if show_number_of_matched_texts is True:
		output_df.registerTempTable('output_df')
		sqlContext.sql(u"""
			SELECT has_entity, COUNT(*)
			FROM (
			SELECT SIZE(%s) > 0 AS has_entity
			FROM output_df
			) AS temp
			GROUP BY has_entity
			"""%(entity_type)).show()
	###save the results
	print('saving output to '+output_json)
	output_df.write.mode("Overwrite").json(output_json)
	print('outputs saved to '+output_json)
	print('runing time: '+str(time.time()-start_time))
	return output_df

'''
from pami_spark import * 

def sender_name_extract(text, \
	text_entity_context, \
	entities):
	output = []
	outputs = text_normalized2text_entity_comb(\
			text_entity_context['context'], \
			entities = entities,\
			indicator_re = \
			r"i am _name_ \' s _title_ \, _entity_")
	output += [{'entity': e, 'method':'jimi5'}\
		for e in outputs]
	return output

text = u"I am Jim's brother, John."
text_entity_context = {'context': \
	u" _start_ i am jim ' s brother , _entity_ . _end_ ",\
	'enetity':'john'}
entities = {"name":["jim","john"],"title":["brother"]}

sender_name_extract(text, \
	text_entity_context, \
	entities)

sudo rm input.json
sudo vi input.json
i{"text":"I am Jim's brother, John.","text_normalized":" _start_ i am jim ' s brother , john wang . _end_ ","name":["jim","john","john wang"],"title":["brother"]}
{"text":"this is a test","text_normalized":" _start_ this is a test _end_ ","name":[],"title":[]}
{"text":"My name is Wang.","text_normalized":" _start_ my name is wang _end_ ","name":["wang"],"title":[]}

sudo rm context_indicator.csv
sudo vi context_indicator.csv
i"i am _name_ ' s _title_ , _entity_"
my name is _entity_

from pami_spark import *

input_json = 'input.json'
output_json = 'output.json'
target_entity = 'name'
sub_entities = ['name', 'title']
entity_context_name = 'sender_name_context'
context_extract_func = sender_name_extract
context_indicator_csv = 'context_indicator.csv'

text_normalized_entity_context_matching(\
	input_json,\
	output_json,\
	target_entity,\
	entity_context_name,\
	context_extract_func = sender_name_extract,\
	context_indicator_csv = 'context_indicator.csv',\
	sub_entities = ['name', 'title'],\
	sqlContext = None)

cat output.json/*
'''
def text_normalized_entity_context_matching(\
	input_json,\
	output_json,\
	target_entity,\
	entity_context_name,\
	context_extract_func = None,\
	context_indicator_csv = None,\
	sub_entities = [],\
	show_number_of_matched_texts = True,\
	sqlContext = None):
	start_time = time.time()
	if sqlContext is None:
		sqlContext = sqlContext_local
	print('loading data from '+input_json)
	input_df = sqlContext.read.json(input_json)
	print('combining existing entities to one colume')
	##create the entities cloumn for further matching
	sub_entities.append(target_entity)
	sub_entities = list(set(sub_entities))
	input_df = merge_entities(input_df,\
		sub_entities = sub_entities)
	input_df = input_df.cache()
	output_df1 = input_df
	####
	print('converting texts to entity-wise records')
	udf_entity_context_list = udf(\
		lambda input, entities: 
		text_normalized2text_entity_context_list(\
		input,\
		entities,\
		target_entity = target_entity), \
		ArrayType(MapType(StringType(),StringType())))
	##
	output_df1 = output_df1.withColumn(\
		'text_entity_context',\
		udf_entity_context_list('text_normalized',\
		'entities'))
	output_df1 = output_df1.withColumn(\
		'text_entity_context',\
		explode('text_entity_context'))
	##
	output_df1 = output_df1.cache()
	########using the fucntion to match the context
	output_df2 = output_df1
	if context_extract_func is not None:
		print('matching the entity context by fucntion '\
			+str(context_extract_func))		
		print('matching the enetity context by '\
			+str(context_extract_func))
		udf_context_match = udf(\
			lambda text, text_entity_context, \
			entities: context_extract_func(text, \
			text_entity_context, \
			entities), ArrayType(MapType(StringType(),\
			StringType())))
		output_df2 = output_df2.withColumn(\
			'context_func',\
			udf_context_match('text', \
			'text_entity_context',\
			'entities'))
		output_df2 = output_df2.cache()
	##using the context indicator to match the context
	output_df3 = output_df2
	if context_indicator_csv is not None:
		print('matching the context by a context file '\
			+context_indicator_csv)
		print('loading context indicators from '\
			+context_indicator_csv)
		sqlContext.read.format('csv')\
			.load(context_indicator_csv)\
			.withColumnRenamed('_c0', 'indicator')\
			.withColumn('indicator',\
				udf(entity2entity_normalized, StringType())\
				('indicator'))\
			.registerTempTable('entities')
		entities_df = sqlContext.sql(u"""
			SELECT DISTINCT * 
			FROM entities
			WHERE indicator IS NOT NULL
			""")
		entities_df.cache()
		print('loaded '+str(entities_df.count())\
			+' context indicators from '\
			+context_indicator_csv)
		entities_list = [str(row['indicator']) \
			for row in entities_df.collect()]
		output_df3 = output_df3.withColumn(\
			'indicators', \
			udf(lambda input: entities_list,\
			ArrayType(StringType()))('text'))
		output_df3 = output_df3.cache()
		##
		def match_entity_list(text_entity_context, \
			entities, \
			indicators):
			outputs = []
			for indicator in indicators:
				output = text_normalized2text_entity_comb(\
					text_entity_context['context'], 
					indicator_text = indicator,\
					entities = entities)
				if len(output) > 0:
					outputs += [{'entity': e, 'method':\
						'indicator: '+indicator} \
						for e in output]
			return outputs
		##
		udf_func_entit_extract = udf(match_entity_list, \
			ArrayType(MapType(StringType(),StringType())))
		output_df3 = output_df3.drop('entity_indi')
		output_df3 = output_df3.withColumn(\
			'context_indi', \
			udf_func_entit_extract(\
				'text_entity_context',\
				'entities','indicators'))
		output_df3 = output_df3.drop('indicators')
		output_df3 = output_df3.cache()
	###
	print('merging the results of matching by both indicator file and matching function')
	if 'context_func' in output_df3.columns \
		and 'context_indi' not in output_df3.columns:
		output_df = output_df3.withColumnRenamed('context_func',\
			entity_context_name)\
			.drop('entities')
	if 'context_indi' in output_df3.columns \
		and 'context_func' not in output_df3.columns:
		output_df = output_df3.withColumnRenamed('context_indi',\
			entity_context_name)\
			.drop('entities')
	if 'context_indi' in output_df3.columns \
		and 'context_func' in output_df3.columns:
		output_df = output_df3.withColumn(entity_context_name,\
			udf(lambda context_indi, context_func:
			context_func+context_indi,\
			ArrayType(MapType(StringType(),StringType())))\
			('context_indi', 'context_func'))\
			.drop('context_indi').drop('context_func')\
			.drop('entities')
	output_df = output_df.cache()
	###save the results
	print('saving output to '+output_json)
	output_df.write.mode("overwrite").json(output_json)
	print('outputs saved to '+output_json)
	###
	if show_number_of_matched_texts is True:
		output_df = sqlContext.read.json(output_json)
		udf_list_not_empty = udf(\
			lambda input: bool(input), \
			BooleanType())
		output_df.withColumn('has_entity', \
			udf_list_not_empty(entity_context_name))\
			.registerTempTable('output_df')
		sqlContext.sql(u"""
			SELECT has_entity, COUNT(*)
			FROM output_df
			GROUP BY has_entity
			""").show()
	print('runing time: '+str(time.time()-start_time)\
		+' seconds')
	return output_df

'''
sudo rm input.json
sudo vi input.json
i{"text":"jim is my father","text_normalized":" _start_ jim is my father _end_ ","name":["jim"],"role":["father"]}

input_json = 'input.json'
sub_entities = ['name','role']
sqlContext = None
subject_entity = 'name'
object_entity = 'role'
context_extract_func = \
subect_is_senders_object_match
entity_context_name = 'subject_is_senders_object_context_indicator'
output_json = 'output.json'

text_normalized_subject_object_context_matching(\
	input_json,\
	output_json,\
	subject_entity,\
	object_entity,\
	entity_context_name,\
	context_extract_func,\
	sub_entities = sub_entities,\
	sqlContext = None)
'''
def text_normalized_subject_object_context_matching(\
	input_json,\
	output_json,\
	subject_entity,\
	object_entity,\
	entity_context_name,\
	context_extract_func,\
	sub_entities = [],\
	show_number_of_matched_texts = True,\
	sqlContext = None,
	workspace = '.'):
	start_time = time.time()
	if sqlContext is None:
		sqlContext = sqlContext_local
	print('loading data from '+input_json)
	input_df = sqlContext.read.json(input_json)
	print('loaded '+str(input_df.count())+' records from '+input_json)
	print('combining existing entities to one colume')
	##create the entities cloumn for further matching
	sub_entities.append(subject_entity)
	sub_entities.append(object_entity)
	sub_entities = list(set(sub_entities))
	input_df = merge_entities(input_df,\
		sub_entities = sub_entities)
	input_df = input_df.cache()
	output_df1 = input_df
	####
	print('converting texts to subject-object-wise records')
	udf_subject_object_context_list = udf(\
		lambda input, entities: 
		text_normalized2text_subject_object_context_list(\
		input,\
		entities,\
		subject_entity = subject_entity,
		object_entity = object_entity), \
		ArrayType(MapType(StringType(),StringType())))
	##
	output_df1 = output_df1.withColumn(\
		'text_subject_object_context',\
		udf_subject_object_context_list('text_normalized',\
		'entities'))
	output_df1.withColumn(\
		'text_subject_object_context',\
		explode('text_subject_object_context')).write.mode('Overwrite').json('%s/text_subject_object_context.json'%(workspace))
	output_df1 = sqlContext.read.json('%s/text_subject_object_context.json'%(workspace))
	print('detected %d subject-object triplets'%(output_df1.count()))
	########using the fucntion to match the context
	output_df2 = output_df1
	print('matching the entity context by fucntion '\
		+str(context_extract_func))		
	udf_context_match = udf(\
		lambda text, text_subject_object_context, \
		entities: context_extract_func(text, \
		text_subject_object_context, \
		entities), ArrayType(MapType(StringType(),\
		StringType())))
	output_df2 = output_df2.withColumn(\
		entity_context_name,\
		udf_context_match('text', \
		'text_subject_object_context',\
		'entities'))
	output_df = output_df2.cache()
	if show_number_of_matched_texts is True:
		output_df.registerTempTable('output_df')
		sqlContext.sql(u"""
			SELECT has_entity, COUNT(*)
			FROM (
			SELECT SIZE(%s) > 0 AS has_entity
			FROM output_df
			) AS temp
			GROUP BY has_entity
			"""%(entity_context_name)).show()
	###save the results
	print('saving output to '+output_json)
	output_df.write.mode("overwrite").json(output_json)
	print('outputs saved to '+output_json)
	print('runing time: '+str(time.time()-start_time)\
		+' seconds')
	return output_df

'''
sudo rm input.json
sudo vi input.json
i{"text":"My name is Jim","text_normalized":" _start_ my name is jim _end_ ","name":["jim"],"sender_married_indicator":[]}
{"text":"My name is Jane","text_normalized":" _start_ my wife name is jane _end_ ","name":["jane"],"sender_married_indicator":["my wife"]}
{"text":"My name is Jim","text_normalized":" _start_ my name is jim _end_ ","name":["jim"],"sender_married_indicator":[]}
{"text":"My name is Jim","text_normalized":" _start_ my name is jim _end_ ","name":["jim"],"sender_married_indicator":[]}

from pami_spark import * 

prepare_text_dl_input(\
	input_json = 'input.json',\
	output_json = 'output.json',\
	positive_indicator = 'sender_married_indicator',\
	wild_entities = ['name'],\
	negative_sample_number = 3)

cat output.json/* | grep label
'''
def prepare_text_dl_input(\
	input_json,\
	output_json,\
	positive_indicator,\
	wild_entities = [],\
	negative_sample_number = None,\
	sqlContext = None):
	start_time = time.time()
	if sqlContext is None:
		sqlContext = sqlContext_local
	print('loading data from '+input_json)
	input_df = sqlContext.read.json(input_json)
	#decide the labels according to indicators
	input_df = input_df.withColumn(\
		'label',\
		udf(lambda input: 1 if len(input) > 0\
		else 0, \
		IntegerType())(positive_indicator))
	if negative_sample_number is not None:
		print('limiting the negative number to '\
			+str(negative_sample_number))
		input_df.registerTempTable('input_df')
		sqlContext.sql(u"""
			SELECT * FROM input_df
			WHERE label != 0
			""").registerTempTable('positives')
		sqlContext.sql(u"""
			SELECT * FROM input_df
			WHERE label = 0
			LIMIT """+str(negative_sample_number))\
			.registerTempTable('negatives')
		input_df = sqlContext.sql(u"""
			SELECT * FROM positives
			UNION ALL 
			SELECT * FROM negatives
			""")
		input_df = input_df.cache()
	##create the entities cloumn for further matching
	input_df = merge_entities(input_df,\
		sub_entities = wild_entities)
	output_df1 = input_df
	##replace the entities by wild cards
	udf_wildcard = udf(lambda input,\
		entities:\
		text_normalized2text_entity_wild(input, 
		entities = entities,\
		wild_entities = wild_entities), \
		StringType())
	output_df1 = output_df1.withColumn(\
		'word_idx',\
		udf_wildcard('text_normalized',\
		'entities'))
	#split to words
	output_df1 = output_df1.withColumn(\
		'word_idx',\
		udf(text_normalized2words, \
		ArrayType(StringType()))('word_idx'))
	#convert word to index
	output_df1 = output_df1.withColumn(\
		'word_idx',\
		udf(words2word_idx, \
		ArrayType(IntegerType()))('word_idx'))
	output_df1 = output_df1.drop('entities')
	output_df = output_df1.cache()
	###save the results
	print('saving output to '+output_json)
	output_df.write.mode("overwrite").json(output_json)
	print('outputs saved to '+output_json)
	print('running time: '+str(time.time() - start_time)\
		+' seconds')
	return output_df

'''
sudo rm input.json
sudo vi input.json
i{"name":["jim","john","john wang"],"text":"I am Jim's brother, John.","text_normalized":" _start_ i am jim ' s brother , john wang . _end_ ","title":["brother"],"text_entity_context":{"context":" _start_ i am jim ' s brother , _entity_ . _end_ ","entity":"john wang"},"sender_name_context":[{"method":"jimi5","entity":"i am jim ' s brother , _entity_"},{"method":"indicator: i am _name_ ' s _title_ , _entity_","entity":"i am jim ' s brother , _entity_"}]}
{"name":["jim","john","john wang"],"text":"I am Jim's brother, John.","text_normalized":" _start_ i am jim ' s brother , john wang . _end_ ","title":["brother"],"text_entity_context":{"context":" _start_ i am jim ' s brother , _entity_ wang . _end_ ","entity":"john"},"sender_name_context":[{"method":"jimi5","entity":"i am jim ' s brother , _entity_"},{"method":"indicator: i am _name_ ' s _title_ , _entity_","entity":"i am jim ' s brother , _entity_"}]}
{"name":["jim","john","john wang"],"text":"I am Jim's brother, John.","text_normalized":" _start_ i am jim ' s brother , john wang . _end_ ","title":["brother"],"text_entity_context":{"context":" _start_ i am _entity_ ' s brother , john wang . _end_ ","entity":"jim"},"sender_name_context":[]}
{"name":["wang"],"text":"My name is Wang.","text_normalized":" _start_ my name is wang _end_ ","title":[],"text_entity_context":{"context":" _start_ my name is _entity_ _end_ ","entity":"wang"},"sender_name_context":[{"method":"indicator: my name is _entity_","entity":"my name is _entity_"}]}
{"name":["jim","john","john wang"],"text":"I am Jim's brother, John.","text_normalized":" _start_ i am jim ' s brother , john wang . _end_ ","title":["brother"],"text_entity_context":{"context":" _start_ i am _entity_ ' s brother , john wang . _end_ ","entity":"jim"},"sender_name_context":[]}
{"name":["jim","john","john wang"],"text":"I am Jim's brother, John.","text_normalized":" _start_ i am jim ' s brother , john wang . _end_ ","title":["brother"],"text_entity_context":{"context":" _start_ i am _entity_ ' s brother , john wang . _end_ ","entity":"jim"},"sender_name_context":[]}
{"name":["jim","john","john wang"],"text":"I am Jim's brother, John.","text_normalized":" _start_ i am jim ' s brother , john wang . _end_ ","title":["brother"],"text_entity_context":{"context":" _start_ i am _entity_ ' s brother , john wang . _end_ ","entity":"jim"},"sender_name_context":[]}
{"name":["jim","john","john wang"],"text":"I am Jim's brother, John.","text_normalized":" _start_ i am jim ' s brother , john wang . _end_ ","title":["brother"],"text_entity_context":{"context":" _start_ i am _entity_ ' s brother , john wang . _end_ ","entity":"jim"},"sender_name_context":[]}
{"name":["jim","john","john wang"],"text":"I am Jim's brother, John.","text_normalized":" _start_ i am jim ' s brother , john wang . _end_ ","title":["brother"],"text_entity_context":{"context":" _start_ i am _entity_ ' s brother , john wang . _end_ ","entity":"jim"},"sender_name_context":[]}

from pami_spark import * 

prepare_entity_context_dl_input(\
	input_json = 'input.json',\
	output_json = 'output.json',\
	positive_indicator = "sender_name_context",\
	negative_sample_number = 2,\
	context_wild_entities =['name','title'])

cat output.json/* | grep label
'''
def prepare_entity_context_dl_input(\
	input_json,\
	output_json,\
	positive_indicator,\
	context_wild_entities = [],\
	negative_sample_number = None,\
	sqlContext = None):
	start_time = time.time()
	if sqlContext is None:
		sqlContext = sqlContext_local
	##
	print('loading data from '+input_json)
	input_df = sqlContext.read.json(input_json)
	input_df = merge_entities(input_df,\
		sub_entities = context_wild_entities)
	###
	output_df1 = input_df.cache()
	print('labeling the data according to the context indicator')
	output_df1 = output_df1.withColumn(\
		'label',\
		udf(lambda input: 1 if len(input) > 0\
		else 0, \
		IntegerType())(positive_indicator))
	if negative_sample_number is not None:
		print('limiting the negatie number to '\
			+str(negative_sample_number))
		output_df1.registerTempTable('input_df')
		sqlContext.sql(u"""
			SELECT * FROM input_df
			WHERE label != 0
			""").registerTempTable('positives')
		sqlContext.sql(u"""
			SELECT * FROM input_df
			WHERE label = 0
			LIMIT """+str(negative_sample_number))\
			.registerTempTable('negatives')
		output_df1 = sqlContext.sql(u"""
			SELECT * FROM positives
			UNION ALL 
			SELECT * FROM negatives
			""")
		output_df1 = output_df1.cache()
	##
	print('replace the entities in the context by wild cards')
	output_df1 = output_df1.cache()
	udf_context_word_idx = udf(\
		lambda text_entity_context,\
		entities:\
		text_entity_context2context_entity_wild(\
		text_entity_context,\
		entities,\
		conext_wild_entities = context_wild_entities),\
		MapType(StringType(),StringType()))
	output_df1 = output_df1.withColumn(\
		'context_word_idx',\
		udf_context_word_idx('text_entity_context',\
		'entities'))
	##
	output_df2 = output_df1.cache()
	print('converting the context words to word index')
	udf_entity_context2context_words = \
		udf(entity_context2context_words, MapType(\
		StringType(),ArrayType(StringType())))
	output_df2 = output_df2.withColumn(\
		'context_word_idx',\
		udf_entity_context2context_words\
		('context_word_idx'))
	output_df2 = output_df2.withColumn('context_word_idx',\
		udf(context_words2word_idx, MapType(\
		StringType(),ArrayType(IntegerType())))\
		('context_word_idx'))
	##
	output_df2 = output_df2.withColumn('left_word_idx',\
		udf(lambda input: \
		input['left_word_idx'], ArrayType(IntegerType()))\
		('context_word_idx'))
	output_df2 = output_df2.withColumn('right_word_idx',\
		udf(lambda input: \
		input['right_word_idx'], ArrayType(IntegerType()))\
		('context_word_idx'))
	output_df2 = output_df2.withColumn('entity_word_idx',\
		udf(lambda input: \
		input['entity_word_idx'], ArrayType(IntegerType()))\
		('context_word_idx'))
	output_df2 = output_df2.drop('context_word_idx')
	###save the results
	output_df2 = output_df2.drop('entities')
	output_df = output_df2.cache()
	print('saving output to '+output_json)
	output_df.write.mode("overwrite").json(output_json)
	print('outputs saved to '+output_json)
	print('running time: '+str(time.time() - start_time)\
		+' seconds')
	return output_df

'''
sudo rm input.json
sudo vi input.json
i{"name":["jim"],"role":["father"],"text":"jim is my father","text_normalized":" _start_ jim is my father _end_ ","entities":{"name":["jim"],"role":["father"]},"text_subject_object_context":{"context":" _start_ _subject_ is my _object_ _end_ ","subject":"jim","object":"father"},"subject_is_senders_object_context_indicator":[{"method":"jim_subject_is_senders_object_190705_1","entity":"_subject_ is my _object_"}]}

prepare_subject_object_context_dl_input(\
	input_json = '/data/jim/name_is_senders_role.json',\
	output_json = '/data/jim/name_is_senders_role_train.json',\
	positive_indicator = 'name_is_senders_role_indicator',\
	context_wild_entities = ['name','role',\
	'location','title','time'],\
	negative_sample_number = 50000,\
	sqlContext = sqlContext)

sudo rm -r name_is_senders_role_train.json
hadoop fs -get /data/jim/name_is_senders_role_train.json ./
cat name_is_senders_role_train.json/* \
| grep context_word
'''
def prepare_subject_object_context_dl_input(\
	input_json,\
	output_json,\
	positive_indicator,\
	context_wild_entities = [],\
	negative_sample_number = None,\
	sqlContext = None,
	workspace = '.'):
	start_time = time.time()
	if sqlContext is None:
		sqlContext = sqlContext_local
	##
	print('loading data from '+input_json)
	input_df = sqlContext.read.json(input_json)
	input_df = merge_entities(input_df,\
		sub_entities = context_wild_entities)
	###
	output_df1 = input_df.cache()
	print('labeling the data according to the context indicator')
	output_df1.registerTempTable('output_df1')
	sqlContext.sql(u"""
		SELECT *,
		CASE 
			WHEN SIZE(%s) > 0 THEN 1
			ELSE 0
		END AS label
		FROM output_df1
		"""%(positive_indicator)).write.mode('Overwrite').json('%s/label.json'%(workspace))
	output_df1 = sqlContext.read.json('%s/label.json'%(workspace))
	if negative_sample_number is not None:
		print('limiting the negatie number to '\
			+str(negative_sample_number))
		output_df1.registerTempTable('input_df')
		print('saving the traing set')
		sqlContext.sql(u"""
			SELECT * FROM input_df
			WHERE label != 0
			""").write.mode('Overwrite').json('%s/label_positives.json'%(workspace))
		sqlContext.sql(u"""
			SELECT * FROM input_df
			WHERE label = 0
			LIMIT %d"""%(negative_sample_number)).write.mode('Overwrite').json('%s/label_negatives.json'%(workspace))
		output_df1 = sqlContext.read.json('%s/label_*.json'%(workspace))
	##
	print('replace the entities in the context by wild cards')
	output_df2 = output_df1.cache()
	udf_context_word_idx = udf(\
		lambda text_entity_context,\
		entities:\
		text_entity_context2context_entity_wild(\
		text_entity_context,\
		entities,\
		conext_wild_entities = context_wild_entities),\
		MapType(StringType(),StringType()))
	output_df2 = output_df2.withColumn(\
		'subject_object_context_word_idx',\
		udf_context_word_idx(\
		'text_subject_object_context',\
		'entities'))
	##
	output_df3 = output_df2.cache()
	print('converting the context words to word index')
	udf_entity_context2context_words = \
		udf(subject_object_context2context_words, MapType(\
		StringType(),ArrayType(StringType())))
	output_df3 = output_df3.withColumn(\
		'subject_object_context_word_idx',\
		udf_entity_context2context_words\
		('subject_object_context_word_idx'))
	output_df3 = output_df3.withColumn(\
		'subject_object_context_word_idx',\
		udf(context_words2word_idx, MapType(\
		StringType(),ArrayType(IntegerType())))\
		('subject_object_context_word_idx'))
	##
	output_df4 = output_df3.cache()
	output_df4 = output_df4\
		.withColumn('subject_word_idx',\
		udf(lambda input: \
		input['subject_word_idx'], ArrayType(IntegerType()))\
		('subject_object_context_word_idx'))
	output_df4 = output_df4\
		.withColumn('subject_left_idx',\
		udf(lambda input: \
		input['subject_left_idx'], ArrayType(IntegerType()))\
		('subject_object_context_word_idx'))
	output_df4 = output_df4\
		.withColumn('subject_right_idx',\
		udf(lambda input: \
		input['subject_right_idx'], ArrayType(IntegerType()))\
		('subject_object_context_word_idx'))
	##
	output_df4 = output_df4\
		.withColumn('object_word_idx',\
		udf(lambda input: \
		input['object_word_idx'], ArrayType(IntegerType()))\
		('subject_object_context_word_idx'))
	output_df4 = output_df4\
		.withColumn('object_left_idx',\
		udf(lambda input: \
		input['object_left_idx'], ArrayType(IntegerType()))\
		('subject_object_context_word_idx'))
	output_df4 = output_df4\
		.withColumn('object_right_idx',\
		udf(lambda input: \
		input['object_right_idx'], ArrayType(IntegerType()))\
		('subject_object_context_word_idx'))
	##
	output_df4 = output_df4\
		.withColumn('context_word_idx',\
		udf(lambda input: \
		input['context_word_idx'], ArrayType(IntegerType()))\
		('subject_object_context_word_idx'))
	output_df4 = output_df4.drop('subject_object_context_word_idx')
	###save the results
	output_df4 = output_df4.drop('entities')
	output_df = output_df4.cache()
	print('saving output to '+output_json)
	output_df.write.mode("overwrite").json(output_json)
	print('outputs saved to '+output_json)
	print('running time: '+str(time.time() - start_time)\
		+' seconds')
	return output_df

'''
sudo rm input.csv
sudo vi input.csv
iwang
jim
123
_number_
"dubai, mall"
777

sudo rm add.csv
sudo vi add.csv
i&&&
yan

sudo rm delete.csv
sudo vi delete.csv
ijim
777

from pami_spark import *

entity_csv_update(entity_file_input = 'input.csv',
	entity_file_to_delete = 'delete.csv',
	entity_file_to_add = 'add.csv',
	output_file_csv = 'output.csv')
'''
def entity_csv_update(entity_file_input,
	entity_file_to_delete = None,
	entity_file_to_add = None,
	output_file_csv = None,
	sqlContext = None):
	start_time = time.time()
	if sqlContext is None:
		sqlContext = sqlContext_local
	print('loading indicators from '+entity_file_input)
	sqlContext.read.format("csv")\
		.option("header", "false")\
		.schema(StructType([\
		StructField("indicator", StringType(), True)]))\
		.load(entity_file_input)\
		.withColumn('indicator',\
			udf(entity2entity_normalized, StringType())\
			('indicator'))\
		.registerTempTable('entities')
	entities_df = sqlContext.sql(u"""
		SELECT DISTINCT * 
		FROM entities
		WHERE indicator IS NOT NULL
		""")
	output_df = entities_df.cache()
	output_df.registerTempTable('output_df')
	###
	if entity_file_to_delete is not None:
		print('loading indicators from '+entity_file_to_delete)
		sqlContext.read.format("csv")\
			.option("header", "false")\
			.schema(StructType([\
			StructField("indicator", StringType(), True)]))\
			.load(entity_file_to_delete)\
			.withColumn('indicator',\
				udf(entity2entity_normalized, StringType())\
				('indicator'))\
			.registerTempTable('entities')
		entities_df_delete = sqlContext.sql(u"""
			SELECT DISTINCT * 
			FROM entities
			WHERE indicator IS NOT NULL
			""")
		entities_df_delete = entities_df_delete.cache()
		entities_df_delete.registerTempTable('entities_df_delete')
		print('deleting entities in '+entity_file_to_delete)
		output_df = sqlContext.sql(u"""
			SELECT output_df.*
			FROM output_df
			LEFT JOIN entities_df_delete
			ON entities_df_delete.indicator = 
			output_df.indicator
			WHERE entities_df_delete.indicator IS NULL
			""")
		output_df = output_df.cache()
		output_df.registerTempTable('output_df')
	if entity_file_to_add is not None:
		##
		print('loading indicators from '+entity_file_to_add)
		sqlContext.read.format("csv")\
			.option("header", "false")\
			.schema(StructType([\
			StructField("indicator", StringType(), True)]))\
			.load(entity_file_to_add)\
			.withColumn('indicator',\
				udf(entity2entity_normalized, StringType())\
				('indicator'))\
			.registerTempTable('entities')
		entities_df_add = sqlContext.sql(u"""
			SELECT DISTINCT * 
			FROM entities
			WHERE indicator IS NOT NULL
			""")
		entities_df_add = entities_df_add.cache()
		entities_df_add.registerTempTable('entities_df_add')
		print('adding entities in '\
			+entity_file_to_add)
		output_df = sqlContext.sql(u"""
			SELECT DISTINCT * FROM (
			SELECT * FROM output_df
			UNION ALL
			SELECT * FROM entities_df_add
			) AS temp
			""")	
		output_df = output_df.cache()
		output_df.registerTempTable('output_df')
	print('saving output to '+output_file_csv)
	output_df.write.mode('Overwrite').format('csv').save(output_file_csv)
	print('outputs saved to '+output_file_csv)
	print('running time: '+str(time.time() - start_time)\
		+' seconds')
	return output_df


def title_csv2title_relation_df(input_file,\
	relation_type,\
	relation_reverse_type = None,\
	sqlContext = None):
	if sqlContext is None:
		sqlContext = sqlContext_local
	if input_file.split('.')[-1] == 'csv':
		customSchema = StructType([\
			StructField("title", StringType(), True)])
		sqlContext.read.format("csv").option("header", "false")\
			.schema(customSchema)\
			.load(input_file)\
			.withColumn('title',\
			udf(entity2entity_normalized,\
			StringType())\
			('title'))\
			.registerTempTable('temp')
	if input_file.split('.')[-1] == 'json':
		sqlContext.read.json(input_file)\
			.registerTempTable('temp')
	if relation_reverse_type is not None:
		return sqlContext.sql(u"""
			SELECT DISTINCT *, 
			'"""+relation_type+u"""' AS relation,
			'"""+relation_reverse_type+u"""' AS relation_reverse
			FROM temp
			""")
	else:
		return sqlContext.sql(u"""
			SELECT DISTINCT *, 
			'"""+relation_type+u"""' AS relation,
			NULL AS relation_reverse
			FROM temp
			""")


'''
sqlContext.createDataFrame([
("My name is Jim", ["jim"]),
("I am jim", ["jim"]),
("Jim is here", ["jim"]),
("Hi Jim", ["jim"]),
("My name is Jim and you?", ["jim"]),
("I am a NLP scientist",[]),
("I work as an engineer, but also a researcher",[]),
("Hi, How are you?",[]),
("Hi, Jim, How are you?",[]),
("Jim is here",[]),
("Jim is not here, but jim wang will com",[]),
], ["text", "indicator"]).write.mode('Overwrite').json('/raid/jim/input.json')

input_json = '/raid/jim/input.json'
output_json = '/raid/jim/output.json'
indicator_column_name = 'indicator'

os.system(u"""
	rm -r /raid/jim/temp6780
	mkdir /raid/jim/temp6780
	""")

match_negatives_to_positives_from_json(
	input_json = '/raid/jim/input.json',
	output_json = '/raid/jim/output.json',
	indicator_column_name = 'indicator',
	sqlContext = sqlContext,
	workspace = '/raid/jim/temp6780',
	returned_negative_number = 1000).show()
'''

def match_negatives_to_positives_from_json(
	input_json,
	output_json,
	indicator_column_name,
	sqlContext,
	workspace = '.',
	returned_negative_number = 1000,
	negative_number_for_matching = None):
	print('loading data from %s'%(input_json))
	input_df = sqlContext.read.json('%s/*'%(input_json))
	if negative_number_for_matching is not None:
		print('limiting negative text number')
		input_df.registerTempTable('input_df')
		sqlContext.sql(u"""
			SELECT * FROM input_df
			WHERE SIZE(%s) > 0
			UNION ALL 
			SELECT * FROM input_df
			WHERE SIZE(%s) = 0
			LIMIT %d
			"""%(indicator_column_name, indicator_column_name, negative_number_for_matching)).write.mode('Overwrite').json('%s/input.json/*'%(workspace))
		input_json = '%s/input.json/*'%(workspace)
	if 'text_normalized' not in input_df.columns:
		text_json2text_normalized_json(
		input_json = input_json,
		output_json = '%s/input_text_normalized.json'%(workspace),
		sqlContext = sqlContext)
		input_json = '%s/input_text_normalized.json'%(workspace)
	print('spliting to words')
	sqlContext.read.json('%s/*'%(input_json)).withColumn('words', udf(text_normalized2words, ArrayType(StringType()))('text_normalized')).write.mode('Overwrite').json('%s/input_words.json'%(workspace))
	print('exploding words')
	sqlContext.read.json('%s/input_words.json/*'%(workspace)).registerTempTable('input_words')
	sqlContext.sql(u"""
		SELECT DISTINCT * FROM (
		SELECT *, EXPLODE(words) AS word
		FROM input_words
		) AS temp
		WHERE word NOT IN ('_start_', '_end_','_entity_','_subject_','_object_')
		""").write.mode('Overwrite').json('%s/input_word.json'%(workspace))
	sqlContext.read.json('%s/input_word.json/*'%(workspace)).registerTempTable('input_word')
	print('saving positives')
	sqlContext.sql(u"""
		SELECT * FROM input_word
		WHERE SIZE(%s) > 0
		"""%(indicator_column_name)).write.mode('Overwrite').json('%s/input_word_positive.json'%(workspace))
	print('saving negatives')
	sqlContext.sql(u"""
		SELECT * FROM input_word
		WHERE SIZE(%s) = 0
		"""%(indicator_column_name)).write.mode('Overwrite').json('%s/input_word_negative.json'%(workspace))
	print('matching candidates')
	sqlContext.read.json('%s/input_word_positive.json/*'%(workspace)).repartition('word').registerTempTable('input_word_positive')
	sqlContext.read.json('%s/input_word_negative.json/*'%(workspace)).repartition('word').registerTempTable('input_word_negative')
	sqlContext.sql(u"""
		SELECT DISTINCT input_word_negative.text_normalized AS negative_text,
		input_word_negative.words AS negative_text_words,
		input_word_positive.text_normalized AS positive_text,
		input_word_positive.words AS positive_text_words
		FROM input_word_negative
		JOIN input_word_positive
		ON input_word_positive.word
		= input_word_negative.word
		""").write.mode('Overwrite').json('%s/matched_text_pairs.json'%(workspace))
	print('matching similiary scores')
	matched_text_pairs = sqlContext.read.json('%s/matched_text_pairs.json/*'%(workspace))
	matched_text_pairs.withColumn('matching_score', udf(overlapping_words_rate, FloatType())('positive_text_words', 'negative_text_words')).write.mode('Overwrite').json('%s/matching_score.json'%(workspace))
	sqlContext.read.json('%s/matching_score.json'%(workspace)).registerTempTable('matching_score')
	print('saving results to %s'%(output_json))
	'''
	output_df = sqlContext.sql(u"""
		SELECT * 
		FROM (
			SELECT negative_text AS text_normalized, positive_text, matching_score,
			ROW_NUMBER() OVER (PARTITION BY negative_text ORDER BY matching_score DESC) AS rank
			FROM matching_score
		) AS temp 
		WHERE rank = 1
		ORDER BY matching_score DESC
		LIMIT %d
		"""%(returned_negative_number))
	'''
	output_df = sqlContext.sql(u"""
		SELECT * 
		FROM (
			SELECT negative_text AS text_normalized, MEAN(matching_score) AS matching_score
			FROM matching_score
			GROUP BY negative_text
		) AS temp 
		ORDER BY matching_score DESC
		LIMIT %d
		"""%(returned_negative_number))
	output_df.write.mode('Overwrite').json(output_json)
	return output_df


'''
os.system(u"""
	rm -r /raid/jim/temp6780
	mkdir /raid/jim/temp6780
	""")

sqlContext.createDataFrame([
("My name is Jim", "45678"),
("I am jim", "45678"),
("Jim is here", "45678"),
("Hi Jim", "45678"),
("My name is Jim and you?", "45678"),
], ["text", "phone"]).write.mode('Overwrite').json('/raid/jim/positives.json')

sqlContext.createDataFrame([
("I am a NLP scientist", "45678"),
("I work as an engineer, but also a researcher", "45678"),
("Hi, How are you?", "45678"),
("Hi, Jim, How are you?", "45678"),
("Jim is here", "45678"),
("Jim is not here, but jim wang will com", "45678"),
], ["text", "phone"]).write.mode('Overwrite').json('/raid/jim/database.json')

prepare_text_for_word_indexing(
	input_json = '/raid/jim/positives.json',
	sqlContext = sqlContext,
	output_word_weight_json = '/raid/jim/positive_word_weight.json',
	word_counting_unit = 'text',
	workspace = '/raid/jim/temp6780')

prepare_text_for_word_indexing(
	input_json = '/raid/jim/database.json',
	sqlContext = sqlContext,
	output_text_word_json = '/raid/jim/text_word_weight.json',
	word_counting_unit = 'text',
	workspace = '/raid/jim/temp6780').show(100)
'''
def prepare_text_for_word_indexing(
	input_json,
	sqlContext,
	output_word_weight_json = None,
	output_text_word_json = None,
	word_counting_unit = 'text',
	workspace = '.'):
	print('loading data from %s'%(input_json))
	input_df = sqlContext.read.json('%s/*'%(input_json))
	if 'text_normalized' not in input_df.columns:
		text_json2text_normalized_json(
		input_json = input_json,
		output_json = '%s/input_text_normalized.json'%(workspace),
		sqlContext = sqlContext)
		input_json = '%s/input_text_normalized.json'%(workspace)
	print('spliting to words')
	sqlContext.read.json('%s/*'%(input_json)).withColumn('words', udf(text_normalized2words, ArrayType(StringType()))('text_normalized')).write.mode('Overwrite').json('%s/input_words.json'%(workspace))
	print('exploding words')
	sqlContext.read.json('%s/input_words.json/*'%(workspace)).registerTempTable('input_words')
	sqlContext.sql(u"""
		SELECT * FROM (
		SELECT *, 
		SIZE(words) AS text_word_number, 
		EXPLODE(words) AS word
		FROM input_words
		) AS temp
		WHERE word NOT IN ('_start_', '_end_','_entity_','_subject_','_object_')
		""").write.mode('Overwrite').json('%s/input_word.json'%(workspace))
	sqlContext.read.json('%s/input_word.json/*'%(workspace)).registerTempTable('input_word')
	if output_word_weight_json is not None:
		print('counting the weights of words')
		output_df = sqlContext.sql(u"""
			SELECT word, COUNT(DISTINCT %s) AS word_weight
			FROM input_word
			GROUP BY word
			"""%(word_counting_unit))
		output_df.write.mode('Overwrite').json(output_word_weight_json)
	if output_text_word_json is not None:
		print('counting the weights of words in each text')
		output_df = sqlContext.sql(u"""
			SELECT %s, text_word_number, word, COUNT(*) AS word_weight
			FROM input_word
			GROUP BY %s, text_word_number, word
			"""%(word_counting_unit, word_counting_unit))
		output_df.write.mode('Overwrite').json(output_text_word_json)
	return output_df

'''
os.system(u"""
	rm -r /raid/jim/temp6780
	mkdir /raid/jim/temp6780
	""")
match_positive_words_to_text_json(
	input_word_json = '/raid/jim/positive_word_weight.json',
	input_text_json = '/raid/jim/text_word_weight.json',
	output_json = '/raid/jim/recommended.json',
	sqlContext = sqlContext,
	recommended_text_num = 3,
	word_counting_unit = 'text',
	workspace = '/raid/jim/temp6780').show()
'''

def match_positive_words_to_text_json(
	input_word_json,
	input_text_json,
	output_json,
	sqlContext,
	recommended_text_num = 1000,
	word_counting_unit = 'text',
	workspace = '.'):
	print('loading words from %s'%(input_word_json))
	sqlContext.read.json('%s/*'%(input_word_json)).repartition('word').registerTempTable('input_word')
	print('laoding text from %s'%(input_text_json))
	sqlContext.read.json('%s/*'%(input_text_json)).repartition('word').registerTempTable('input_text')
	print('matching by words')
	sqlContext.sql(u"""
		SELECT input_text.*,
		input_word.word_weight AS word_weight_positive,
		input_word.word_weight*input_text.word_weight AS word_weight_join
		FROM input_text
		JOIN input_word
		ON input_word.word
		= input_text.word
		""").write.mode('Overwrite').json('%s/word_weight_join.json'%(workspace))
	print('calculating the text ranking scores')
	sqlContext.read.json('%s/word_weight_join.json/*'%(workspace)).registerTempTable('word_weight_join')
	sqlContext.sql(u"""
		SELECT %s,
		COUNT(text_word_number)/text_word_number AS ranking_score, 
		SUM(word_weight_join)/text_word_number AS ranking_score1,
		COLLECT_LIST(word)
		FROM word_weight_join
		GROUP BY %s, text_word_number
		"""%(word_counting_unit, word_counting_unit)).write.mode('Overwrite').json('%s/ranking_score.json'%(workspace))
	print('saving results to %s'%(output_json))
	sqlContext.read.json('%s/ranking_score.json/*'%(workspace)).registerTempTable('ranking_score')
	output_df = sqlContext.sql(u"""
		SELECT * FROM ranking_score
		ORDER BY ranking_score DESC
		LIMIT %d
		"""%(recommended_text_num))
	output_df.write.mode('Overwrite').json(output_json)
	return output_df

##################pami_spark.py##################