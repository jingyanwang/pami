#############pami_re.py###########
'''
# -*- coding: utf-8 -*-
'''

import re
from hashlib import md5

num_max_text_len = 50
num_max_entity_len = 10
num_max_context_len = 10
num_word_max = 200000

re_email = re.compile(("([A-z0-9!#$%&*+\/=?^_`{|}~-]+(?:\.[A-z0-9!#$%&'*+\/=?^_`"
	"{|}~-]+)*(@)(?:[A-z0-9](?:[A-z0-9-]*[A-z0-9])?(\.|"
	"\sdot\s))+[A-z0-9](?:[A-z0-9-]*[A-z0-9])?)"))

re_url = re.compile(r'(http|ftp|https):\/\/[\w-]+(\.[\w-]+)+([\w.,@?^=%&amp;:\/~+#-]*[\w@?^=%&amp;\/~+#-])?')

#\u0660-\u0669\u06F0-\u06F9 is arabic numbers
#\d is english numbers
#\u060C-\u061B\u066A-\u066C is the data/number sign of arabic
re_number = re.compile(u'[\+\-\$\%]*'\
	+u'[\d\u0660-\u0669\u06F0-\u06F9]+'\
	+u'(([\.\,\:\-\\\/\u060C-\u061B\u066A-\u066C]'\
	+u'[\d\u0660-\u0669\u06F0-\u06F9]+)+)?'\
	+u'[\+\-\$\%]*')

#\u0021-\u002f\u003a-\u0040\u005b-\u0060\u007b-\u007e is enelighs puntutations
#\u060C-\u060F\u061F\u066D\u06DD\u06DE\u06E9 is arabic puntuations
re_puntuation = re.compile(u'[\u0021-\u002f\u003a-\u0040\u005b-\u0060\u007b-\u007e'\
	+u'\u060C-\u060F\u061F\u066D\u06DD\u06DE\u06E9]+')

'''
http://ascii-table.com/ascii.php
https://en.wikipedia.org/wiki/Latin_script_in_Unicode

\u0020-\u007E is the assica  
\u0600-\u06FF is the arabic unicode
'''
re_letter_remove = re.compile(u'[^\u0020-\u007E\u0600-\u06FF\u0100-\u017F\u0180-\u024F\u1E00-\u1EFF\u00C0-\u00FF]')

'''
from pami_re import * 

input = u"""
\u0000\u0000\u0000This is a \u4e2d\u6587text with %23.125+ and, you are.good xxx@gmail.com and https://www.dropbox.com/h
hahah  https://www.br22anah.com/unicode-converter=xrrxx@gmail.com
your email:xgw2g@galgx.com.... 78/88 78/88:55 
_url_
"""

text2url_email_number_puntuation(u"\u004d\u1ef9 \u0110\u1ee9\u0063")

input = u"""
\u064a\u0645\u0643\u0646\u0643 \u0623\u0646 \u062a\u0623\u062a\u064a \u0627\u0644\u064a\u0648\u0645 \u0031\u0030\u003a\u0030\u0030\u061f \u0628\u0631\u064a\u062f\u064a \u0627\u0644\u0625\u0644\u0643\u062a\u0631\u0648\u0646\u064a \u0647\u0648 \u0078\u0067\u0077\u0065\u0040\u0067\u0061\u006d\u0063\u002e\u0063\u006f\u006d \u0633\u0623\u062f\u0641\u0639\u0660\u0661\u0663\u060c\u0660\u0661 \u062f\u0631\u0647\u0645\u0021
"""

entities = text2url_email_number_puntuation(input)

for e in entities['puntuation']:
	print e
'''

'''
s1 = {1,2,3}
s2 = {2,3}
s3 = {3,4}
setlist = [s1,s2,s3]

res = set.intersection(*setlist)
'''

def overlapping_words_rate(input1, input2):
	text_len = len(set(input1)) if len(set(input1)) >= len(set(input2)) else len(set(input2))
	return float(len(set(input1) & set(input2)))/float(text_len)

'''
overlapping_words_rate(['a', 'b'], ['b', 'c', 'd'])
'''

def text2url_email_number_puntuation(input):
	try:
		#remove the non-english letters
		input = re.sub(re_letter_remove,' ', input)
		text_normalized = input
		###extract the url and replace them by 0000url0000
		url_list = [e.group() for e in re.finditer(re_url, text_normalized)]
		for e in url_list:
			text_normalized = re.sub(re.escape(e), u' \u0000url\u0000 ', text_normalized)
		###extract the email and replace them by 0000email0000
		email_list = [e.group() for e in re.finditer(re_email, input)]
		for e in email_list:
			text_normalized = re.sub(re.escape(e), u' \u0000email\u0000 ', text_normalized)
		###extract the number and replace them by 0000number0000
		number_list = [e.group() for e in re.finditer(re_number, text_normalized)]
		number_list1 = number_list[:]
		number_list1.sort(key=len, reverse=True)
		for e in number_list1:
			text_normalized = re.sub(re.escape(e), u' \u0000number\u0000 ', text_normalized)
		###extract the puntuations and replace by the \u0000puntuation\u0000
		puntuation_list = [e.group() for e in re.finditer(re_puntuation, text_normalized)]
		puntuation_list1 = puntuation_list[:]
		puntuation_list1.sort(key=len, reverse=True)
		for e in puntuation_list1:
			text_normalized = re.sub(re.escape(e), u' \u0000puntuation\u0000 ', text_normalized)
		#
		text_normalized = ' '+re.sub('\s+', ' ', \
			text_normalized).lower().strip()+' '
		#recover the url, email, number and puntuation
		text_normalized = text_normalized.split(u'\u0000url\u0000')[0] + \
			''.join([e+part_text for e, part_text in 
			zip(url_list, text_normalized.split(u'\u0000url\u0000')[1:])])
		text_normalized = text_normalized.split(u'\u0000email\u0000')[0] + \
			''.join([e+part_text for e, part_text in 
			zip(email_list, text_normalized.split(u'\u0000email\u0000')[1:])])
		text_normalized = text_normalized.split(u'\u0000number\u0000')[0] + \
			''.join([e+part_text for e, part_text in 
			zip(number_list, text_normalized.split(u'\u0000number\u0000')[1:])])
		text_normalized = text_normalized.split(u'\u0000puntuation\u0000')[0] + \
			''.join([e+part_text for e, part_text in 
			zip(puntuation_list, text_normalized.split(u'\u0000puntuation\u0000')[1:])])
		text_normalized = text_normalized.strip()
		return {'text_normalized':text_normalized,\
			'url':url_list,\
			'email':email_list,\
			'number':number_list,\
			'puntuation':puntuation_list}
	except:
		return None

'''
print(text2url_email_number_puntuation(u"\u00d6\u0073\u006b\u00fc\u002c \u0141\u0119\u0073\u006b\u002c \u0053\u004b"))
'''

'''
input = u"""
this is a sample text with 123.778
this is a [text] and [[[]]]
"""

text2text_normalized(input)
'''
def text2text_normalized(input):
	try:
		output = text2url_email_number_puntuation(input)
		output['text_normalized'] = ' _start_ '\
			+output['text_normalized']\
			+' _end_ '
		return output
	except:
		return None

'''
input = u" this is jim wang "
entity_list = ['jim','wang']
text_normalized2entities_from_lookup(input,\
	entity_list)
'''
def text_normalized2entities_from_lookup(
	text_normalized,\
	entity_list,\
	entities = None):
	words = set(text_normalized.strip().split(' '))
	output = []
	for e in entity_list:
		entity_words = set(e.strip().split(' '))
		if bool(words & entity_words):
			o1 = text_normalized2text_entity_comb(\
				input = text_normalized, 
				entities = entities,\
				indicator_text = e)
			output += o1
	return output

'''
input = u"""
Group 42
"""

input = u'^^&& SSS __ _number_ _number_ of _location_ _name_ .178 sg@ga.com '

entity2entity_normalized(input)

entity2entity_normalized(u'this is a [text _name_ ] and [[[]]]')
'''
def entity2entity_normalized(input):
	try:
		input = re.sub(re_letter_remove,' ', input)
		input = re.sub(r'_\s+_', '_  _', input)
		input = ' '+input+' '
		wild_entities = [e.group() for e in \
			re.finditer(r' _[a-z]+_ ', input)]
		input = re.sub(r' _[a-z]+_ ', u' \u0000 ', input)
		input_parts = input.split(u' \u0000 ')
		input_parts = [text2url_email_number_puntuation(part)\
			['text_normalized']
			for part in input_parts]
		output = input_parts[0]\
			+''.join([w+t for w, t \
			in zip(wild_entities, \
			input_parts[1:])])
		output = output.strip()
		return output
	except:
		return None

'''
input = u" _start_ this is from group 42 and group 778 , or dubai dubai mall . ibm is also good _end_ "

text_normalized2text_entity_comb(input, 
	entities = {'number':['42', '778'], \
	'location':[{'entity':'dubai','method':'jim1'}],\
	'orgnizationtype':['mall']},\
	indicator_re = \
	'(_location_ )+_orgnizationtype_')

text_normalized2text_entity_comb(input = u" new york university ", 
	indicator_text = 'new york university')

text_normalized2text_entity_comb(input = u" new york university ", 
	indicator_text = 'new york')

text_normalized2text_entity_comb(' _start_ this is _end_ ', 
	entities = None,\
	indicator_text = 'this is')

text_normalized2text_entity_comb(' _start_ leaving abu dhabi i will leave _end_ ', 
	entities = None,\
	indicator_re = '(_start_ (leaving|will leave)|i will leave)', 
	indicator_text = '_start_ leaving')

text_normalized = u' _start_ i am in abu dhabi uae clinic and work in dubai mall _end_ '
entities = {'location': ['abu dhabi uae', 'habi', 'dhabi', 'dubai'],\
	'orgnizationtype':['mall', 'clinic']}
text_normalized2text_entity_comb(\
	text_normalized, 
	entities = entities,
	indicator_re = r'_location_ _orgnizationtype_')

text_normalized2text_entity_comb(\
	' i am at [ building 8 78 and 78 ]', 
	entities = {'number':['8', '78']},\
	indicator_re = r'building _number_ _number_')

text_normalized2text_entity_comb(\
	' [ build build ] ', 
	entities = {'place':['build']},\
	indicator_re = r'_place_ _place_')

text_normalized2text_entity_comb(\
	u" this is [ jim wang ] and jim smith wang xx cc jim haha ",\
	entities = {'name':\
	['jim smith wang xx cc jim haha', 'smith wang xx', 'wang', 'jim']},\
	indicator_re = '_name_')

from pami_re import * 

text_normalized2text_entity_comb(\
	u" this is [ jim wang ] and jim smith wang xx cc jim haha xx wang ",\
	entities = {'name':\
	['jim smith wang xx cc jim haha', 'smith wang xx', 'wang', 'jim'],\
	'title':['wang']},\
	indicator_re = '(_name_|_title_)')
'''

re_space_entity = re.compile(r'[^a-z\_]+')
re_entity_wile = re.compile(r'(?P<entity_name> _[a-z]+_ )')
re_entity_strip = re.compile(r'^\s+_|_\s+$')
re_entity_wildcard = re.compile(r'_[^_ ]+_')

def text_normalized2text_entity_comb(\
	input, 
	entities = None,\
	indicator_re = None, 
	indicator_text = None):
	try:
		'''
		in the most simple case, when the indicator_text is simple text without wild card of entities
		just find if it is in the text
		'''
		if indicator_text is not None:
			if re.search(re_entity_wildcard, indicator_text) is None:
				if ' '+indicator_text+' ' in input:
					return [indicator_text]
				else:
					return []
		'''
		if the indicator_re does not contain any wildcard ,then it is only a simple re matching problem
		'''
		if indicator_re is not None:
			if re.search(re_entity_wildcard, indicator_re) is None:
				return [re.search(' '+indicator_re+' ', input).group().strip()]
		'''
		then consider the complex case when wild card entity is included
		'''
		if entities is None:
			entities = {}
		#find the sub entity names of wild card
		temp = indicator_re if indicator_re is not None else indicator_text
		temp = ' '+re.sub(re_space_entity, '  ', temp)+' '
		wild_entities = [re.sub(re_entity_strip, '', e.group('entity_name')) for e in re.finditer(re_entity_wile, temp)]
		wild_entities = list(set(wild_entities))
		#replace the sub entities by wildcards
		matched_entities = {}
		for entity_name in wild_entities:
			if entity_name in entities:
				if len(entities[entity_name]) > 0:
					entity_list = entities[entity_name]
					#if the input entity list is a dicationary
					#take only the entity text
					if isinstance(entity_list[0],dict):
						entity_list = [e['entity'] for e in entity_list]
					entity_list = list(set(entity_list))
					entity_list.sort(key = len, reverse = True)
					for w in entity_list:
						#the re.sub cannot do overlapping sub
						#so we do it many times until nothing to replace
						finish_matching = False
						while not finish_matching:
							#whenever an entity is matched, 
							#we replace the spaces inside the entity by 0002
							#so that it will not be matched again inside, 
							#by a smaller entity
							input1 = re.sub(r' '+re.escape(w)+r' ', \
								u' \u0000'+re.sub(' ',u'\u0002',w)+u'\u0001 ', \
								input)
							if input1 == input:
								finish_matching = True
							input = input1
					input = re.sub(u'\u0002', ' ', input) 
					##extract the matched entities and save them
					matched_entities[entity_name] = [\
						re.sub(u"^\u0000|\u0001$", \
							'', v.group()).strip()\
						for v \
						in re.finditer(u"\u0000[^\u0000-\u0001]+\u0001", input)]
					##for the matched entities, replace by the wild cards
					input = re.sub(u"\u0000[^\u0000-\u0001]+\u0001",\
						u'_'+entity_name+u'_', input)
					input = re.sub('\s+', ' ', input)
		#match to the indicator
		try:
			pattern_ind = r' '+indicator_re+r' ' \
				if indicator_re is not None \
				else r' '+re.escape(indicator_text)+r' '
			outputs = [output.group() for output \
				in re.finditer(pattern_ind,\
				input)]
			outputs = list(set(outputs))
			outputs.sort(key = len, reverse = True)
			#surround the matched context by 0000 and 0001
			for output in outputs:
				'''
				input = re.sub(re.escape(output), \
					u' \u0000'+output.strip()+u'\u0001 ',\
					input)
				'''
				finish_matching = False				
				while not finish_matching:
					input1 = re.sub(re.escape(output), \
						u' \u0000'+re.sub(' ',u'\u0002',output.strip())+u'\u0001 ', \
						input)
					if input1 == input:
						finish_matching = True
					input = input1
			input = re.sub(u'\u0002', ' ', input) 
		except Exception as e:
			#print(e)
			return []
		#recover the sub entities
		output = input
		for entity_name in matched_entities:
			part_text = output.split('_'+entity_name+'_')
			output = part_text[0] + \
				''.join([e1+t1 for e1, t1 in \
				zip(matched_entities[entity_name], part_text[1:])])
		#extract the entities
		output_entities = [re.sub(u"^\s+\u0000|\u0001\s+$", \
			'', e.group()) for e in \
			re.finditer(u' \u0000[^\u0000-\u0001]+\u0001 ', \
			output)]
		#
		return output_entities
	except Exception as e:
		#print(e)
		return []

'''
print(text_normalized2text_entity_comb(\
	input = u" abu dhabi mall at abu dhabi ", 
	entities = {'node':["abu dhabi", "abu dhabi mall", "dubai mall"]},\
	indicator_re = r'_node_'))
'''

'''
o = text_normalized2text_entity_comb(\
	u" hi my name is jim wang ",\
	entities = {'name':['jim smith wang xx cc jim haha', 'smith wang xx', 'wang', 'jim'],\
	'title':['wang']},\
	indicator_text = '_name_ _name_')

print(o)
'''

'''
input = u' _start_ this is jim from abu dhabi , i live in dubai hi sam wang _start_ '

entities = {\
	'name':['jim', 'sam', 'wang', 'sam wang'],\
	'location':[{'entity':'dubai',\
	'method':'jim1'},\
	{'entity':'abu dhabi',\
	'method':'jim1'}]}

wild_entities = ['name', 'location']

text_normalized2text_entity_wild(input, 
	entities = entities,\
	wild_entities = wild_entities)

text_normalized2text_entity_wild(\
	u" this is jim jim wang ",\
	entities = {'name':['jim', 'wang', 'jim wang']},\
	wild_entities = ['name'])

text_normalized2text_entity_wild(\
	u" this is jim jim wang from dubai ",\
	entities = {'name':['jim'], 'location':['dubai']},\
	wild_entities = ['name', 'location'])

text_normalized2text_entity_wild(\
	u" this is jim jim wang from dubai ",\
	entities = {'name':None, 'location':['dubai']},\
	wild_entities = ['name', 'location'])

'''
def text_normalized2text_entity_wild(
	input, 
	entities = None,
	wild_entities = None,
	):
	#replace the sub entities by wildcards
	matched_entities = {}
	for entity_name in wild_entities:
		if entity_name in entities:
			if entities[entity_name] is None:
				#print(entity_name)
				pass
			else:
				if len(entities[entity_name]) > 0:
					entity_list = entities[entity_name]
					if isinstance(entity_list[0],dict):
						entity_list = [e['entity'] for e in entity_list]
					entity_list = list(set(entity_list))
					entity_list.sort(key = len, reverse = True)
					###
					for w in entity_list:
						#for each entity, replace it until cannot find any more
						finished_replacing = False
						while finished_replacing is not True:
							input1 = re.sub(r' '+re.escape(w)+r' ', \
								u' _'+entity_name+u'_ ', \
								input)
							if input1 == input:
								finished_replacing = True
							input = input1
					input = re.sub(r'\s+', ' ', input)
	return input

'''
input = u" _start_ hi jim i am jim wang ' s brother , john . _end_ "
entities = {"name":["jim","john","jim wang"],"title":["brother"]}
target_entity = 'name'
text_normalized2text_entity_context_list(\
	input,\
	entities,\
	target_entity)
'''
def text_normalized2text_entity_context_list(\
	input,\
	entities,\
	target_entity):	
	##
	if target_entity not in entities:
		return []
	if len(entities[target_entity]) == 0:
		return []
	###
	entity_list = entities[target_entity]
	if isinstance(entity_list[0],dict):
		entity_list = [e['entity'] for e in entity_list]
	##
	entity_list = list(set(entity_list))
	entity_list.sort(key = len, reverse = True)
	###
	entity_context_list = []
	for e in entity_list:
		c = {'context': re.sub(re.escape(' '+e+' '), \
			' _entity_ ', input),\
			'entity': e}		
		entity_context_list.append(c)
	return entity_context_list

'''
from pami_re import *
input = ' _start_ jim is my father and yan is my mother , we live in abu dhabi _end_ '
entities = {'name':['jim','yan'], 'title':['father','mother'],'location':['abu dhabi']}
subject_entity = 'name'
object_entity = 'title'

text_normalized2text_subject_object_context_list(\
	input,\
	entities,\
	subject_entity,\
	object_entity)
'''
def text_normalized2text_subject_object_context_list(\
	input,\
	entities,\
	subject_entity,\
	object_entity):
	##
	if subject_entity not in entities\
		or object_entity not in entities:
		return []
	##
	if len(entities[subject_entity]) == 0\
		or len(entities[object_entity]) == 0:
		return []
	###
	subject_list = entities[subject_entity]
	if isinstance(subject_list[0],dict):
		subject_list = [e['entity'] for e in subject_list]
	object_list = entities[object_entity]
	if isinstance(object_list[0],dict):
		object_list = [e['entity'] for e in object_list]
	###
	subject_list = list(set(subject_list))
	subject_list.sort(key = len, reverse = True)
	##
	object_list = list(set(object_list))
	object_list.sort(key = len, reverse = True)
	##
	subject_object_pair = [(subject,object) \
		for subject in subject_list\
		for object in object_list]
	##
	if subject_entity == object_entity:
		subject_object_pair = [e for e in \
			subject_object_pair\
			if e[0] != e[1]]
	##
	subject_object_context_list = []
	for e in subject_object_pair:
		context = re.sub(re.escape(' '+e[0]+' '), \
			' _subject_ ', input)
		context = re.sub(re.escape(' '+e[1]+' '), \
			' _object_ ', context)
		c = {'context': context, \
			'subject': e[0],\
			'object': e[1]}		
		subject_object_context_list.append(c)
	##
	return subject_object_context_list

'''
text_entity_context = {"context":" _start_ i am _entity_ ' s brother , john wang . _end_ ","entity":"jim"}
entities = {"title":["brother"],"name":["jim","john wang"]}

text_entity_context2context_entity_wild(\
	text_entity_context,\
	entities,\
	conext_wild_entities = ["title","name"])

subject_object_context_list = [{'object': 'father', 'context': ' _start_ jim is my _object_ and _subject_ is my mother , we live in abu dhabi _end_ ', 'subject': 'yan'}, {'object': 'mother', 'context': ' _start_ jim is my father and _subject_ is my _object_ , we live in abu dhabi _end_ ', 'subject': 'yan'}, {'object': 'father', 'context': ' _start_ _subject_ is my _object_ and yan is my mother , we live in abu dhabi _end_ ', 'subject': 'jim'}, {'object': 'mother', 'context': ' _start_ _subject_ is my father and yan is my _object_ , we live in abu dhabi _end_ ', 'subject': 'jim'}]
entities = {'name':['jim','yan'], 'title':['father','mother'],'location':['abu dhabi']}

text_entity_context2context_entity_wild(\
	subject_object_context_list[0],\
	entities,\
	conext_wild_entities = ["title","name","location"])
'''
def text_entity_context2context_entity_wild(\
	text_entity_context,\
	entities,\
	conext_wild_entities = []):	
	output = {}
	output['context'] = text_normalized2text_entity_wild(\
		text_entity_context["context"], 
		entities = entities,\
		wild_entities = conext_wild_entities)
	try:
		output['entity'] = text_entity_context['entity']
	except:
		pass
	try:
		output['subject'] = text_entity_context['subject']
	except:
		pass
	try:
		output['object'] = text_entity_context['object']
	except:
		pass
	return output

'''
input = ' _start_ this is a ... gsdg@gmail.com xxx _name_ _end_ '
text_normalized2words(input)
'''
def text_normalized2words(input):
	try:
		return input.strip().split(' ')
	except:
		return None

'''
input = {'entity':'jim','context':' _start_ my name is _entity_ how are you _location_ '}
entity_context2context_words(input)
'''
def entity_context2context_words(input):
	entity = text_normalized2words(input['entity'])
	context = input['context'].split('_entity_')	
	left_context = context[0]
	left_context = text_normalized2words(left_context)
	right_context = context[-1]
	right_context = text_normalized2words(right_context)
	return {'left_words':left_context,\
		'right_words':right_context,\
		'entity_words':entity}

'''
input = {'object': 'father', 'context': u' _start_ _name_ is my _object_ and _subject_ is my _title_ , we live in _location_ _end_ ', 'subject': 'yan'}
subject_object_context2context_words(input)
'''
def subject_object_context2context_words(input):
	subject = text_normalized2words(input['subject'])
	object = text_normalized2words(input['object'])
	##
	subject_context = input['context'].split('_subject_')
	subject_left_context = text_normalized2words(\
		subject_context[0])
	subject_right_context = text_normalized2words(\
		subject_context[-1])
	##
	object_context = input['context'].split('_object_')
	object_left_context = text_normalized2words(\
		object_context[0])
	object_right_context = text_normalized2words(\
		object_context[-1])
	##
	context_words = text_normalized2words(\
		input['context'])
	##
	return {'subject_words':subject,\
		'subject_left_words':subject_left_context,\
		'subject_right_words':subject_right_context,\
		'object_words':object,\
		'object_left_words':object_left_context,\
		'object_right_words':object_right_context,\
		'context_words':context_words}

'''
input = ['_start_', 'my', 'name', 'is', '_name_', '_end_']
words2word_idx(input)
'''
hash_function = lambda w: \
	int(md5(w.encode('utf-16')).hexdigest(), 16)

def words2word_idx(input, \
	num_word_max = num_word_max):
	return [(hash_function(w.lower()) \
		% (num_word_max - 1) + 1) \
		for w in input]

'''
input = {'right_words': ['how', 'are', 'you', '_location_'], 'entity_words': ['jim'], 'left_words': ['_start_', 'my', 'name', 'is']}
context_words2word_idx(input)

input = {'object_right_words': [u'and', u'_subject_', u'is', u'my', u'_title_', u',', u'we', u'live', u'in', u'_location_', u'_end_'], 'object_left_words': [u'_start_', u'_name_', u'is', u'my'], 'object_words': ['father'], 'context_words': [u'_start_', u'_name_', u'is', u'my', u'_object_', u'and', u'_subject_', u'is', u'my', u'_title_', u',', u'we', u'live', u'in', u'_location_', u'_end_'], 'subject_words': ['yan'], 'subject_left_words': [u'_start_', u'_name_', u'is', u'my', u'_object_', u'and'], 'subject_right_words': [u'is', u'my', u'_title_', u',', u'we', u'live', u'in', u'_location_', u'_end_']}
context_words2word_idx(input)
'''
def context_words2word_idx(\
	input,\
	num_word_max = num_word_max):
	output = {}
	if 'entity_words' in input:
		output['entity_word_idx'] = \
			words2word_idx(\
			input['entity_words'],\
			num_word_max = num_word_max)
	if 'left_words' in input:
		output['left_word_idx'] = \
			words2word_idx(\
			input['left_words'],\
			num_word_max = num_word_max)
	if 'right_words' in input:
		output['right_word_idx'] = \
			words2word_idx(\
			input['right_words'],\
			num_word_max = num_word_max)
	if 'subject_words' in input:
		output['subject_word_idx'] = \
			words2word_idx(\
			input['subject_words'],\
			num_word_max = num_word_max)
	if 'subject_left_words' in input:
		output['subject_left_idx'] = \
			words2word_idx(\
			input['subject_left_words'],\
			num_word_max = num_word_max)
	if 'subject_right_words' in input:
		output['subject_right_idx'] = \
			words2word_idx(\
			input['subject_right_words'],\
			num_word_max = num_word_max)
	if 'object_words' in input:
		output['object_word_idx'] = \
			words2word_idx(\
			input['object_words'],\
			num_word_max = num_word_max)
	if 'object_left_words' in input:
		output['object_left_idx'] = \
			words2word_idx(\
			input['object_left_words'],\
			num_word_max = num_word_max)
	if 'object_right_words' in input:
		output['object_right_idx'] = \
			words2word_idx(\
			input['object_right_words'],\
			num_word_max = num_word_max)
	if 'context_words' in input:
		output['context_word_idx'] = \
			words2word_idx(\
			input['context_words'],\
			num_word_max = num_word_max)
	return output
#############pami_re.py###########