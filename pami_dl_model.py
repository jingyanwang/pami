################pami_dl_model.py###############
'''
pip install --upgrade keras
'''

import h5py
import time
import numpy as np

from keras.losses import *
from keras.metrics import *
from keras.models import *
from keras.layers import *
from keras.utils import *
from tensorflow.keras.utils import to_categorical
from keras.preprocessing.text import *
from keras.preprocessing.sequence import *

from pami_re import *

'''
sequence_labelding model
'''
def build_sequence_tagging_model(
	positive_class_num = 1,
	gpus = None):
	model = Sequential()
	model.add(Embedding(input_dim = num_word_max, 
		output_dim = 300, 
		input_length = num_max_text_len))
	model.add(Dropout(0.1))
	model.add(Bidirectional(
		LSTM(300, 
			dropout=0.1, 
			recurrent_dropout=0.1,
			return_sequences=True)))
	model.add(Bidirectional(
		LSTM(300, 
			dropout=0.1, 
			recurrent_dropout=0.1,
			return_sequences=True)))
	model.add(Conv1D(500,
		kernel_size = num_max_context_len,
		activation='relu',
		padding = 'same',
		strides=1))
	model.add(Conv1D(500,
		kernel_size = num_max_context_len,
		activation='relu',
		padding = 'same',
		strides=1))
	model.add(MaxPooling1D(
		strides=1, padding='same'))
	model.add(Conv1D(500,
		kernel_size = num_max_context_len,
		activation='relu',
		padding = 'same',
		strides=1))
	model.add(MaxPooling1D(
		strides=1, padding='same'))
	model.add(TimeDistributed(Dense(300, activation='relu')))
	model.add(TimeDistributed(Dense(positive_class_num+1, 
		activation='softmax')))
	if gpus is not None:
		model = multi_gpu_model(model, gpus = gpus)
	return model


'''
usage:

num_max_text_len = 5
x_train = np.array([
	[1,8,1,1,1],
	[1,1,1,10,8],
	[1,1,1,10,8]
	])
y_train = np.array([
	[0,1,0,0,0],
	[0,0,0,1,1],
	[0,0,0,0,0]
	])

model = train_sequence_tagging_model(
	x_train, y_train, \
	positive_class_num = None,\
	positive_weight = 1, \
	model_file = 'temp_model.h5py', \
	batch_size=500, 
	epochs = 20,
	gpus = None)

print(model.predict(x_train))
'''
def train_sequence_tagging_model(
	x_train, y_train, \
	positive_class_num = None,\
	positive_weight = 1, \
	model_file = 'temp_model.h5py', \
	batch_size = 500, 
	epochs = 3,
	gpus = None):
	y_train1 = to_categorical(y_train)
	if positive_class_num is None:
		positive_class_num = y_train1.shape[-1]-1
	model = build_sequence_tagging_model(
		positive_class_num = positive_class_num,
		gpus = gpus)
	sample_weights = np.array(y_train != 0).astype(int)\
		*positive_weight \
		+ np.array(y_train == 0).astype(int)
	model.compile(
		optimizer='rmsprop', 
		loss='categorical_crossentropy',
		metrics=['accuracy'],
		sample_weight_mode='temporal')
	model.fit(x_train, y_train1,
		sample_weight = sample_weights,
		batch_size=batch_size, 
		epochs=epochs)
	model.save_weights(model_file)
	return model

'''
multi-class models
'''
def build_text_categorization_model(\
	positive_class_num = 1,\
	gpus = None):
	model = Sequential()
	model.add(Embedding(input_dim = num_word_max,
		output_dim = 300,
		input_length=num_max_text_len))
	model.add(Dropout(0.1))
	model.add(Conv1D(filters = 500,
		kernel_size = 2, padding='valid',
		activation='relu', strides=1))
	model.add(Conv1D(filters = 500,
		kernel_size = 2, padding='valid',
		activation='relu', strides=1))
	model.add(MaxPooling1D(pool_size=2))
	model.add(Dropout(0.1))
	model.add(Conv1D(filters = 500,
		kernel_size = 3, padding='valid',
		activation='relu', strides=1))
	model.add(GlobalMaxPooling1D())
	model.add(Dropout(0.1))
	model.add(Dense(units = 500, 
		activation='relu'))
	model.add(Dense(positive_class_num+1, \
		activation='softmax'))
	if gpus is not None:
		model = multi_gpu_model(model, gpus = gpus)
	return model

def train_text_categorization_model(x_train, y_train, \
	positive_class_num = None,\
	positive_weight = 1, \
	model_file = None, \
	batch_size = 500, \
	epochs = 3,\
	gpus = None):
	if positive_class_num is None:
		positive_class_num = y_train.shape[1]-1
	model = build_text_categorization_model(positive_class_num,\
		gpus = gpus)
	model.compile(loss='categorical_crossentropy',
		optimizer='adam', metrics=['accuracy'])
	class_weight =  {0: 1}
	for class_idx in range(1,positive_class_num+1):
		class_weight[class_idx] = positive_weight
	model.fit(x_train, y_train,
		class_weight = class_weight,
		batch_size=batch_size, epochs=epochs)
	if model_file is not None:
		model.save_weights(model_file)
	return model

def load_text_categorization_model(\
	model_file,\
	positive_class_num = 1):		
	model = build_text_categorization_model(\
		positive_class_num = positive_class_num)
	model.load_weights(model_file)
	model.compile(loss='categorical_crossentropy',
		optimizer='adam', metrics=['accuracy'])
	model._make_predict_function()
	return model

'''
document_transfer_from_receiver_model = \
	load_text_categorization_model(\
	'document_transfer_from_receiver.h5py',\
	positive_class_num = 1)
input = u" _start_ please send me the document _end_ "
text_categorization_dl(input, 
	document_transfer_from_receiver_model)
'''
def text_categorization_model_predict(input, 
	model):
	try:
		words = text_normalized2words(input)
		word_idx = words2word_idx(words)
		x = pad_sequences([word_idx], \
			maxlen = num_max_text_len)
		y_score = model.predict(x)
		prediction = np.argmax(y_score, axis=1)[0]
		score = np.max(y_score, axis=1)[0]
		return prediction, score
	except:
		return None

'''
entity models
'''
def build_entity_context_model(\
	num_max_context_len = num_max_context_len,\
	num_max_entity_len = num_max_entity_len,\
	include_entity_input = False,\
	gpus = None):
	###left context
	input_left = Input(shape=(num_max_context_len, ))
	emb_word_left = Dropout(0.3)(\
		Embedding(
		input_dim = num_word_max,
		output_dim = 300,
		input_length=num_max_context_len)\
		(input_left))
	conv_left = Conv1D(filters = 500,
		kernel_size = 2, padding='valid',
		activation='relu', strides=1)(\
		emb_word_left)
	max_pooling_left = \
		Dropout(0.3)(\
		GlobalMaxPooling1D()(\
		conv_left))
	###entity
	input_name = Input(shape=(num_max_entity_len, ))
	emb_name = Dropout(0.3)(\
		Embedding(
		input_dim = num_word_max,
		output_dim = 300,
		input_length=num_max_entity_len)\
		(input_name))
	conv_name = Conv1D(filters = 100,
		kernel_size = 1, padding='valid',
		activation='relu', strides=1)(\
		emb_name)
	max_pooling_name = \
		Dropout(0.3)(\
		GlobalMaxPooling1D()(\
		conv_name))
	###right context
	input_right = Input(shape=(num_max_context_len, ))
	emb_word_right = Dropout(0.3)(\
		Embedding(
		input_dim = num_word_max,
		output_dim = 300,
		input_length=num_max_context_len)\
		(input_right))
	conv_right = Conv1D(filters = 500,
		kernel_size = 2, padding='valid',
		activation='relu', strides=1)(\
		emb_word_right)
	max_pooling_right = \
		Dropout(0.3)(\
		GlobalMaxPooling1D()(\
		conv_right))
	###
	if include_entity_input:
		marge_layer = [max_pooling_left,\
			input_name,\
			max_pooling_right]
		input_layer = [input_left, input_name, input_right]

	else:
		marge_layer = [max_pooling_left,\
			max_pooling_right]
		input_layer = [input_left, input_right]
	merge_context = concatenate(marge_layer)
	dense = Dense(units = 1000, 
		activation='relu')(\
		merge_context)
	output = Dense(units = 2, 
		activation='softmax')(\
		dense)
	model = Model(\
		inputs=input_layer,\
		outputs=output)
	if gpus is not None:
		model = multi_gpu_model(model, gpus= gpus)
	return model

def train_entity_context_model(x_train, y_train, \
	positive_weight = 1, \
	model_file = None, \
	batch_size=500, \
	num_max_context_len = num_max_context_len,\
	gpus = None,\
	epochs = 3):
	model = build_entity_context_model(num_max_context_len \
		= num_max_context_len,\
		gpus = gpus)
	model.compile(loss='categorical_crossentropy',
		optimizer='adam', metrics=['accuracy'])
	model.fit(x_train, y_train,
		class_weight = {0: 1, 1: positive_weight},
		batch_size=batch_size, epochs=epochs)
	if model_file is not None:
		model.save_weights(model_file)
	return model

def load_entity_context_model(model_file,\
	num_max_context_len = num_max_context_len):		
	model = build_entity_context_model(\
		num_max_context_len \
		= num_max_context_len)
	model.load_weights(model_file)
	model.compile(loss='categorical_crossentropy',
		optimizer='adam', metrics=['accuracy'])
	model._make_predict_function()
	return model

'''
input = {'entity':'jim','context':' _start_ _entity_ is going to travel to _location_ '}

model_travel_person = load_entity_context_model(\
	'travel_person.h5py')

entity_context_categorization_model_predict(\
	input,\
	model = model_travel_person)
'''
def entity_context_categorization_model_predict(\
	input,\
	model,\
	num_max_context_len = num_max_context_len):
	try:
		context_words = entity_context2context_words(input)
		context_word_idx = context_words2word_idx(context_words)
		x_left = pad_sequences(\
			[context_word_idx['left_word_idx']], \
			maxlen = num_max_context_len,\
			truncating = 'pre',\
			padding='pre')
		x_right = pad_sequences(\
			[context_word_idx['right_word_idx']], \
			maxlen = num_max_context_len,\
			truncating = 'post',\
			padding='post')
		x = [x_left, x_right]
		y_score = model.predict(x)
		prediction = np.argmax(y_score, axis=1)[0]
		score = np.max(y_score, axis=1)[0]
		return prediction, score
	except:
		return None

'''
subject object relatioin models
'''

'''
building basic cnn layer for context/entity

input_layer, output_layer = \
	build_context_cnn_layer(\
	num_max_context_len = num_max_context_len)
'''
def build_context_cnn_layer(\
	num_max_context_len = num_max_context_len):
	input_left = Input(shape=(num_max_context_len, ))
	emb_word_left = Dropout(0.3)(\
		Embedding(
		input_dim = num_word_max,
		output_dim = 300,
		input_length=num_max_context_len)\
		(input_left))
	conv_left = Conv1D(filters = 500,
		kernel_size = 2, padding='valid',
		activation='relu', strides=1)(\
		emb_word_left)
	max_pooling_left = \
		Dropout(0.3)(\
		GlobalMaxPooling1D()(\
		conv_left))
	return input_left, max_pooling_left

'''
subject-object models

build_subject_object_context_model(\
	num_max_context_len = 3,\
	num_max_entity_len = 2,\
	num_max_text_len = 10,\
	num_word_max = 400000,\
	include_entity_input = True,\
	gpus = None)
'''
def build_subject_object_context_model(\
	num_max_context_len = num_max_context_len,\
	num_max_entity_len = num_max_entity_len,\
	num_max_text_len = num_max_text_len,\
	num_word_max = num_word_max,\
	include_entity_input = False,\
	gpus = None):
	###
	context_input, context_cnn = \
		build_context_cnn_layer(\
		num_max_context_len = num_max_text_len)
	##
	subject_left_input, subject_left_cnn = \
		build_context_cnn_layer(\
		num_max_context_len = num_max_context_len)
	subject_right_input, subject_right_cnn = \
		build_context_cnn_layer(\
		num_max_context_len = num_max_context_len)
	object_left_input, object_left_cnn = \
		build_context_cnn_layer(\
		num_max_context_len = num_max_context_len)
	object_right_input, object_right_cnn = \
		build_context_cnn_layer(\
		num_max_context_len = num_max_context_len)
	##
	subject_input, subject_cnn = \
		build_context_cnn_layer(\
		num_max_context_len = num_max_entity_len)
	object_input, object_cnn = \
		build_context_cnn_layer(\
		num_max_context_len = num_max_entity_len)
	####
	if include_entity_input:
		marge_layer = [\
			context_cnn,\
			subject_left_cnn,\
			subject_right_cnn,\
			object_left_cnn,\
			object_right_cnn,\
			subject_cnn,\
			object_cnn]
		input_layer = [\
			context_input,\
			subject_left_input,\
			subject_right_input,\
			object_left_input,\
			object_right_input,\
			subject_input,\
			object_input]
	else:
		marge_layer = [\
			context_cnn,\
			subject_left_cnn,\
			subject_right_cnn,\
			object_left_cnn,\
			object_right_cnn]
		input_layer = [\
			context_input,\
			subject_left_input,\
			subject_right_input,\
			object_left_input,\
			object_right_input]
	##
	merge_context = concatenate(marge_layer)
	dense = Dense(units = 3000, 
		activation='relu')(\
		merge_context)
	output = Dense(units = 2, 
		activation='softmax')(\
		dense)
	model = Model(\
		inputs=input_layer,\
		outputs=output)
	if gpus is not None:
		model = multi_gpu_model(model, gpus= gpus)
	return model

def train_subject_object_context_model(\
	x_train, y_train, \
	positive_weight = 1, \
	model_file = None, \
	batch_size=500, \
	num_max_context_len = num_max_context_len,\
	num_max_entity_len = num_max_entity_len,\
	num_max_text_len = num_max_text_len,\
	num_word_max = num_word_max,\
	gpus = None,\
	epochs = 3):
	model = build_subject_object_context_model(\
		num_max_context_len = num_max_context_len,\
		num_max_entity_len = num_max_entity_len,\
		num_max_text_len = num_max_text_len,\
		num_word_max = num_word_max,\
		include_entity_input = False,\
		gpus = gpus)
	model.compile(loss='categorical_crossentropy',
		optimizer='adam', metrics=['accuracy'])
	model.fit(x_train, y_train,
		class_weight = {0: 1, 1: positive_weight},
		batch_size=batch_size, epochs=epochs)
	if model_file is not None:
		model.save_weights(model_file)
	return model

def load_subject_object_context_model(model_file,\
	num_max_context_len = num_max_context_len,\
	gpus = None):		
	model = build_subject_object_context_model(\
		num_max_context_len = num_max_context_len,\
		num_max_entity_len = num_max_entity_len,\
		num_max_text_len = num_max_text_len,\
		num_word_max = num_word_max,\
		include_entity_input = False,\
		gpus = gpus)
	model.load_weights(model_file)
	model.compile(loss='categorical_crossentropy',
		optimizer='adam', metrics=['accuracy'])
	model._make_predict_function()
	return model

def subject_object_context_categorization_model_predict(\
	input,\
	model,\
	num_max_context_len = num_max_context_len,\
	num_max_text_len = num_max_text_len):
	try:
		context_words = subject_object_context2context_words(input)
		context_word_idx = context_words2word_idx(context_words)
		x_subject_left_words = pad_sequences(\
			[context_word_idx['subject_left_idx']], \
			maxlen = num_max_context_len,\
			truncating = 'pre',\
			padding='pre')
		x_object_left_words = pad_sequences(\
			[context_word_idx['object_left_idx']], \
			maxlen = num_max_context_len,\
			truncating = 'pre',\
			padding='pre')
		x_subject_right_words = pad_sequences(\
			[context_word_idx['subject_right_idx']], \
			maxlen = num_max_context_len,\
			truncating = 'post',\
			padding='post')
		x_object_right_words = pad_sequences(\
			[context_word_idx['object_right_idx']], \
			maxlen = num_max_context_len,\
			truncating = 'post',\
			padding='post')
		x_context_words = pad_sequences(\
			[context_word_idx['context_word_idx']], \
			maxlen = num_max_text_len)
		x = [x_context_words, \
			x_subject_left_words,\
			x_subject_right_words,\
			x_object_left_words,\
			x_object_right_words]
		y_score = model.predict(x)
		prediction = np.argmax(y_score, axis=1)[0]
		score = np.max(y_score, axis=1)[0]
		return prediction, score
	except:
		return None

################pami_dl_model.py###############