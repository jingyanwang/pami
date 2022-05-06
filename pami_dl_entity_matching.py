import h5py
import time
import numpy

from pami_spark import *

from keras.utils import *
from keras.losses import *
from keras.models import *
from keras.layers import *
from keras.metrics import *
from keras.utils.training_utils import *
from keras.preprocessing.text import *
from keras.preprocessing.sequence import *

def build_timestamp_attribute_embedding_layer(\
	max_attribute_type_num,\
	max_attribute_len = 40,\
	attribute_emb_dim = 300,\
	attribute_seq_emb_dim = 500):
	input_layer = Input(shape=(max_attribute_len, ))
	###attribute embedding
	emb_layer = Dropout(0.3)(\
		Embedding(
		input_dim = max_attribute_type_num,
		output_dim = 200,
		input_length = max_attribute_len)\
		(input_layer))
	####extrcting 500
	conv_layer = Conv1D(filters = 400,
		kernel_size = 2, padding='valid',
		activation='relu', strides=1)(\
		emb_layer)
	max_pooling_layer1 = Dropout(0.2)(MaxPooling1D()(conv_layer))
	conv_layer1 = Conv1D(filters = attribute_seq_emb_dim, kernel_size = 2, padding='valid', activation='relu', strides=1)(max_pooling_layer1)
	max_pooling_layer1 = Dropout(0.2)(GlobalMaxPooling1D()(conv_layer1))
	return input_layer, max_pooling_layer1

def building_entity_embedding_model_from_attribute(embedding_attributes_x,
	max_attribute_len,
	layer_name = 'embedding_layer'):
	input_layer_all = []
	embed_layer_all = []
	#####
	for a in embedding_attributes_x:
		max_attribute_type_num = a["attribute_type_num"]
		if a["attribute_type_num"] < 1000:
			attribute_emb_dim = 50
			attribute_seq_emb_dim = 100
		else:
			attribute_emb_dim = 300
			attribute_seq_emb_dim = 500
		input_layer, embedding_layer = build_timestamp_attribute_embedding_layer(\
			max_attribute_type_num = a["attribute_type_num"],\
			max_attribute_len = max_attribute_len,\
			attribute_emb_dim = attribute_emb_dim,\
			attribute_seq_emb_dim = attribute_seq_emb_dim)
		input_layer_all.append(input_layer)
		embed_layer_all.append(embedding_layer)
	###
	marge_layer1 = concatenate(embed_layer_all)
	dense1 = Dense(units = 500, activation='relu')(marge_layer1)
	dense2 = Dense(units = 500, activation='relu', name = layer_name)(dense1)
	return input_layer_all, dense2

def build_entity_embedding_comparision_model(
	embedding_layer_x,
	embedding_layer_y):
	layer_subtract = subtract([embedding_layer_x, embedding_layer_y])
	layer_multiply = multiply([embedding_layer_x, embedding_layer_y])
	layer_concatenate = concatenate([embedding_layer_x, embedding_layer_y], axis=-1)
	layer_dot = dot([embedding_layer_x, embedding_layer_y], axes = -1, normalize=False)
	####
	layer_match = concatenate([layer_subtract,layer_multiply,layer_concatenate,layer_dot])
	###
	dense1 = Dense(units = 500, activation='relu')(layer_match)
	output = Dense(units = 2, activation='softmax')(dense1)
	return output

def building_entity_matching_model(
	embedding_attributes_x,
	embedding_attributes_y,
	max_attribute_len = 10,
	gpus = None):
	input_layer_x, embedding_layer_x = building_entity_embedding_model_from_attribute(embedding_attributes_x,
		max_attribute_len, layer_name = 'embeding_layer_x')
	input_layer_y, embedding_layer_y = building_entity_embedding_model_from_attribute(embedding_attributes_y,
		max_attribute_len, layer_name = 'embeding_layer_y')
	###
	output = build_entity_embedding_comparision_model(embedding_layer_x, embedding_layer_y)
	##
	model = Model(inputs=input_layer_x+input_layer_y, outputs=output)
	if gpus is not None:
		model = multi_gpu_model(model, gpus = gpus)
	return model

def left_pad_sequence(input, max_len = 10):
	len_input = len(input)
	if len_input >= max_len:
		return input[0:max_len]
	output = [0]*max_len
	output[0:len_input] = input
	return output

'''
print(left_pad_sequence([1,2,3], max_len = 2))
'''

def convert_entity_matching_json_to_npy_file(input_train_data_json,
	output_x_npy_file,
	output_document_id_npy_file,
	embedding_attributes_x,
	embedding_attributes_y,
	sqlContext,
	output_y_npy_file = None,
	max_attribute_len = 10):
	print('loading data from %s'%(input_train_data_json))
	matching_candidate = sqlContext.read.json(input_train_data_json)
	###
	udf_idx_pad = udf(lambda input: left_pad_sequence(input, max_attribute_len), ArrayType(IntegerType()))
	number_pair = matching_candidate.count()
	print('loaded %d entity pairs from %s'%(number_pair, input_train_data_json))
	###
	print('extracting entity attribuest and conver to word index')
	x = []
	for a in embedding_attributes_x+embedding_attributes_y:
		input_column_name = a['attribute_name']
		input_attribute_num = a['attribute_type_num']
		udf_idx = udf(lambda input: words2word_idx(input, input_attribute_num), ArrayType(IntegerType()))
		idx_data = matching_candidate\
			.withColumn('idx', udf_idx(input_column_name))\
			.withColumn('idx', udf_idx_pad('idx'))\
			.select('idx')
		#idx_data.show()
		#idx_data = [numpy.array(r.idx) for r in idx_data]
		x_attribute = numpy.array(idx_data.collect())
		x_attribute = x_attribute.reshape(number_pair, max_attribute_len)
		#print(x_attribute)
		x.append(x_attribute)
	x = numpy.array(x)
	print('saving x to %s'%(output_x_npy_file))
	numpy.save(output_x_npy_file, x)
	####
	if 'label' in matching_candidate.columns:
		print('extracting labels')
		label_data = matching_candidate.select('label').collect()
		y = to_categorical(numpy.array(label_data).reshape(number_pair, 1))
		print('saving y to %s'%(output_y_npy_file))
		numpy.save(output_y_npy_file, y)
	if 'document_id' in matching_candidate.columns:
		print('extracting document_id')
		document_id_data = matching_candidate.select('document_id').collect()
		z = numpy.array(document_id_data).reshape(number_pair)
		print('saving document_id to %s'%(output_document_id_npy_file))
		numpy.save(output_document_id_npy_file, z)

def train_entity_matching_model(
	embedding_attributes_x,
	embedding_attributes_y,
	sqlContext,
	input_x_npy = None,
	input_y_npy = None,
	max_attribute_len = 10,
	entity_matching_model_file = None,
	epochs = 10,
	class_weight =  {0:1, 1:1},
	update_existing_model = False,
	show_prediction_rate = True):
	if input_x_npy is not None:
		print('loading data from %s'%(input_x_npy))
		x = np.load(input_x_npy)
		x =  list(x)
		print('loaded %d pairs of entities for training from %s'%(x[0].shape[0], input_x_npy))
	if  input_y_npy is not None:
		print('loading label from %s'%(input_y_npy))
		y = np.load(input_y_npy)
		print('loaded %d labels for training from %s'%(y.shape[0], input_y_npy))
	###
	print('building the model')
	entity_matching_model = building_entity_matching_model(
		embedding_attributes_x,
		embedding_attributes_y,
		max_attribute_len = max_attribute_len,
		gpus = None)
	#entity_matching_model.predict(x)
	entity_matching_model.compile(loss='categorical_crossentropy',
		optimizer='adam', metrics=['accuracy'])
	batch_size = 100
	if update_existing_model is True:
		print('loading existing model from %s'%(entity_matching_model_file))
		entity_matching_model.load_weights(entity_matching_model_file)
	print('training the model')
	entity_matching_model.fit(x, y,
		class_weight = class_weight,
		batch_size = batch_size, epochs = epochs)
	print('saving the model to %s'%(entity_matching_model_file))
	if entity_matching_model_file is not None:
		entity_matching_model.save_weights(entity_matching_model_file)
	if show_prediction_rate is True:
		print('predicting the matching score')
		y_score = entity_matching_model.predict(x)
		prediction = numpy.argmax(y_score, axis = 1)
		label = numpy.argmax(y, axis = 1)
		data_y = [(str(p),str(l)) for p,l in zip(prediction, label)]
		####
		data_prediction = sqlContext.createDataFrame(data_y, ["prediction", "label"]).persist()
		print('showing the confusion matrix of the matching model over training set')
		data_prediction.registerTempTable("prediction")
		sqlContext.sql(u"""
			SELECT label, prediction, COUNT(*)
			FROM prediction
			GROUP BY label, prediction
			""").show()	
	return entity_matching_model

def entity_matching_predict(
	embedding_attributes_x,
	embedding_attributes_y,
	input_x_npy,
	input_document_id_npy,
	entity_matching_model_file,
	output_predict_json,
	sqlContext,
	max_attribute_len = 10,
	show_prediction_rate = True):
	####
	if input_x_npy is not None:
		print('loading data from %s'%(input_x_npy))
		x = np.load(input_x_npy)
		x =  list(x)
	####
	if input_document_id_npy is not None:
		print('loading document_id from %s'%(input_document_id_npy))
		document_id = np.load(input_document_id_npy)
		document_id =  list(document_id)
	####
	print('building the model')
	entity_matching_model = building_entity_matching_model(
		embedding_attributes_x,
		embedding_attributes_y,
		max_attribute_len = max_attribute_len,
		gpus = None)
	####
	print('loading model from %s'%(entity_matching_model_file))
	entity_matching_model.load_weights(entity_matching_model_file)
	####
	print('predicting the matching score')
	y_score = entity_matching_model.predict(x)
	y_score = list([list(s) for s in y_score])
	####
	data_prediction = [(str(d), [float(s[0]), float(s[1])]) for d, s in zip(document_id, y_score)]
	data_prediction = sqlContext.createDataFrame(data_prediction, ["document_id", "score"]).persist()
	data_prediction.registerTempTable("matching_score")
	######
	print('saving the prediction results to %s'%(output_predict_json))
	sqlContext.sql(u"""
		SELECT document_id, 
		CASE 
			WHEN score[1] > score[0]
			THEN 'matched'
			ELSE 'not_matched'
		END AS prediction,
		CASE 
			WHEN score[1] > score[0]
			THEN score[1]
			ELSE score[0]
		END AS score
		FROM matching_score
		""").write.mode('Overwrite').json(output_predict_json)
	print('saved to %s'%(output_predict_json))
	if show_prediction_rate is True:
		sqlContext.read.json(output_predict_json).registerTempTable('output_predict_json')
		sqlContext.sql(u"""
			SELECT prediction, COUNT(*)
			FROM output_predict_json
			GROUP BY prediction
			""").show()


