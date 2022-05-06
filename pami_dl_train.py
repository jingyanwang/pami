################pami_dl_train.py###############
from pami_spark import *
from pami_dl_model import *

'''
train a deep learning model from a input json file
the input json file should have text, word_indx, label feilds

usage:

sudo rm input.json
sudo vi input.json
i{"name":["jane"],"sender_married_indicator":["my wife"],"text":"My name is Jane","text_normalized":" _start_ my wife name is jane _end_ ","label":1,"word_idx":[133723,193720,49720,30974,89323,147520,44856]}
{"name":["jim"],"sender_married_indicator":[],"text":"My name is Jim","text_normalized":" _start_ my name is jim _end_ ","label":0,"word_idx":[133723,193720,30974,89323,147520,44856]}
{"name":["jim"],"sender_married_indicator":[],"text":"My name is Jim","text_normalized":" _start_ my name is jim _end_ ","label":0,"word_idx":[133723,193720,30974,89323,147520,44856]}
{"name":["jim"],"sender_married_indicator":[],"text":"My name is Jim","text_normalized":" _start_ my name is jim _end_ ","label":0,"word_idx":[133723,193720,30974,89323,147520,44856]}

train_text_categorization_model_from_json(\
	input_json = 'input.json',\
	positive_weight_factor = 10000,\
	model_file = 'model.h5py',\
	output_json_prediction = 'output.json',\
	output_json_recommend = 'recommend.json',\
	num_train = 100,\
	num_recommend = 5000,\
	epochs = 2)

cat output.json/*
cat recommend.json/*
'''
def train_text_categorization_model_from_json(\
	input_json,\
	positive_weight = None,\
	positive_weight_factor = 1,\
	model_file = None,\
	output_json_prediction = None,\
	output_json_recommend = None,\
	num_train = None,\
	num_recommend = 5000,\
	epochs = 3,\
	gpus = None,\
	sqlContext = None):
	start_time = time.time()
	if sqlContext is None:
		sqlContext = sqlContext_local
	print('loading data from '+input_json)
	data = sqlContext.read.json(input_json)
	num_text = data.count()
	print('loaded '+str(num_text)\
		+' records from '+input_json)
	##
	print('prepare the input')
	data_text_idx1 = np.array(\
		data.select('word_idx').collect())\
		.reshape((num_text,))
	x = pad_sequences(data_text_idx1, \
		maxlen = num_max_text_len)
	if 'label' in data.columns:
		print('prepare the label')
		label = np.array(\
			data.select('label')\
			.collect())\
			.reshape((num_text,))
		y = to_categorical(label)
	#if not os.path.isfile(model_file):
	print('training the model')
	print('number of total samples of each class')
	print(np.sum(y,0))
	idx_negative = np.where(y[:,0] == 1)[0]
	idx_positive = np.where(y[:,0] != 1)[0]
	if num_train is None:
		num_train = num_text
	else:
		num_train = num_text if num_text <= num_train \
			else num_train
	np.random.shuffle(idx_negative)
	idx_train = np.concatenate((idx_positive, \
		idx_negative[0:num_train-len(idx_positive)]), \
		axis=0)
	x_train = x[idx_train]
	y_train = y[idx_train]
	#calculate the weights of the positive 
	num_sample_per_class = np.sum(y_train, 0)
	weight_1 = positive_weight \
		if positive_weight is not None else \
		(num_sample_per_class[0]/np.sum(num_sample_per_class[1:]))\
		*positive_weight_factor
	print('weight of non-negative class:\t', weight_1)
	#train the model
	##
	model = train_text_categorization_model(\
		x_train, y_train,\
		positive_weight = weight_1, \
		model_file = model_file, \
		epochs = epochs,\
		gpus = gpus)
	'''
	else:
		print('loading model from '+model_file)
		model = load_text_categorization_model(\
			model_file = model_file)
	'''
	#perform prediction and save the prediction results
	print('predicting the labels from data')
	y_score = model.predict(x)
	if 'label' in data.columns:
		df_list = [[int(label1), \
			int(preidction), \
			score, word_idx] \
			for label1, preidction, score, word_idx in \
			zip(label,\
			np.argmax(y_score, axis=1).tolist(),\
			np.max(y_score, axis=1).tolist(),\
			data_text_idx1.tolist())]
		df_schema = StructType([StructField("label", LongType()),\
			StructField('prediction', LongType()),\
			StructField('score', DoubleType()),\
			StructField('word_idx', ArrayType(LongType()))])
	else:
		df_list = [[int(preidction), \
			score, word_idx] \
			for preidction, score, word_idx in \
			zip(np.argmax(y_score, axis=1).tolist(),\
			np.max(y_score, axis=1).tolist(),\
			data_text_idx1.tolist())]	
		df_schema = StructType([\
			StructField('prediction', LongType()),\
			StructField('score', DoubleType()),\
			StructField('word_idx', ArrayType(LongType()))])
	##
	df = sqlContext.createDataFrame(\
		df_list,\
		schema=df_schema)
	df = df.cache()
	df.registerTempTable('prediction')
	if 'label' in df.columns:
		sqlContext.sql(u"""
			SELECT label, prediction, COUNT(*)
			FROM prediction
			GROUP BY label, prediction
			""").show()
	else:
		sqlContext.sql(u"""
			SELECT prediction, COUNT(*)
			FROM prediction
			GROUP BY prediction
			""").show()
	###
	data.registerTempTable('data')
	if output_json_prediction is not None \
		or output_json_recommend is not None:		
		prediction_df = sqlContext.sql(u"""
			SELECT DISTINCT word_idx, score, prediction
			FROM prediction
			""")
		prediction_df = prediction_df.cache()
		prediction_df.registerTempTable('prediction')
	##save the prediction results
	if output_json_prediction is not None:
		output_df = sqlContext.sql(u"""
			SELECT data.*,
			prediction.prediction, prediction.score
			FROM data
			LEFT JOIN prediction
			ON prediction.word_idx
			= data.word_idx
			""").drop('word_idx')
		###save the results
		print('saving prediction results to '+output_json_prediction)
		output_df.write.mode('Overwrite').json(output_json_prediction)
		print('outputs saved to '+output_json_prediction)
	##
	if output_json_recommend is not None:
		recommend_df = sqlContext.sql(u"""
			SELECT data.text, 
			data.text_normalized
			FROM data
			LEFT JOIN prediction
			ON prediction.word_idx
			= data.word_idx
			WHERE label = 0 AND prediction != 0
			ORDER BY score DESC 
			LIMIT """+str(num_recommend))
		###save the results
		print('saving recommended positives to '+output_json_recommend)
		recommend_df.write.mode('Overwrite').json(output_json_recommend)
		print('recommended positives to '+output_json_recommend)
	print('running time: '+str(time.time() - start_time)\
		+' seconds')
	return model

'''
sudo rm input.json
sudo vi input.json
i{"name":["jim","john","john wang"],"sender_name_context":[{"entity":"i am jim ' s brother , _entity_","method":"jimi5"},{"entity":"i am jim ' s brother , _entity_","method":"indicator: i am _name_ ' s _title_ , _entity_"}],"text":"I am Jim's brother, John.","text_entity_context":{"context":" _start_ i am jim ' s brother , _entity_ . _end_ ","entity":"john wang"},"text_normalized":" _start_ i am jim ' s brother , john wang . _end_ ","title":["brother"],"label":1,"left_word_idx":[133723,132517,131610,147520,123858,91324,3237,150994],"right_word_idx":[64256,44856],"entity_word_idx":[91168,168637]}
{"name":["jim","john","john wang"],"sender_name_context":[{"entity":"i am jim ' s brother , _entity_","method":"jimi5"},{"entity":"i am jim ' s brother , _entity_","method":"indicator: i am _name_ ' s _title_ , _entity_"}],"text":"I am Jim's brother, John.","text_entity_context":{"context":" _start_ i am jim ' s brother , _entity_ wang . _end_ ","entity":"john"},"text_normalized":" _start_ i am jim ' s brother , john wang . _end_ ","title":["brother"],"label":1,"left_word_idx":[133723,132517,131610,147520,123858,91324,3237,150994],"right_word_idx":[168637,64256,44856],"entity_word_idx":[91168]}
{"name":["wang"],"sender_name_context":[{"entity":"my name is _entity_","method":"indicator: my name is _entity_"}],"text":"My name is Wang.","text_entity_context":{"context":" _start_ my name is _entity_ _end_ ","entity":"wang"},"text_normalized":" _start_ my name is wang _end_ ","title":[],"label":1,"left_word_idx":[133723,193720,30974,89323],"right_word_idx":[44856],"entity_word_idx":[168637]}
{"name":["jim","john","john wang"],"sender_name_context":[],"text":"I am Jim's brother, John.","text_entity_context":{"context":" _start_ i am _entity_ ' s brother , john wang . _end_ ","entity":"jim"},"text_normalized":" _start_ i am jim ' s brother , john wang . _end_ ","title":["brother"],"label":0,"left_word_idx":[133723,132517,131610],"right_word_idx":[123858,91324,3237,150994,147520,64256,44856],"entity_word_idx":[54106]}
{"name":["jim","john","john wang"],"sender_name_context":[],"text":"I am Jim's brother, John.","text_entity_context":{"context":" _start_ i am _entity_ ' s brother , john wang . _end_ ","entity":"jim"},"text_normalized":" _start_ i am jim ' s brother , john wang . _end_ ","title":["brother"],"label":0,"left_word_idx":[133723,132517,131610],"right_word_idx":[123858,91324,3237,150994,147520,64256,44856],"entity_word_idx":[54106]}

from pami_dl_train import *

train_entity_context_categorization_model_from_json(\
	input_json = 'input.json',\
	positive_weight = 100,\
	positive_weight_factor = 1,\
	model_file = 'model.h5py',\
	output_json_prediction = 'output.json',\
	output_json_recommend = 'recommend.json',\
	num_train = 100,\
	num_recommend = 100,\
	epochs = 1,\
	gpus = None,\
	sqlContext = None)
'''
def train_entity_context_categorization_model_from_json(\
	input_json,\
	positive_weight = None,\
	positive_weight_factor = 1,\
	model_file = None,\
	output_json_prediction = None,\
	output_json_recommend = None,\
	num_train = None,\
	num_recommend = 5000,\
	epochs = 3,\
	gpus = None,\
	sqlContext = None):
	##
	start_time = time.time()
	if sqlContext is None:
		sqlContext = sqlContext_local
	##
	print('loading data from '+input_json)
	data = sqlContext.read.json(input_json)
	num_text = data.count()
	print('loaded '+str(num_text)\
		+' records from '+input_json)
	##
	print('prepare the input')
	data_left_word_idx = np.array(\
		data.select('left_word_idx')\
		.collect())\
		.reshape((num_text,))
	data_right_word_idx = np.array(\
		data.select('right_word_idx')\
		.collect())\
		.reshape((num_text,))
	##
	left_word_idx = np.array(data_left_word_idx)
	x_left_word_idx = pad_sequences(left_word_idx, \
		maxlen = num_max_context_len,\
		truncating = 'pre',\
		padding='pre')
	###
	right_word_idx = np.array(data_right_word_idx)
	x_right_word_idx = pad_sequences(right_word_idx, \
		maxlen = num_max_context_len,\
		truncating = 'post',\
		padding='post')
	x = [x_left_word_idx, \
		x_right_word_idx]
	###
	if 'label' in data.columns:
		print('prepare the label')
		label = np.array(\
			data.select('label')\
			.collect())\
			.reshape((num_text,))
		y = to_categorical(label)
	##
	'''
	if not os.path.isfile(model_file):
	else:
		print('loading the model from '+model_file)
		model = load_entity_context_model(model_file,\
			num_max_context_len = num_max_context_len)
	'''
	print('training the model')
	print('number of total samples of each class')
	print(np.sum(y,0))
	idx_negative = np.where(y[:,0] == 1)[0]
	idx_positive = np.where(y[:,0] != 1)[0]
	if num_train is None:
		num_train = num_text
	else:
		num_train = num_text if num_text <= num_train \
			else num_train
	np.random.shuffle(idx_negative)
	idx_train = np.concatenate((idx_positive, \
		idx_negative[0:num_train-len(idx_positive)]), \
		axis=0)
	x_train = [x_left_word_idx[idx_train], \
		x_right_word_idx[idx_train]]
	y_train = y[idx_train]
	#calculate the weights of the positive 
	num_sample_per_class = np.sum(y_train, 0)
	weight_1 = positive_weight \
		if positive_weight is not None else \
		(num_sample_per_class[0]/np.sum(num_sample_per_class[1:]))\
		*positive_weight_factor
	print('weight of non-negative class:\t', weight_1)
	#train the model
	model = train_entity_context_model(x_train, y_train, \
		positive_weight = weight_1, \
		model_file = model_file, \
		num_max_context_len = num_max_context_len,\
		gpus = gpus,\
		epochs = epochs)
	#perform prediction and save the prediction results
	print('predicting labels from data')
	y_score = model.predict(x)
	##
	if 'label' in data.columns:
		df_list = [[int(label1),\
			int(preidction), \
			score, \
			left_word_idx,\
			right_word_idx] \
			for label1, preidction, score, \
			left_word_idx, right_word_idx in \
			zip(np.argmax(y, axis=1).tolist(),\
			np.argmax(y_score, axis=1).tolist(),\
			np.max(y_score, axis=1).tolist(),\
			data_left_word_idx.tolist(),\
			data_right_word_idx.tolist())]
		df_schema = StructType([StructField('label', LongType()),\
			StructField('prediction', LongType()),\
			StructField('score', DoubleType()),\
			StructField('left_word_idx', ArrayType(LongType())),\
			StructField('right_word_idx', ArrayType(LongType()))])
	else:
		df_list = [[int(preidction), \
			score, \
			left_word_idx,\
			right_word_idx] \
			for preidction, score, \
			left_word_idx, right_word_idx in \
			zip(np.argmax(y_score, axis=1).tolist(),\
			np.max(y_score, axis=1).tolist(),\
			data_left_word_idx.tolist(),\
			data_right_word_idx.tolist())]
		df_schema = StructType([StructField('prediction', LongType()),\
			StructField('score', DoubleType()),\
			StructField('left_word_idx', ArrayType(LongType())),\
			StructField('right_word_idx', ArrayType(LongType()))])
	df = sqlContext.createDataFrame(\
		df_list,\
		schema=df_schema)
	df = df.cache()
	df.registerTempTable('prediction')
	if 'label' in data.columns:
		sqlContext.sql(u"""
			SELECT label, prediction, COUNT(*)
			FROM prediction
			GROUP BY label, prediction
			""").show()
	else:
		sqlContext.sql(u"""
			SELECT prediction, COUNT(*)
			FROM prediction
			GROUP BY prediction
			""").show()
	###
	data.registerTempTable('data')
	if output_json_prediction is not None \
		or output_json_recommend is not None:		
		prediction_df = sqlContext.sql(u"""
			SELECT DISTINCT 
			left_word_idx, right_word_idx, 
			score, prediction
			FROM prediction
			""")
		prediction_df = prediction_df.cache()
		prediction_df.registerTempTable('prediction')
	##save the prediction results
	if output_json_prediction is not None:
		output_df = sqlContext.sql(u"""
			SELECT data.*,
			prediction.prediction, prediction.score
			FROM data
			LEFT JOIN prediction
			ON prediction.left_word_idx
			= data.left_word_idx 
			AND prediction.right_word_idx
			= data.right_word_idx 
			""").drop('left_word_idx')\
			.drop('right_word_idx')\
			.drop('entity_word_idx')
		###save the results
		print('saving prediction results to '+output_json_prediction)
		os.system(u"hadoop fs -rm -r "+output_json_prediction)
		os.system(u"rm -r "+output_json_prediction)
		output_df.write.mode('Overwrite').json(output_json_prediction)
		print('outputs saved to '+output_json_prediction)
	##
	if output_json_recommend is not None:
		recommend_df = sqlContext.sql(u"""
			SELECT data.text, 
			data.text_entity_context
			FROM data
			LEFT JOIN prediction
			ON prediction.left_word_idx
			= data.left_word_idx 
			AND prediction.right_word_idx
			= data.right_word_idx
			WHERE label = 0 AND prediction != 0
			ORDER BY score DESC 
			LIMIT """+str(num_recommend))
		###save the results
		print('saving recommended positives to '+output_json_recommend)
		os.system(u"hadoop fs -rm -r "+output_json_recommend)
		os.system(u"rm -r "+output_json_recommend)
		recommend_df.write.mode('Overwrite').json(output_json_recommend)
		print('recommended positives to '+output_json_recommend)
	print('running time: '+str(time.time() - start_time)\
		+' seconds')
	return model


'''
train_subject_object_context_categorization_model_from_json(\
	input_json = 'name_is_senders_role_train.json',\
	positive_weight_factor = 1,\
	model_file = 'name_is_senders_role.h5py',\
	output_json_prediction = 'name_is_senders_role_predict.json',\
	epochs = 4)
'''
def train_subject_object_context_categorization_model_from_json(\
	input_json,\
	positive_weight = None,\
	positive_weight_factor = 1,\
	model_file = None,\
	output_json_prediction = None,\
	output_json_recommend = None,\
	num_train = None,\
	num_recommend = 5000,\
	epochs = 3,\
	gpus = None,\
	sqlContext = None):
	##
	start_time = time.time()
	if sqlContext is None:
		sqlContext = sqlContext_local
	##
	print('loading data from '+input_json)
	data = sqlContext.read.json(input_json)
	num_text = data.count()
	print('loaded '+str(num_text)\
		+' records from '+input_json)
	##
	print('prepare the input')
	context_word_idx = np.array(data.select('context_word_idx').collect()).reshape((num_text,))
	##
	subject_left_idx = np.array(data.select('subject_left_idx').collect()).reshape((num_text,))
	subject_right_idx = np.array(data.select('subject_right_idx').collect()).reshape((num_text,))
	##
	object_left_idx = np.array(data.select('object_left_idx').collect()).reshape((num_text,))
	object_right_idx = np.array(data.select('object_right_idx').collect()).reshape((num_text,))
	##
	context_word_idx1 = pad_sequences(context_word_idx, maxlen = num_max_text_len)
	##
	subject_left_idx1 = pad_sequences(subject_left_idx, maxlen = num_max_context_len,truncating = 'pre',padding='pre')
	object_left_idx1 = pad_sequences(object_left_idx, maxlen = num_max_context_len,truncating = 'pre',padding='pre')
	subject_right_idx1 = pad_sequences(subject_right_idx, maxlen = num_max_context_len,truncating = 'post',padding='post')
	object_right_idx1 = pad_sequences(object_right_idx, maxlen = num_max_context_len,truncating = 'post',padding='post')
	##
	x = [context_word_idx1, \
		subject_left_idx1,\
		subject_right_idx1,\
		object_left_idx1,\
		object_right_idx1]
	###
	print('prepare the label')
	label = np.array(data.select('label').collect()).reshape((num_text,))
	y = to_categorical(label)
	##
	print('number of total samples of each class')
	print(np.sum(y,0))
	idx_negative = np.where(y[:,0] == 1)[0]
	idx_positive = np.where(y[:,0] != 1)[0]
	if num_train is None:
		num_train = num_text
	else:
		num_train = num_text if num_text <= num_train \
			else num_train
	np.random.shuffle(idx_negative)
	idx_train = np.concatenate((idx_positive, \
		idx_negative[0:num_train-len(idx_positive)]), \
		axis=0)
	x_train = [context_word_idx1[idx_train], \
		subject_left_idx1[idx_train],\
		subject_right_idx1[idx_train],\
		object_left_idx1[idx_train],\
		object_right_idx1[idx_train]]
	y_train = y[idx_train]
	#calculate the weights of the positive 
	num_sample_per_class = np.sum(y_train, 0)
	weight_1 = positive_weight \
		if positive_weight is not None else \
		(num_sample_per_class[0]/np.sum(num_sample_per_class[1:]))\
		*positive_weight_factor
	print('weight of non-negative class:\t', weight_1)
	#train the model
	model = train_subject_object_context_model(\
		x_train, y_train, \
		positive_weight = weight_1, \
		model_file = model_file, \
		num_max_context_len = num_max_context_len,\
		gpus = gpus,\
		epochs = epochs)
	#perform prediction and save the prediction results
	y_score = model.predict(x)
	##
	df_list = \
		[[int(label1),\
		int(preidction), \
		score, \
		context_word_idx1] \
		for label1, preidction, score, \
		context_word_idx1 in \
		zip(np.argmax(y, axis=1).tolist(),\
		np.argmax(y_score, axis=1).tolist(),\
		np.max(y_score, axis=1).tolist(),\
		context_word_idx.tolist()\
		)]
	df_schema = StructType([StructField('label', LongType()),\
		StructField('prediction', LongType()),\
		StructField('score', DoubleType()),\
		StructField('context_word_idx', ArrayType(LongType()))])
	df = sqlContext.createDataFrame(\
		df_list, schema=df_schema)
	df = df.cache()
	df.registerTempTable('prediction')
	sqlContext.sql(u"""
		SELECT label, prediction, COUNT(*)
		FROM prediction
		GROUP BY label, prediction
		""").show()
	###
	data.registerTempTable('data')
	if output_json_prediction is not None \
		or output_json_recommend is not None:		
		prediction_df = sqlContext.sql(u"""
			SELECT DISTINCT 
			context_word_idx, 
			score, prediction
			FROM prediction
			""")
		prediction_df = prediction_df.cache()
		prediction_df.registerTempTable('prediction')
	##save the prediction results
	if output_json_prediction is not None:
		output_df = sqlContext.sql(u"""
			SELECT data.*,
			prediction.prediction, prediction.score
			FROM data
			LEFT JOIN prediction
			ON prediction.context_word_idx
			= data.context_word_idx 
			""").drop('subject_word_idx')\
			.drop('subject_left_idx')\
			.drop('subject_right_idx')\
			.drop('object_word_idx')\
			.drop('object_left_idx')\
			.drop('object_right_idx')\
			.drop('context_word_idx')
		###save the results
		print('saving prediction results to '+output_json_prediction)
		output_df.write.mode('Overwrite').json(output_json_prediction)
		print('outputs saved to %s'%(output_json_prediction))
	##
	if output_json_recommend is not None:
		recommend_df = sqlContext.sql(u"""
			SELECT data.text, 
			data.text_subject_object_context
			FROM data
			LEFT JOIN prediction
			ON prediction.context_word_idx
			= data.context_word_idx 
			WHERE label = 0 AND prediction != 0
			ORDER BY score DESC 
			LIMIT """+str(num_recommend))
		###save the results
		print('saving recommended positives to '+output_json_recommend)
		recommend_df.write.mode('Overwrite').json(output_json_recommend)
		print('recommended positives to %s'%(output_json_recommend))
	print('running time: '+str(time.time() - start_time)\
		+' seconds')
	return model

################pami_dl_train.py###############