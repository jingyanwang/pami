############pami_local_spark_building.py###########
'''
sudo rm ./conf/spark-env.sh
sudo vi ./conf/spark-env.sh
i#!/usr/bin/env bash

export PYSPARK_PYTHON=/usr/local/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3
'''

import re
import os
import time
import random
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

'''
os.system(u"""
export PYSPARK_PYTHON=/usr/local/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3
""")
'''

sc = SparkContext("local", "pami")
sqlContext = SparkSession.builder.getOrCreate()
#print('created sqlContext_local')
############pami_local_spark_building.py###########