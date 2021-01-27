#! /usr/bin/env python3.7

import findspark
findspark.init("/usr/lib/spark-current")
if __package__ is None  or name__ == '__main__':
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import pyspark
# PyArrow compatibility https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#compatibility-setting-for-pyarrow--0150-and-spark-23x-24x
conf = pyspark.SparkConf().setAppName("Spark DQR App").setExecutorEnv(
    'ARROW_PRE_0_15_IPC_FORMAT', '1')
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "true")

spark.sparkContext.addPyFile("dqr.zip")

# System functions
import os, sys, time
from datetime import timedelta

from math import ceil
import pickle
import numpy as np
import pandas as pd
import string
from math import ceil

# Spark functions
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType, monotonically_increasing_id, log

from pyspark.ml.classification import LogisticRegression as SLogisticRegression  # do not mixed with sklearn's logisticregression
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler, StandardScaler

# dlsa functions
from dlsa.dlsa import dlsa, dlsa_mapred  #, dlsa_r
from dlsa.models import simulate_logistic, logistic_model
from dlsa.model_eval import logistic_model_eval_sdf
from dlsa.sdummies import get_sdummies
from dlsa.utils import clean_airlinedata, insert_partition_id_pdf
from dlsa.utils_spark import convert_schema

# https://docs.azuredatabricks.net/spark/latest/spark-sql/udf-python-pandas.html#setting-arrow-batch-size
# spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 10000) # default

# spark.conf.set("spark.sql.shuffle.partitions", 10)
# print(spark.conf.get("spark.sql.shuffle.partitions"))

##----------------------------------------------------------------------------------------
## SETTINGS
##----------------------------------------------------------------------------------------

# General  settings
#-----------------------------------------------------------------------------------------
using_data = "real_hdfs"  # ["simulated_pdf", "real_pdf", "real_hdfs"
partition_method = "systematic"
model_saved_file_name = '~/running/dqr_model_' + time.strftime(
    "%Y-%m-%d-%H:%M:%S", time.localtime()) + '.pkl'

# If save data descriptive statistics
data_info_path = {
    'save': True,
    'path': "~/running/data/used_cars_data/data_info.csv"
}

# Model settings
#-----------------------------------------------------------------------------------------
fit_intercept = True
# fit_algorithms = ['dlsa_logistic', 'spark_logistic']
fit_algorithms = ['dqr']
# fit_algorithms = ['spark_logistic']

#  Settings for using real data
#-----------------------------------------------------------------------------------------
if using_data in ["real_hdfs"]:
#-----------------------------------------------------------------------------------------
    file_path = ['/data/used_cars_data_clean.csv']  # HDFS file

    usecols_x = [ 'mileage', 'year', 'exterior_color']

    schema_sdf = StructType([ StructField('vin', StringType(), True),
                              StructField('back_legroom', DoubleType(), True),
                              StructField('body_type', StringType(), True),
                              StructField('city', StringType(), True),
                              StructField('city_fuel_economy', DoubleType(), True),
                              StructField('daysonmarket', DoubleType(), True),
                              StructField('dealer_zip', DoubleType(), True),
                              StructField('engine_cylinders', StringType(), True),
                              StructField('engine_displacement', DoubleType(), True),
                              StructField('engine_type', StringType(), True),
                              StructField('exterior_color', StringType(), True),
                              StructField('franchise_dealer', StringType(), True),
                              StructField('front_legroom', DoubleType(), True),
                              StructField('fuel_tank_volume', DoubleType(), True),
                              StructField('fuel_type', StringType(), True),
                              StructField('has_accidents', StringType(), True),
                              StructField('height', DoubleType(), True),
                              StructField('highway_fuel_economy', DoubleType(), True),
                              StructField('horsepower', DoubleType(), True),
                              StructField('interior_color', StringType(), True),
                              StructField('isCab', StringType(), True),
                              StructField('latitude', DoubleType(), True),
                              StructField('length', DoubleType(), True),
                              StructField('listed_date', DoubleType(), True),
                              StructField('listing_color', StringType(), True),
                              StructField('listing_id', StringType(), True),
                              StructField('longitude', DoubleType(), True),
                              StructField('major_options', StringType(), True),
                              StructField('make_name', StringType(), True),
                              StructField('maximum_seating', DoubleType(), True),
                              StructField('mileage', DoubleType(), True),
                              StructField('model_name', StringType(), True),
                              StructField('owner_count', DoubleType(), True),
                              StructField('power', DoubleType(), True),
                              StructField('price', DoubleType(), True),
                              StructField('savings_amount', DoubleType(), True),
                              StructField('seller_rating', DoubleType(), True),
                              StructField('sp_id', StringType(), True),
                              StructField('sp_name', StringType(), True),
                              StructField('torque', DoubleType(), True),
                              StructField('transmission', StringType(), True),
                              StructField('transmission_display', StringType(), True),
                              StructField('trimId', StringType(), True),
                              StructField('trim_name', StringType(), True),
                              StructField('wheel_system', StringType(), True),
                              StructField('wheelbase', DoubleType(), True),
                              StructField('width', DoubleType(), True),
                              StructField('year', DoubleType(), True) ])

    dummy_info_path = {
        # 'save': True,  # If False, load it from the path
        'save': False,  # If False, load it from the path
        'path': "~/running/data/used_cars_data/dummy_info.pkl"
        # 'path': "~/running/data/airdelay/dummy_info.pkl"
    }

    if dummy_info_path["save"] is True:
        dummy_info = []
        print("Create dummy and information and save it!")
    else:
        dummy_info = pickle.load(
            open(os.path.expanduser(dummy_info_path["path"]), "rb"))


    dummy_columns = ['exterior_color', 'has_accidents']
    # Dummy factors to drop as the baseline when fitting the intercept
    if fit_intercept:
        dummy_factors_baseline = ['Month_1', 'DayOfWeek_1', 'UniqueCarrier_000_OTHERS',
                                  'Origin_000_OTHERS', 'Dest_000_OTHERS']
    else:
        dummy_factors_baseline = []

    dummy_keep_top = [0.8, 0.8]

    n_files = len(file_path)
    partition_num_sub = []
    max_sample_size_per_sdf = 100000  # No effect with `real_hdfs` data
    sample_size_per_partition = 100000

    Y_name = "price"
    sample_size_sub = []
    memsize_sub = []

# Read or load data chunks into pandas
#-----------------------------------------------------------------------------------------
time_2sdf_sub = []
time_repartition_sub = []

loop_counter = 0
for file_no_i in range(n_files):
    tic_2sdf = time.perf_counter()

    ## Using HDFS data
    ## ------------------------------
    isub = 0  # fixed, never changed

    # Read HDFS to Spark DataFrame and clean NAs
    data_sdf_i = spark.read.csv(file_path[file_no_i],
                                header=True,
                                schema=schema_sdf)
    data_sdf_i = data_sdf_i.select(usecols_x + [Y_name])
    data_sdf_i = data_sdf_i.dropna()

    # Define or transform response variable. Or use
    # https://spark.apache.org/docs/latest/ml-features.html#binarizer
    data_sdf_i = data_sdf_i.withColumn(
        Y_name,
        F.when(data_sdf_i[Y_name] > 0, 1).otherwise(0))

    sample_size_sub.append(data_sdf_i.count())
    partition_num_sub.append(
        ceil(sample_size_sub[file_no_i] / sample_size_per_partition))

    ## Add partition ID
    data_sdf_i = data_sdf_i.withColumn(
        "partition_id",
        monotonically_increasing_id() % partition_num_sub[file_no_i])

    ## Create dummy variables We could do it either directly with
    ## https://stackoverflow.com/questions/35879372/pyspark-matrix-with-dummy-variables
    ## or we do it within grouped dlsa (default)

##----------------------------------------------------------------------------------------
## MODEL FITTING ON PARTITIONED DATA
##----------------------------------------------------------------------------------------
# Split the process into small subs if reading a real big DataFrame which my cause
    # Load or Create descriptive statistics used for standardizing data.
    if data_info_path["save"] is True:
        # descriptive statistics
        data_info = data_sdf_i.describe().toPandas()
        data_info.to_csv(os.path.expanduser(data_info_path["path"]),
                         index=False)
        print("Descriptive statistics for data are saved to:\t" +
              data_info_path["path"])
    else:
        # Load data info
        data_info = pd.read_csv(os.path.expanduser(data_info_path["path"]))
        print("Descriptive statistics for data are loaded from file:\t" +
              data_info_path["path"])

    # Independent fit chunked data with UDF.
    if 'dqr' in fit_algorithms:
        tic_repartition = time.perf_counter()
        data_sdf_i = data_sdf_i.repartition(partition_num_sub[file_no_i],
                                            "partition_id")
        time_repartition_sub.append(time.perf_counter() - tic_repartition)

        ## Register a user defined function via the Pandas UDF
        schema_beta = StructType([
            StructField('par_id', IntegerType(), True),
            StructField('coef', DoubleType(), True),
            StructField('Sig_invMcoef', DoubleType(), True)
        ] + convert_schema(usecols_x, dummy_info, fit_intercept, dummy_factors_baseline))

        @pandas_udf(schema_beta, PandasUDFType.GROUPED_MAP)
        def logistic_model_udf(sample_df):
            return logistic_model(sample_df=sample_df,
                                  Y_name=Y_name,
                                  fit_intercept=fit_intercept,
                                  dummy_info=dummy_info,
                                  dummy_factors_baseline=dummy_factors_baseline,
                                  data_info=data_info)

        # pdb.set_trace()
        # partition the data and run the UDF
        model_mapped_sdf_i = data_sdf_i.groupby("partition_id").apply(logistic_model_udf)

        # Union all sequential mapped results.
        if file_no_i == 0 and isub == 0:
            model_mapped_sdf = model_mapped_sdf_i
            # memsize_sub = sys.getsizeof(data_pdf_i)
        else:
            model_mapped_sdf = model_mapped_sdf.unionAll(model_mapped_sdf_i)

##----------------------------------------------------------------------------------------
## AGGREGATING THE MODEL ESTIMATES
##----------------------------------------------------------------------------------------
if 'dqr' in fit_algorithms:
    # sample_size=model_mapped_sdf.count()
    sample_size = sum(sample_size_sub)

    # Obtain Sig_inv and beta
    tic_mapred = time.perf_counter()
    Sig_inv_beta = dlsa_mapred(model_mapped_sdf)
    time_mapred = time.perf_counter() - tic_mapred

    tic_dlsa = time.perf_counter()
    out_dlsa = dlsa(Sig_inv_=Sig_inv_beta.iloc[:, 2:],
                    beta_=Sig_inv_beta["beta_byOLS"],
                    sample_size=sample_size,
                    fit_intercept=fit_intercept)

    time_dlsa = time.perf_counter() - tic_dlsa
    ##----------------------------------------------------------------------------------------
    ## Model Evaluation
    ##----------------------------------------------------------------------------------------
    tic_model_eval = time.perf_counter()

    out_par = out_dlsa
    out_par["beta_byOLS"] = Sig_inv_beta["beta_byOLS"]
    out_par["beta_byONESHOT"] = Sig_inv_beta["beta_byONESHOT"]

    out_model_eval = logistic_model_eval_sdf(data_sdf=data_sdf_i,
                                             par=out_par,
                                             fit_intercept=fit_intercept,
                                             Y_name=Y_name,
                                             dummy_info=dummy_info,
                                             dummy_factors_baseline=dummy_factors_baseline,
                                             data_info=data_info)

    time_model_eval = time.perf_counter() - tic_model_eval
    ##----------------------------------------------------------------------------------------
    ## PRINT OUTPUT
    ##----------------------------------------------------------------------------------------
    memsize_total = sum(memsize_sub)
    partition_num = sum(partition_num_sub)
    time_repartition = sum(time_repartition_sub)
    # time_2sdf = sum(time_2sdf_sub)
    # sample_size_per_partition = sample_size / partition_num

    out_time = pd.DataFrame(
        {
            "sample_size": sample_size,
            "sample_size_per_partition": sample_size_per_partition,
            "n_par": len(schema_beta) - 3,
            "partition_num": partition_num,
            "memsize_total": memsize_total,
            # "time_2sdf": time_2sdf,
            "time_repartition": time_repartition,
            "time_mapred": time_mapred,
            "time_dlsa": time_dlsa,
            "time_model_eval": time_model_eval
        },
        index=[0])

    # save the model to pickle, use pd.read_pickle("test.pkl") to load it.
    # out_dlas.to_pickle("test.pkl")
    out = [Sig_inv_beta, out_dlsa, out_par, out_model_eval, out_time]
    pickle.dump(out, open(os.path.expanduser(model_saved_file_name), 'wb'))
    print("Model results are saved to:\t" + model_saved_file_name)

    # print(", ".join(format(x, "10.2f") for x in out_time))
    print("\nModel Summary:\n")
    print(out_time.to_string(index=False))

    print("\nModel Evaluation:")
    print("\tlog likelihood:\n")
    print(out_model_eval.to_string(index=False))

    print("\nDLSA Coefficients:\n")
    print(out_par.to_string())

    # Print results
    print(lrModel.intercept)
    print(lrModel.coefficients)
    logLike.show()


elif 'qr_spark' in fit_algorithms:
    logging.warning("not implemented!")
