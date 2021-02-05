#! /usr/bin/env python3.7

import findspark
findspark.init("/usr/lib/spark-current")
# if __package__ is None  or __name__ == '__main__':
#     from os import sys, path
#     sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import pyspark
# PyArrow compatibility https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#compatibility-setting-for-pyarrow--0150-and-spark-23x-24x
conf = pyspark.SparkConf().setAppName("Spark DQR App").setExecutorEnv(
    'ARROW_PRE_0_15_IPC_FORMAT', '1')
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN") # "DEBUG", "ERROR"

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "true")

spark.sparkContext.addPyFile("dqr.zip")
from dqr.sdummies import get_sdummies
from dqr.math import XTX
from dqr.utils_spark import spark_onehot_to_pd_dense
from dqr.models import qr_asymptotic_comp

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
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType, monotonically_increasing_id

# dqr
from statsmodels.regression.quantile_regression import QuantReg

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
    "%Y-%m-%d-%H.%M.%S", time.localtime()) + '.pkl'

# If save data descriptive statistics
data_info_path = {
    'save': True,
    'path': "~/running/data/used_cars_data/data_info.csv"
}

# Model settings
#-----------------------------------------------------------------------------------------
fit_algorithms = ['dqr']

dqr_conf = {
    'fit_intercept': True,
    'pilot_sampler': 0.01,
    'quantile': 0.25
}

#  Settings for using real data
#-----------------------------------------------------------------------------------------
if using_data in ["real_hdfs"]:
#-----------------------------------------------------------------------------------------
    file_path = ['/data/used_cars_data_clean.csv']  # HDFS file

    col_names_x = [ 'mileage', 'year']
    Y_name = "price"
    # col_names_dummy = []
    col_names_dummy = ['exterior_color']#, 'fuel_type']
    dummy_keep_top = [0.5]#, 0.9]

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
        'save': True,  # If False, load it from the path
        'path': "~/running/data/used_cars_data/dummy_info.pkl"
    }

    # Dummy factors to drop as the baseline when fitting the intercept
    # if dqr_conf['fit_intercept']:
    #     dummy_factors_baseline = ['Month_1', 'DayOfWeek_1', 'UniqueCarrier_000_OTHERS',
    #                               'Origin_000_OTHERS', 'Dest_000_OTHERS']
    # else:
    #     dummy_factors_baseline = []

    sample_size_per_partition = 100000


# Read or load data chunks into pandas
#-----------------------------------------------------------------------------------------
n_files = len(file_path)
partition_num_sub = []
sample_size_sub = []
memsize_sub = []
time_2sdf_sub = []
time_repartition_sub = []

for file_no_i in range(n_files):
    tic_2sdf = time.perf_counter()

    ## Using HDFS data

    # Read HDFS to Spark DataFrame and clean NAs
    data_sdf_i = spark.read.csv(file_path[file_no_i],
                                header=True,
                                schema=schema_sdf)
    XY_sdf_i = data_sdf_i.select(col_names_x + [Y_name] + col_names_dummy)
    XY_sdf_i = XY_sdf_i.dropna()

    sample_size_sub.append(XY_sdf_i.count())
    partition_num_sub.append(
        ceil(sample_size_sub[file_no_i] / sample_size_per_partition))

    ## Add partition ID
    XY_sdf_i = XY_sdf_i.withColumn(
        "partition_id",
        monotonically_increasing_id() % partition_num_sub[file_no_i])

##----------------------------------------------------------------------------------------
## MODEL FITTING ON PARTITIONED DATA
##----------------------------------------------------------------------------------------
# Split the process into small subs if reading a real big DataFrame which my cause
    # Load or Create descriptive statistics used for standardizing data.
    if data_info_path["save"] is True:
        # descriptive statistics
        data_info = XY_sdf_i.describe().toPandas()
        data_info.to_csv(os.path.expanduser(data_info_path["path"]),
                         index=False)
        print("Descriptive statistics for data are saved to:\t" +
              data_info_path["path"])
    else:
        # Load data info
        data_info = pd.read_csv(os.path.expanduser(data_info_path["path"]))
        print("Descriptive statistics for data are loaded from file:\t" +
              data_info_path["path"])

    # Obtain ONEHOT encoding
    if len(col_names_dummy) > 0:
        # Retrieve dummy information
        if dummy_info_path["save"] is True:
            dummy_info = []
            print("Dummy and information will be created and saved to disk!")
        else:
            dummy_info = pickle.load(
                open(os.path.expanduser(dummy_info_path["path"]), "rb"))

        XY_sdf_i, dummy_info = get_sdummies(sdf=XY_sdf_i,
                                              keep_top=dummy_keep_top,
                                              replace_with="000_OTHERS",
                                              dummy_columns=col_names_dummy,
                                              dummy_info=dummy_info,
                                              dropLast=dqr_conf['fit_intercept'])

    # Independent fit chunked data with UDF.
    if 'dqr' in fit_algorithms:

        # Step 0: Data transformation

        # Standardize non-categorical columns
        sample_size = int(data_info.iloc[0,1])
        for non_dummy_col in col_names_x:
            XY_sdf_i.withColumn(non_dummy_col,(F.col(non_dummy_col)-data_info[non_dummy_col][1])/data_info[non_dummy_col][2])

        # Add the intercept column
        if dqr_conf['fit_intercept']:
            col_names_x.insert(0, 'Intercept')
            XY_sdf_i = XY_sdf_i.withColumn('Intercept', F.lit(1))

        # Step 1: Pilot Sampler
        XY_pilot_sdf_i = XY_sdf_i.sample(withReplacement=False,
                                             fraction=dqr_conf['pilot_sampler'])

        XY_pilot_pdf_i = XY_pilot_sdf_i.toPandas()  # Send to master

        # Run a pilot model with sampled data from Spark. Note: statsmodels does not support sparse matrix, TERRIBLE!
        if len(col_names_dummy) > 0:
            onehot_column_names = ['_'.join([key, values_i])
                                  for key in dummy_info['factor_selected_names'].keys()
                                  for values_i in dummy_info['factor_selected_names'][key]]
            XY_pilot_pdf_i = spark_onehot_to_pd_dense(pdf=XY_pilot_pdf_i,
                                                        onehot_column='features_ONEHOT',
                                                        onehot_column_names=onehot_column_names,
                                                        onehot_column_is_sparse=False)
        else:
            onehot_column_names = []

        column_names_x_full = col_names_x + onehot_column_names
        dqr_pilot = QuantReg(endog=XY_pilot_pdf_i[Y_name],
                             exog=(XY_pilot_pdf_i[column_names_x_full]).astype(float))
        dqr_pilot_res = dqr_pilot.fit(q=dqr_conf['quantile'])

        dqr_pilot_par = {
            'bandwidth': dqr_pilot_res.bandwidth,
            'params': dqr_pilot_res.params
        }

        # Step 2: Updating QR components
        tic_repartition = time.perf_counter()
        XY_sdf_i = XY_sdf_i.repartition(partition_num_sub[file_no_i],
                                            "partition_id")
        time_repartition_sub.append(time.perf_counter() - tic_repartition)

        ## Register a user defined function via the Pandas UDF
        Xdim = len(column_names_x_full) #  - 2 # with Y, partition_id
        XTX_tril_len = int(Xdim * (Xdim + 1) / 2)
        schema_XTX = StructType([StructField(str(i), DoubleType(), True)
                                 for i in range(XTX_tril_len)])
        @pandas_udf(schema_XTX, PandasUDFType.GROUPED_MAP)
        def XTX_udf(pdf):
            # Convert Spark ONEHOT encoded column into Pandas dense DataFrame
            if len(col_names_dummy) > 0:
                pdf_dense = spark_onehot_to_pd_dense(
                    pdf=pdf,
                    onehot_column='features_ONEHOT',
                    onehot_column_names=onehot_column_names,
                    onehot_column_is_sparse=False)
            else:
                pdf_dense = pdf
            return XTX(X=pdf_dense[column_names_x_full])

        # partition the data and run the UDF
        XTX_sdf = XY_sdf_i.groupby("partition_id").apply(XTX_udf)

        ## Register a user defined function via the Pandas UDF
        schema_qr_comp = StructType([StructField(str(i), DoubleType(), True)
                                     for i in range(Xdim + 1)])
        @pandas_udf(schema_qr_comp, PandasUDFType.GROUPED_MAP)
        def qr_asymptotic_comp_udf(pdf):
            if len(col_names_dummy) > 0:
                pdf_dense = spark_onehot_to_pd_dense(
                    pdf=pdf,
                    onehot_column='features_ONEHOT',
                    onehot_column_names=onehot_column_names,
                    onehot_column_is_sparse=False)
            else:
                pdf_dense = pdf
            return qr_asymptotic_comp(
                pdf=pdf_dense[column_names_x_full + [Y_name]],
                # pdf=X_sdf.drop('partition_id', axis=1),
                beta0=dqr_pilot_res.params,
                quantile=dqr_conf['quantile'],
                bandwidth=dqr_pilot_res.bandwidth,
                Y_name=Y_name)

        # partition the data and run the UDF
        qr_comp_sdf = XY_sdf_i.groupby("partition_id").apply(qr_asymptotic_comp_udf)

        # Step 3: Merge and combine
        qr_comp = qr_comp_sdf.toPandas().to_numpy()
        XTX = XTX_sdf.toPandas().to_numpy()

        XTX_tril = np.sum(XTX,axis=0)
        XTX_full = np.zeros((Xdim, Xdim))
        XTX_full[np.tril_indices(XTX_full.shape[0], k=0)] = XTX_tril
        XTX_full = XTX_full + XTX_full.T - np.diag(np.diag(XTX_full))
        XTX_inv = np.linalg.inv(XTX_full)  ## p-by-p

        # Step 4: One-step updating coefficients
        qr_comp_sum = np.sum(qr_comp, axis=0)
        f_hat_inv = sample_size * dqr_pilot_res.bandwidth / qr_comp_sum[-1]
        out_beta = dqr_pilot_res.params + f_hat_inv * XTX_inv.dot(qr_comp_sum[:-1])

    ##----------------------------------------------------------------------------------------
    ## PRINT OUTPUT
    ##----------------------------------------------------------------------------------------
    out = {'dqr_pilot_res': dqr_pilot_res,
           'out_beta': out_beta
    }
    pickle.dump(out, open(os.path.expanduser(model_saved_file_name), 'wb'))

    print("Model results are saved to:\t" + model_saved_file_name)

    print("\nModel Summary:\n")
    # print(out_time.to_string(index=False))

    print("\nModel Evaluation:")
    print("\tlog likelihood:\n")

    print("\nDQR Coefficients:\n")
    # print(out_par.to_string())
