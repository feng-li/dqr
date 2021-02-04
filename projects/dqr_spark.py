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

# spark.sparkContext.addPyFile("dqr.zip")

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
fit_intercept = True
# fit_algorithms = ['dlsa_logistic', 'spark_logistic']
fit_algorithms = ['dqr']

dqr_conf = {
    'fit_intercept': False,
    'pilot_sampler': 0.01,
    'quantile': 0.25
}
# fit_algorithms = ['spark_logistic']

#  Settings for using real data
#-----------------------------------------------------------------------------------------
if using_data in ["real_hdfs"]:
#-----------------------------------------------------------------------------------------
    file_path = ['/data/used_cars_data_clean.csv']  # HDFS file

    usecols_x = [ 'mileage', 'year', 'exterior_color', 'fuel_type']
    Y_name = "price"
    dummy_columns = ['exterior_color', 'fuel_type']
    dummy_keep_top = [0.5, 0.9]

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

    if dummy_info_path["save"] is True:
        dummy_info = []
        print("Dummy and information will be created and saved to disk!")
    else:
        dummy_info = pickle.load(
            open(os.path.expanduser(dummy_info_path["path"]), "rb"))

    # Dummy factors to drop as the baseline when fitting the intercept
    if fit_intercept:
        dummy_factors_baseline = ['Month_1', 'DayOfWeek_1', 'UniqueCarrier_000_OTHERS',
                                  'Origin_000_OTHERS', 'Dest_000_OTHERS']
    else:
        dummy_factors_baseline = []

    n_files = len(file_path)
    partition_num_sub = []
    sample_size_per_partition = 100000

    sample_size_sub = []
    memsize_sub = []

# Read or load data chunks into pandas
#-----------------------------------------------------------------------------------------
time_2sdf_sub = []
time_repartition_sub = []

loop_counter = 0

file_no_i = 0
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

    sample_size_sub.append(data_sdf_i.count())
    partition_num_sub.append(
        ceil(sample_size_sub[file_no_i] / sample_size_per_partition))

    ## Add partition ID
    data_sdf_i = data_sdf_i.withColumn(
        "partition_id",
        monotonically_increasing_id() % partition_num_sub[file_no_i])

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

    # Obtain ONEHOT encoding
    data_sdf_i, dummy_info = get_sdummies(sdf=data_sdf_i,
                                          keep_top=dummy_keep_top,
                                          replace_with="000_OTHERS",
                                          dummy_columns=dummy_columns,
                                          dummy_info=dummy_info,
                                          dropLast=dqr_conf['fit_intercept'])


    # Independent fit chunked data with UDF.
    if 'dqr' in fit_algorithms:

        # Step 0: Data transformation

        # Step 1: Pilot Sampler
        data_pilot_sdf_i = data_sdf_i.sample(withReplacement=False,
                                             fraction=dqr_conf['pilot_sampler'])
        data_pilot_pdf_i = data_pilot_sdf_i.toPandas()  # Send to master

        # Convert ONEHOT encoded Pandas to a full dense pandas.
        def spark_onehot_to_pd_dense(pdf, onehot_column, onehot_column_names=[]):
            """Convert Pandas DataFrame containing Spark SparseVector encoded column into pandas dense vector

            """
            # pdf = data_pilot_pdf_i
            # column = 'features_ONEHOT'
            features_ONEHOT = pdf[onehot_column].apply(lambda x: x.toArray())
            features_DENSE = features_ONEHOT.explode().values.reshape(
                features_ONEHOT.shape[0], len(features_ONEHOT[0]))
            features_pd = pd.DataFrame(features_DENSE)

            if len(onehot_column_names) != 0:
                features_pd.columns = onehot_column_names

            pdf_dense = pd.concat([pdf.drop(onehot_column,axis=1), features_pd],axis=1)
            return(pdf_dense)

        # Run a pilot model with sampled data from Spark. Note: statsmodels does not support sparse matrix, TERRIBLE!
        onehot_column_names = ['_'.join([key, values_i])
                              for key in dummy_info['factor_selected_names'].keys()
                              for values_i in dummy_info['factor_selected_names'][key]]
        data_pilot_pdf_i = spark_onehot_to_pd_dense(pdf=data_pilot_pdf_i,
                                                    onehot_column='features_ONEHOT',
                                                    onehot_column_names=onehot_column_names)

        data_column_x_names = (list(set(usecols_x) - set(dummy_columns)) + onehot_column_names)[:21]
        dqr_pilot = QuantReg(endog=data_pilot_pdf_i[Y_name],
                             exog=data_pilot_pdf_i[data_column_x_names].astype(float))
        dqr_pilot_res = dqr_pilot.fit(q=dqr_conf['quantile'])

        dqr_pilot_par = {
            'bandwidth': dqr_pilot_res.bandwidth,
            'params': dqr_pilot_res.params
        }

        tic_repartition = time.perf_counter()
        data_sdf_i = data_sdf_i.repartition(partition_num_sub[file_no_i],
                                            "partition_id")
        time_repartition_sub.append(time.perf_counter() - tic_repartition)

        XY_sdf = data_sdf_i # .select(['partition_id', 'mileage', 'year', 'price'])
        sample_size = XY_sdf.count()

        import numpy as np
        import pandas as pd
        def XTX(X):
            """This function calculates X'X where X is an n-by-p  Pandas DataFrame.

            This function employs the factorization that X'X = \sum_{i=1}^n x_i x'_i where
            x is p-by-1 vector. It will firstly make a row-wise calculation and then sum
            over all rows.

            X: Pandas DataFrame

            Return: 1-by-p(p+1)/2 row column Pandas DataFrame which is the lower
            triangular part (including the diagonal elements) of the symmetric matrix X'X.

            """
            # pdf = data_pilot_pdf_i[ ['mileage', 'year', 'price'] ]
            # if len(onehot_column) != 0:

            mat = X.to_numpy()
            n, p = mat.shape
            m = int(p*(p + 1)/2)
            out_n_tril = np.zeros((n, m))
            for i in range(n):
                outer_i = np.outer(mat[i, :], mat[i, :])
                out_n_tril[i, :] = outer_i[np.tril_indices(p)]

            out_np = np.sum(out_n_tril, axis=0)
            out_pd = pd.DataFrame(out_np.reshape(1, m))
            return(out_pd)


        ## Register a user defined function via the Pandas UDF
        Xdim = len(data_column_x_names) #  - 2 # with Y, partition_id
        XTX_tril_len = int(Xdim * (Xdim + 1) / 2)
        schema_XTX = StructType([StructField(i, DoubleType(), True)
                                 for i in range(data_column_x_names)])
        @pandas_udf(schema_XTX, PandasUDFType.GROUPED_MAP)
        def XTX_udf(pdf):
            # Convert Spark ONEHOT encoded column into Pandas dense DataFrame
            pdf_dense = spark_onehot_to_pd_dense(
                pdf=pdf,
                onehot_column='features_ONEHOT',
                onehot_column_names=onehot_column_names)
            return XTX(X=pdf_dense[data_column_x_names])

        # partition the data and run the UDF
        XTX_sdf = XY_sdf.groupby("partition_id").apply(XTX_udf)

        def qr_asymptotic_comp(pdf, beta0, quantile, bandwidth, Y_name):
            """This function calculates the components in the one-step updating estimator
            for quantile regression based on Koenker (2005), Wang et al. (2007), and
            Chen et al. (2019),

            pdf: Pandas DataFrame containing X and Y

            Return: The last element is for the Gaussian kernel component

            """

            Y = pdf[Y_name].to_numpy()
            X = pdf.drop(Y_name, axis=1).to_numpy()

            n, p = X.shape

            Xbeta = X.dot(beta0)
            error = Y - Xbeta  # n-by-1

            I = (error < 0)
            Z = (quantile - I).reshape(n, 1)
            XZ = np.sum(np.multiply(X, Z), axis=0)  # 1-by-p

            # Gaussian Kernel
            K = np.array(np.sum(1 / np.sqrt(2 * np.pi)  * np.exp(-(error / bandwidth) ** 2 / 2)))
            out = pd.DataFrame(np.concatenate([XZ.reshape(1,p), K.reshape(1,1)],axis=1))

            return(out)


        ## Register a user defined function via the Pandas UDF
        schema_qr_comp = StructType([StructField(i, DoubleType(), True)
                                     for i in [Y_name] + data_column_x_names])
        @pandas_udf(schema_qr_comp, PandasUDFType.GROUPED_MAP)
        def qr_asymptotic_comp_udf(pdf):
            pdf_dense = spark_onehot_to_pd_dense(
                pdf=pdf,
                onehot_column='features_ONEHOT',
                onehot_column_names=onehot_column_names)
            return qr_asymptotic_comp(
                pdf=pdf_dense[data_column_x_names + [Y_name]],
                # pdf=X_sdf.drop('partition_id', axis=1),
                beta0=dqr_pilot_res.params,
                quantile=dqr_conf['quantile'],
                bandwidth=dqr_pilot_res.bandwidth,
                Y_name=Y_name)

        # partition the data and run the UDF
        qr_comp_sdf = XY_sdf.groupby("partition_id").apply(qr_asymptotic_comp_udf)

        # Final update
        qr_comp = qr_comp_sdf.toPandas().to_numpy()
        XTX = XTX_sdf.toPandas().to_numpy()

        XTX_tril = np.sum(XTX,axis=0)
        XTX_full = np.zeros((Xdim, Xdim))
        XTX_full[np.tril_indices(XTX_full.shape[0], k=0)] = XTX_tril
        XTX_full = XTX_full + XTX_full.T - np.diag(np.diag(XTX_full))
        XTX_inv = np.linalg.inv(XTX_full)  ## p-by-p

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
    print(out_time.to_string(index=False))

    print("\nModel Evaluation:")
    print("\tlog likelihood:\n")

    print("\nDQR Coefficients:\n")
    print(out_par.to_string())
