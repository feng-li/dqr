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

# dlsa functions
# from dlsa.dlsa import dlsa, dlsa_mapred  #, dlsa_r
# from dlsa.models import simulate_logistic, logistic_model
# from dlsa.model_eval import logistic_model_eval_sdf
# from dlsa.sdummies import get_sdummies
# from dlsa.utils import clean_airlinedata, insert_partition_id_pdf
# from dlsa.utils_spark import convert_schema


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
    'pilot_sampler': 0.01,
    'quantiles': [0.25, 0.5, 0.75]
}
# fit_algorithms = ['spark_logistic']

#  Settings for using real data
#-----------------------------------------------------------------------------------------
if using_data in ["real_hdfs"]:
#-----------------------------------------------------------------------------------------
    file_path = ['/data/used_cars_data_clean.csv']  # HDFS file

    usecols_x = [ 'mileage', 'year', 'exterior_color']
    Y_name = "price"

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

    # Independent fit chunked data with UDF.
    if 'dqr' in fit_algorithms:

        # Step 0: Data transformation

        # Step 1: Pilot Sampler
        data_pilot_sdf_i = data_sdf_i.sample(withReplacement=False,
                                             fraction=dqr_conf['pilot_sampler'])
        data_pilot_pdf_i = data_pilot_sdf_i.toPandas()  # Send to master

        dqr_pilot = QuantReg(endog=data_pilot_pdf_i[Y_name], exog=data_pilot_pdf_i['mileage'])
        dqr_pilot_res = dqr_pilot.fit(q=dqr_conf['quantiles'][0])

        dqr_pilot_par = {
            'bandwidth': dqr_pilot_res.bandwidth,
            'params': dqr_pilot_res.params
        }

        tic_repartition = time.perf_counter()
        data_sdf_i = data_sdf_i.repartition(partition_num_sub[file_no_i],
                                            "partition_id")
        time_repartition_sub.append(time.perf_counter() - tic_repartition)

        X_sdf = data_sdf_i.select(['partition_id', 'mileage', 'year', 'price'])

        import numpy as np
        import pandas as pd
        def XTX(pdf):
            """This function calculates X'X where X is an n-by-p matrix for a given Pandas DataFrame.

            This function employs the factorization that X'X = \sum_{i=1}^n x_i x'_i where
            x is p-by-1 vector. It will firstly make a row-wise calculation and then sum
            over all rows.

            pdf: Pandas DataFrame

            Return: 1-by-p(p+1)/2 row column Pandas DataFrame which is the lower
            triangular part (including the diagonal elements) of the symmetric matrix X'X.

            """
            # pdf = data_pilot_pdf_i[ ['mileage', 'year', 'price'] ]

            mat = pdf.to_numpy()
            n, p = mat.shape
            m = int(p*(p + 1)/2)
            out_n_tril = np.zeros((n, m))
            for i in range(n):
                outer_i = np.outer(mat[i, :], mat[i, :])
                out_n_tril[i, :] = outer_i[np.tril_indices(p)]

            out_np = np.sum(out_n, axis=0)
            out_pd = pd.DataFrame(out_np.reshape(1, m))
            return(out_pd)


        ## Register a user defined function via the Pandas UDF
        Xdim = len(X_sdf.columns) - 1 # with partition_id
        XTX_tril_len = int(Xdim * (Xdim + 1) / 2)
        schema_XTX = StructType([StructField(str(i), DoubleType(), True)
                                 for i in range(XTX_tril_len)])
        @pandas_udf(schema_XTX, PandasUDFType.GROUPED_MAP)
        def XTX_udf(X_sdf):
            return XTX(X_sdf.drop('partition_id', axis=1))

        # partition the data and run the UDF
        model_mapped_sdf_i = X_sdf.groupby("partition_id").apply(XTX_udf)





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
