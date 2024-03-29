#! /usr/bin/env python3.7

# Only use for interactive mode
import sys, pathlib
if hasattr(sys, 'ps1'):
    import os, findspark
    findspark.init("/usr/lib/spark-current")
    libdir = pathlib.Path(os.getcwd()).parent
    sys.path.append(libdir)

import pyspark
# PyArrow compatibility https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#compatibility-setting-for-pyarrow--0150-and-spark-23x-24x
conf = pyspark.SparkConf().setAppName("Spark DQR App").setExecutorEnv(
    'ARROW_PRE_0_15_IPC_FORMAT', '1')
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN") # "DEBUG", "ERROR"

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "true")

# Spark functions
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, pandas_udf

# dqr
# Or use `spark-submit --py-files dqr-xx.yy.zip dqr_spark.py`
import os, glob, pathlib
spark.sparkContext.addPyFile('dqr.zip')
# spark.sparkContext.addPyFile(max(glob.iglob('../dist/dqr*.zip'), key=os.path.getctime))

from dqr.sdummies import get_sdummies
from dqr.math import XTX
from dqr.utils_spark import spark_onehot_to_pd_dense
from dqr.models import qr_asymptotic_comp
from dqr.memory import commcost_estimate
from statsmodels.regression.quantile_regression import QuantReg
from scipy import stats

# System functions
import sys, time
from math import ceil
import pickle, json, pprint
import numpy as np
import pandas as pd

# https://docs.azuredatabricks.net/spark/latest/spark-sql/udf-python-pandas.html#setting-arrow-batch-size
# spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 10000) # default

# spark.conf.set("spark.sql.shuffle.partitions", 10)
# print(spark.conf.get("spark.sql.shuffle.partitions"))

# ---------------------------------------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------------------------------------

# General  settings
# ----------------------------------------------------------------------------------------
using_data = "real_hdfs"  # ["simulated_pdf", "real_pdf", "real_hdfs"
partition_method = "systematic"
model_saved_file_name = '~/running/dqr_model_' + '_'.join(sys.argv[1:]) + '_' + time.strftime("%Y%m%d-%H.%M.%S", time.localtime())

# If save data descriptive statistics
data_info_path = {
    'save': False,
    'path': "~/running/data/used_cars_data/data_info.csv"
}

# Model settings
# ----------------------------------------------------------------------------------------
fit_algorithms = ['dqr']

dqr_conf = {
    'fit_intercept': True,
    'pilot_sampler': 0.01,
    # 'quantile': 0.025,
    'quantile': float(sys.argv[1])
}

#  Settings for using real data
# ----------------------------------------------------------------------------------------
if using_data in ["real_hdfs"]:
# ----------------------------------------------------------------------------------------
    file_path = ['/data/used_cars_data_clean.csv']  # HDFS file

    Y_name = "price"
    X_names = [ 'back_legroom', 'city_fuel_economy', 'daysonmarket',
                'engine_displacement', 'front_legroom', 'fuel_tank_volume', 'height',
                'highway_fuel_economy', 'horsepower', 'length', 'mileage',
                'seller_rating', 'wheelbase', 'width', 'year' ]

    dummy_names = ['body_type', 'engine_cylinders', 'exterior_color', 'franchise_dealer',
                   'fuel_type', 'has_accidents', 'interior_color', 'isCab', 'make_name',
                   'maximum_seating', 'owner_count', 'transmission_display',
                   'wheel_system']

    # dummy_names = []  # no dummy variable used
    dummy_keep_top = [0.5] * len(dummy_names) #, 0.9]
    dummy_keep_top[2] = 0.3 # exterior_color

    data_processing = {'X': 'standardize', 'Y': 'log'}

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
        # 'save': False,  # If False, load it from the path
        'save': True,  # If False, load it from the path
        'path': "~/running/data/used_cars_data/dummy_info.pkl"
    }

    # Dummy factors to drop as the baseline when fitting the intercept
    # if dqr_conf['fit_intercept']:
    #     dummy_factors_baseline = ['Month_1', 'DayOfWeek_1', 'UniqueCarrier_000_OTHERS',
    #                               'Origin_000_OTHERS', 'Dest_000_OTHERS']
    # else:
    #     dummy_factors_baseline = []

    sample_size_per_partition = 10000

    commcost = False
# Read or load data chunks into pandas
# ----------------------------------------------------------------------------------------
n_files = len(file_path)
partition_num_sub = []
sample_size_sub = []
memsize_sub = []
time_2sdf_sub = []
time_repartition_sub = []

for file_no_i in range(n_files):
    tic_2sdf = time.perf_counter()

    # Using HDFS data

    # Read HDFS to Spark DataFrame and clean NAs
    data_sdf_i = spark.read.csv(file_path[file_no_i],
                                header=True,
                                schema=schema_sdf)

    if commcost:
        out_commcost = commcost_estimate(sdf=data_sdf_i,
                                         fractions=[x/10 for x in range(10)][1:])
    else:
        out_commcost = []

    XY_sdf_i = data_sdf_i.select(X_names + [Y_name] + dummy_names)
    XY_sdf_i = XY_sdf_i.dropna()

    sample_size_sub.append(XY_sdf_i.count())
    partition_num_sub.append(
        ceil(sample_size_sub[file_no_i] / sample_size_per_partition))

    # Add partition ID
    XY_sdf_i = XY_sdf_i.withColumn(
        "partition_id",
        F.monotonically_increasing_id() % partition_num_sub[file_no_i])

# ----------------------------------------------------------------------------------------
# MODEL FITTING ON PARTITIONED DATA
# ----------------------------------------------------------------------------------------
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
        print("Descriptive statistics for data loaded from file:\t" +
              data_info_path["path"])

    # Obtain ONEHOT encoding
    if len(dummy_names) > 0:
        # Retrieve dummy information
        if dummy_info_path["save"] is True:
            dummy_info = []
        else:
            dummy_info = pickle.load(
                open(os.path.expanduser(dummy_info_path["path"]), "rb"))
            print("Dummy information loaded from file:\t" +
                  dummy_info_path["path"])

        XY_sdf_i, dummy_info = get_sdummies(sdf=XY_sdf_i,
                                              keep_top=dummy_keep_top,
                                              replace_with="000_OTHERS",
                                              dummy_columns=dummy_names,
                                              dummy_info=dummy_info,
                                              dropLast=dqr_conf['fit_intercept'])

        if dummy_info_path["save"] is True:
            pickle.dump(dummy_info, open(os.path.expanduser(dummy_info_path["path"]), 'wb'))
            print("Dummy information created and saved to file:\t" +
                  dummy_info_path["path"])


    # Independent fit chunked data with UDF.
    if 'dqr' in fit_algorithms:

        # Step 0: Data transformation

        # Standardize non-categorical columns
        sample_size = int(data_info.iloc[0,1])
        if data_processing['X'] == 'standardize':
            for non_dummy_col in X_names:
                XY_sdf_i = XY_sdf_i.withColumn(non_dummy_col,
                                               (F.col(non_dummy_col)-data_info[non_dummy_col][1])/data_info[non_dummy_col][2])

        # Transformation of response variable
        if data_processing['Y'] == 'log':
            XY_sdf_i = XY_sdf_i.withColumn(Y_name, F.log(F.col(Y_name)))

        # Add the intercept column
        if dqr_conf['fit_intercept']:
            X_names.insert(0, 'Intercept')
            XY_sdf_i = XY_sdf_i.withColumn('Intercept', F.lit(1))

        # Step 1: Pilot Sampler
        XY_pilot_sdf_i = XY_sdf_i.sample(withReplacement=False,
                                         fraction=dqr_conf['pilot_sampler'])

        XY_pilot_pdf_i = XY_pilot_sdf_i.toPandas()  # Send to master

        # Run a pilot model with sampled data from Spark. Note: statsmodels does not support sparse matrix, TERRIBLE!
        if len(dummy_names) > 0:
            onehot_column_names = ['_'.join([key, str(values_i)])
                                  for key in dummy_info['factor_selected_names'].keys()
                                  for values_i in dummy_info['factor_selected_names'][key]]
            XY_pilot_pdf_i = spark_onehot_to_pd_dense(pdf=XY_pilot_pdf_i,
                                                        onehot_column='features_ONEHOT',
                                                        onehot_column_names=onehot_column_names,
                                                        onehot_column_is_sparse=False)
        else:
            onehot_column_names = []

        column_names_x_full = X_names + onehot_column_names

        # statsmodels.quantile_regression is picky about covariates. 1. All covariates
        # must be float, and int dummies are not allowed. 2. Multicolineared covariates
        # will give error.
        dqr_pilot = QuantReg(endog=XY_pilot_pdf_i[Y_name],
                             exog=(XY_pilot_pdf_i[column_names_x_full]).astype(float))
        dqr_pilot_res = dqr_pilot.fit(q=dqr_conf['quantile'])

        # dqr_pilot = QuantReg(endog=XY_pilot_pdf_i[Y_name],
        #                      exog=(XY_pilot_pdf_i[column_names_x_full[:21] + column_names_x_full[24:]]).astype(float))
        # dqr_pilot_res = dqr_pilot.fit(q=dqr_conf['quantile'])

        dqr_pilot_par = {
            'bandwidth': dqr_pilot_res.bandwidth,
            'params': dqr_pilot_res.params
        }

        # Step 2: Updating QR components
        tic_repartition = time.perf_counter()
        XY_sdf_i = XY_sdf_i.repartition(partition_num_sub[file_no_i],
                                            "partition_id")
        time_repartition_sub.append(time.perf_counter() - tic_repartition)

        # Register a user defined function via the Pandas UDF
        Xdim = len(column_names_x_full) #  - 2 # with Y, partition_id
        XTX_tril_len = int(Xdim * (Xdim + 1) / 2)
        schema_XTX = StructType([StructField(str(i), DoubleType(), True)
                                 for i in range(XTX_tril_len)])
        @pandas_udf(schema_XTX, F.PandasUDFType.GROUPED_MAP)
        def XTX_udf(pdf):
            # Convert Spark ONEHOT encoded column into Pandas dense DataFrame
            if len(dummy_names) > 0:
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

        # Register a user defined function via the Pandas UDF
        schema_qr_comp_full = StructType([StructField(str(i), DoubleType(), True)
                                     for i in range(Xdim + 1)])
        @pandas_udf(schema_qr_comp_full, F.PandasUDFType.GROUPED_MAP)
        def qr_asymptotic_comp_udf(pdf):
            if len(dummy_names) > 0:
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
                Y_name=Y_name, out='all')

        # partition the data and run the UDF
        qr_comp_sdf = XY_sdf_i.groupby("partition_id").apply(qr_asymptotic_comp_udf)

        # Step 3: Merge and combine
        qr_comp = qr_comp_sdf.toPandas().to_numpy()
        XTX = XTX_sdf.toPandas().to_numpy()

        XTX_tril = np.sum(XTX,axis=0)
        XTX_full = np.zeros((Xdim, Xdim))
        XTX_full[np.tril_indices(XTX_full.shape[0], k=0)] = XTX_tril
        XTX_full = XTX_full + XTX_full.T - np.diag(np.diag(XTX_full))
        XTX_inv = np.linalg.inv(XTX_full)  # p-by-p

        # Step 4: One-step updating coefficients
        qr_comp_sum = np.sum(qr_comp, axis=0)
        f0_hat_inv = sample_size * dqr_pilot_res.bandwidth / qr_comp_sum[-1]
        out_beta = dqr_pilot_res.params + f0_hat_inv * XTX_inv.dot(qr_comp_sum[:-1])

        # Step 5: Asymptotic covariance
        # Register a user defined function via the Pandas UDF
        schema_qr_comp_f1 = StructType([StructField('f1', DoubleType())])
        @pandas_udf(schema_qr_comp_f1, F.PandasUDFType.GROUPED_MAP)
        def qr_asymptotic_comp_f1_udf(pdf):
            if len(dummy_names) > 0:
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
                beta0=out_beta,
                quantile=dqr_conf['quantile'],
                bandwidth=dqr_pilot_res.bandwidth,
                Y_name=Y_name, out='f0')

        # partition the data and run the UDF
        qr_comp_f1_sdf = XY_sdf_i.groupby("partition_id").apply(qr_asymptotic_comp_f1_udf)
        qr_comp_f1 = qr_comp_f1_sdf.toPandas().to_numpy()
        qr_comp_f1_sum = np.sum(qr_comp_f1, axis=0)
        f1_hat_inv = sample_size * dqr_pilot_res.bandwidth / qr_comp_f1_sum
        out_beta_cov = XTX_inv * sample_size * dqr_conf['quantile'] * (1 - dqr_conf['quantile']) * f1_hat_inv**2
        out_beta_var = out_beta_cov.diagonal() #/ np.sqrt(sample_size)
        out_beta_se = np.sqrt(out_beta_var)

        # P-values for significance
        out_beta_p_values =[2*(1-stats.t.cdf(np.abs(i),sample_size-len(out_beta))) for i in out_beta / out_beta_se]

        # Output
        out_dqr = pd.concat([out_beta,
                             dqr_pilot_res.params,
                             pd.DataFrame(out_beta_var,index=out_beta.index),
                             pd.DataFrame(out_beta_se,index=out_beta.index),
                             pd.DataFrame(out_beta_p_values,index=out_beta.index),
                             dqr_pilot_res.pvalues],
                            axis=1)
        out_dqr.columns = ['beta_dqr', 'beta_pilot', 'var_dqr', 'se_dqr', 'pvalues_dqr', 'pvalues_pilot']
    # ---------------------------------------------------------------------------------------
    # PRINT OUTPUT
    # ---------------------------------------------------------------------------------------
    out = {'dqr_pilot_res': dqr_pilot_res,
           'data_info': data_info,
           # 'dummy_info': dummy_info,
           'out_dqr': out_dqr,
           'out_beta': out_beta,
           'out_beta_cov': out_beta_cov,
           'out_beta_var': out_beta_var,
           'dummy_names': dummy_names,
           'X_names': X_names,
           'X_names_full': column_names_x_full,
           'commcost': out_commcost
    }
    print("\nModel Summary:\n")
    pprint.pprint(out)

    pickle.dump(out, open(os.path.expanduser(model_saved_file_name + '.pkl'), 'wb'))
    # json.dump(out, open(os.path.expanduser(model_saved_file_name + '.json'), 'w'))
    print("Model results are saved to:\t" + model_saved_file_name + '.pkl')
