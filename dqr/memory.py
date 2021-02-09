from pyspark import SparkContext
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark import StorageLevel
import pandas as pd
import time


def pyspark_SizeEstimator(sdf):
    """Return the memory size in bytes of PySpark DataFrame.

    It firstly converts each Python object into Java object by Pyrolite, whenever the RDD
    is serialized in batch or not. Then it utilize the Java function `SizeEstimator` to
    obtain the object size.

    """
    sdf.persist(StorageLevel.MEMORY_ONLY)
    s_rdd = sdf.rdd
    rdd = s_rdd._reserialize(AutoBatchedSerializer(PickleSerializer()))
    java_obj = s_rdd.ctx._jvm.org.apache.spark.mllib.api.python.SerDe.pythonToJava(rdd._jrdd, True)
    size = SparkContext._jvm.org.apache.spark.util.SizeEstimator.estimate(java_obj)

    return size


def convert_bytes(num):
    """Convert bytes to MB.... GB... etc
    """
    step_unit = 1000.0 #1024 bad the size

    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < step_unit:
            return "%3.2f %s" % (num, x)
        num /= step_unit


def commcost_estimate(sdf, fractions):
    """Simple function to estimate the communication cost in a Spark system when transferring
    a distributed Spark DataFrame to a Pandas DataFrame.

    """
    time_commcost = []
    num_partitions = []
    sample_size = []
    mem_byte_size = []
    for fraction in fractions:
        tic_commcost = time.perf_counter()
        pdf_i = sdf.sample(fraction=fraction).toPandas()
        time_commcost.append(time.perf_counter() - tic_commcost)
        sample_size.append(pdf_i.shape[0])
        mem_byte_size.append(pdf_i.memory_usage(deep=True).sum())
        num_partitions.append(sdf.rdd.getNumPartitions())

        del pdf_i

    out = pd.DataFrame({'time_commcost': time_commcost,
                        'sample_size': sample_size,
                        'mem_byte_size': mem_byte_size,
                        'num_partitions': num_partitions})
    return out
