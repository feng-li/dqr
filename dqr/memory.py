from pyspark import SparkContext
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark import StorageLevel


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
    """
    this function will convert bytes to MB.... GB... etc
    """
    step_unit = 1000.0 #1024 bad the size

    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < step_unit:
            return "%3.1f %s" % (num, x)
        num /= step_unit
