#! /bin/bash

# https://help.aliyun.com/document_detail/28124.html

# Fat executors: one executor per node
# EC=16
# EM=30g

# Make a zip package
# zip -r ../../dlsa.zip ../dlsa.py ../R/dlsa_alasso_func.R

MODEL_DESCRIPTION=$1

# Tiny executors: one executor per core
EC=1
EM=6g

# MODEL_FILE=logistic_spark
MODEL_FILE=dqr_spark
OUTPATH=~/running/

# Get current dir path for this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Zip dlsa for uploading
cd $DIR/../
make zip
cd -;

tic0=`date +%Y%m%d-%H.%M.%S`
# for executors in {256..4..-4}
for executors in 32
do
    tic=`date +%s`
    PYSPARK_PYTHON=python3.7 ARROW_PRE_0_15_IPC_FORMAT=1 spark-submit \
                  --master yarn  \
                  --executor-memory ${EM}   \
                  --num-executors ${executors}      \
                  --driver-memory 50g    \
                  --executor-cores ${EC}    \
                  --conf spark.rpc.message.maxSize=2000 \
                  $DIR/${MODEL_FILE}.py "$@"
#                  > ${OUTPATH}${MODEL_DESCRIPTION}_${MODEL_FILE}.NE${executors}.EC${EC}_${tic0}.out 2> ${OUTPATH}${MODEL_DESCRIPTION}_${MODEL_FILE}.NE${executors}.EC${EC}_${tic0}.log
    toc=`date +%s`
    runtime=$((toc-tic))
    echo ${MODEL_FILE}.NE${executors}.EC${EC} finished, "Time used (s):" $runtime

done

exit 0;
