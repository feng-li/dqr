# `dqr`: Distributed Quantile Regression by Pilot Sampling and One-Step Updating

# Conceptual demo in R

- The Required `R` version: `3.5.1`

- Files:
  - `estimator.R`: one-shot estimation and one-step estimation for distributed quantile regression
  - `simulator.R`: simulation functions to generate random/non-random data
  - `uilts.R`: other functions used
  - `main.R`: generate data, conduct estimation and generate plot

- Please run `main.R` to see how to use the functions.

# Spark implementation

## System Requirements

- `Spark >= 2.3.1`
- `Python >= 3.7.0`
  - `pyarrow >= 0.15.0` Please read this [Compatibility issue with Spark 2.3.x or 2.4.x](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#compatibility-setting-for-pyarrow--0150-and-spark-23x-24x)
  - `scikit-learn >= 0.21.2`
  - `rpy2 >= 3.0.4` (optional)

- `R >= 3.5` (optional)
  - `lars`

  See [`setup.py`](setup.py) for detailed requirements.
