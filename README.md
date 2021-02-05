# `dqr`: Distributed Quantile Regression by Pilot Sampling and One-Step Updating

# Spark implementation

## System Requirements

- `Spark >= 2.3.1`
- `Python >= 3.7.0`
  - `pyarrow >= 0.15.0` Please read this [Compatibility issue with Spark 2.3.x or 2.4.x](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#compatibility-setting-for-pyarrow--0150-and-spark-23x-24x)
  - `statsmodels >= 0.12.0`

- See [`setup.py`](setup.py) for detailed requirements.

## Run the code on a Spark platform

- Zip the code as a standard Python module ('placed in `dist/`')

``` python
python3.7 setup.py sdist
```

- Run the project on a Spark platform

``` bash
PYSPARK_PYTHON=/usr/local/bin/python3.7 \
        spark-submit --py-files dist/dqr-0.1.dev53+g1a4b3cb.d20210205.zip \
        projects/dqr_spark.py
```


# Conceptual demo in R

- Contributed by [@edwardguo61](https://github.com/edwardguo61/Quantile_Regression_code)

- The required `R` version: `3.5.1`

- Files:
  - `dqr/Restimator.R`: one-shot estimation and one-step estimation for distributed quantile regression
  - `dqr/R/simulator.R`: simulation functions to generate random/non-random data
  - `dqr/R/uilts.R`: other functions used
  - `projects/dqr_demo.R`: generate data, conduct estimation and generate plot. Please run
    `dqr_demo.R` to see how to use the functions.
