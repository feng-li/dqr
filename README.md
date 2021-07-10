# `dqr`: Distributed Quantile Regression
_by Pilot Sampling and One-Step Updating_

# Spark implementation

## System Requirements

- `Spark >= 2.3.1`
- `Python >= 3.7.0`
  - `pyarrow >= 0.15.0` Please read this [Compatibility issue with Spark 2.3.x or 2.4.x](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#compatibility-setting-for-pyarrow--0150-and-spark-23x-24x)
  - `statsmodels >= 0.12.0`

- See [`setup.py`](setup.py) for detailed requirements.

## Run the code on a Spark platform

- Zip the code into a portable package (a zipped file `dqr.zip` will be placed into the
  `projects` folder)

``` bash
make zip
```

- Run the project on a Spark platform

``` bash
PYSPARK_PYTHON=/usr/local/bin/python3.7 \
        spark-submit --py-files projects/dqr.zip \
        projects/dqr_spark.py
```

## Build a Python module

You could also build the code into standard Python module and deploy to Spark clusters.

``` python
python setup.py bdist
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

# References

- Rui Pan, Tunan Ren, [Baishan Guo](https://github.com/edwardguo61/), [Feng Li](https://feng.li/), Guodong Li and Hansheng Wang (2021). A Note on Distributed Quantile Regression by Pilot Sampling and One-Step Updating, _Journal of Business and Economic Statistics_. (in press). 
