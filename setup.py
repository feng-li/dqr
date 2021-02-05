import os
from setuptools import setup

def read(file):
    return open(os.path.join(os.path.dirname(__file__), file)).read()

setup(name='dqr',
      setup_requires=['setuptools-git-versioning'],
      version_config={
          "template": "{tag}",
          "dev_template": "{tag}.dev{ccount}+git.{sha}",
          "dirty_template": "{tag}.dev{ccount}+git.{sha}.dirty",
          "starting_version": "0.1.1",
          "version_callback": None,
          "version_file": None,
          "count_commits_from_version_file": False
      },
      description='Distributed Quantile Regression',
      keywords='spark, spark-ml, pyspark, mapreduce',
      long_description=read('README.md'),
      long_description_content_type='text/markdown',
      url='https://github.com/feng-li/dqr',
      author='Feng Li',
      author_email='feng.li@cufe.edu.cn',
      license='MIT',
      packages=['dqr'],
      install_requires=[
          'pyspark >= 2.3.1',
          'statsmodels >= 0.12.0',
          'numpy   >= 1.16.3',
          'pandas  >= 0.23.4',
      ],
      zip_safe=False,
      python_requires='>=3.7',
)
