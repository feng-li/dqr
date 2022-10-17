import os
from setuptools import setup
from distutils.command.sdist import sdist as _sdist


def read(file):
    return open(os.path.join(os.path.dirname(__file__), file)).read()


# Pyspark does not allow for tar.gz module to attach, silly... Override sdist to always produce .zip archive
class sdistzip(_sdist):
    def initialize_options(self):
        _sdist.initialize_options(self)
        self.formats = 'zip'


setup(name='dqr',
      use_scm_version=True,
      setup_requires=['setuptools_scm'],
      cmdclass={'sdist': sdistzip},
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
          'pyarrow >= 0.15.0',
          'statsmodels >= 0.12.0',
          'numpy   >= 1.16.3',
          'pandas  >= 0.23.4',
      ],
      zip_safe=False,
      python_requires='>=3.7')
