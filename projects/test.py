#! /usr/bin/env python3


import sys

quantile_args = sys.argv

print(quantile_args[1:])


import numpy as np
import matplotlib.pyplot as plt

# Fixing random state for reproducibility

# the histogram of the data
n, bins, patches = plt.hist(Y, density=False, facecolor='g', alpha=0.75)


plt.xlabel('Price')
plt.ylabel('Probability')
# plt.text(60, .025, r'$\mu=100,\ \sigma=15$')
# plt.xlim(40, 160)
# plt.ylim(0, 0.03)
plt.grid(True)
