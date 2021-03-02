#! /usr/bin/env python3

import pickle
import os
import pandas as pd

model_result_path = '~/running/dqr2021/'
model_result_files = [ 'dqr_model_0.1_20210301-15.46.52.pkl',
                       'dqr_model_0.25_20210301-16.05.59.pkl',
                       'dqr_model_0.5_20210301-16.25.45.pkl',
                       'dqr_model_0.75_20210301-16.45.30.pkl',
                       'dqr_model_0.9_20210301-17.04.49.pkl']

results = []

for file in model_result_files:
    file_full_path = model_result_path + file
    results.append(pickle.load(open(os.path.expanduser(file_full_path), 'rb')))

beta_all = pd.concat([results[i]['out_dqr']['beta_dqr'] for i in range(len(model_result_files))],axis=1)
pvalues_all = pd.concat([results[i]['out_dqr']['pvalues_dqr'] for i in range(len(model_result_files))],axis=1)
