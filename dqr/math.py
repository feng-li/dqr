import numpy as np
import pandas as pd


def XTX(X):
    """This function calculates X'X where X is an n-by-p  Pandas DataFrame.

    This function employs the factorization that X'X = \sum_{i=1}^n x_i x'_i where
    x is p-by-1 vector. It will firstly make a row-wise calculation and then sum
    over all rows.

    X: Pandas DataFrame

    Return: 1-by-p(p+1)/2 row column Pandas DataFrame which is the lower
    triangular part (including the diagonal elements) of the symmetric matrix X'X.

    """
    # pdf = data_pilot_pdf_i[ ['mileage', 'year', 'price'] ]
    # if len(onehot_column) != 0:

    mat = X.to_numpy()
    n, p = mat.shape
    m = int(p*(p + 1)/2)
    out_n_tril = np.zeros((n, m))
    for i in range(n):
        outer_i = np.outer(mat[i, :], mat[i, :])
        out_n_tril[i, :] = outer_i[np.tril_indices(p)]

    out_np = np.sum(out_n_tril, axis=0)
    out_pd = pd.DataFrame(out_np.reshape(1, m))
    return(out_pd)
