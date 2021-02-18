import numpy as np
import pandas as pd


def qr_asymptotic_comp(pdf, beta0, quantile, bandwidth, Y_name, out='all'):
    """This function calculates the components in the one-step updating estimator
    for quantile regression based on Koenker (2005), Wang et al. (2007), and
    Chen et al. (2019),

    pdf: Pandas DataFrame containing X and Y

    Return: The last element is for the Gaussian kernel component

    """

    Y = pdf[Y_name].to_numpy()
    X = pdf.drop(Y_name, axis=1).to_numpy()

    n, p = X.shape

    Xbeta = X.dot(beta0)
    error = Y - Xbeta  # n-by-1

    # Gaussian Kernel
    K = np.array(np.sum(1 / np.sqrt(2 * np.pi) * np.exp(-(error.astype(float) / bandwidth.astype(float) ) ** 2 / 2)))

    if out == 'all':
        I = (error < 0)
        Z = (quantile - I).reshape(n, 1)
        XZ = np.sum(np.multiply(X, Z), axis=0)  # 1-by-p
        out = pd.DataFrame(np.concatenate([XZ.reshape(1,p), K.reshape(1,1)],axis=1))
    elif out == 'f0':
        out = pd.DataFrame(K.reshape(1,1))
    else:
        raise Exception("No such out component:\t" + out)

    return(out)
