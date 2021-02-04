import pandas as pd

# Convert ONEHOT encoded Pandas to a full dense pandas.
def spark_onehot_to_pd_dense(pdf, onehot_column, onehot_column_names=[],
                             onehot_column_is_sparse=True):
    """Convert Pandas DataFrame containing Spark SparseVector encoded column into pandas dense vector

    """
    # pdf = data_pilot_pdf_i
    # column = 'features_ONEHOT'
    if onehot_column_is_sparse:
        features_ONEHOT = pdf[onehot_column].apply(lambda x: x.toArray())
    else:
        features_ONEHOT = pdf[onehot_column]

    # one dense list
    features_DENSE = features_ONEHOT.explode().values.reshape(
        features_ONEHOT.shape[0], len(features_ONEHOT[0]))

    features_pd = pd.DataFrame(features_DENSE)

    if len(onehot_column_names) != 0:
        features_pd.columns = onehot_column_names

    pdf_dense = pd.concat([pdf.drop(onehot_column,axis=1), features_pd],axis=1)
    return(pdf_dense)
