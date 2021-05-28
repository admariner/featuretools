import warnings
from datetime import datetime

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pandas.api.types as pdtypes

from featuretools.utils.gen_utils import import_or_none, is_instance

ks = import_or_none('databricks.koalas')


def get_linked_vars(entity):
    """Return a list with the entity linked variables.
    """
    link_relationships = [r for r in entity.entityset.relationships
                          if r.parent_dataframe.id == entity.id or
                          r.child_dataframe.id == entity.id]
    link_vars = [v.id for rel in link_relationships
                 for v in [rel.parent_column, rel.child_column]
                 if v.entity.id == entity.id]
    return link_vars


def col_is_datetime(col):
    # check if dtype is datetime - use .head() when getting first value
    # in case column is a dask Series
    if (col.dtype.name.find('datetime') > -1 or
            (len(col) and isinstance(col.head(1).iloc[0], datetime))):
        return True

    # if it can be casted to numeric, it's not a datetime
    dropped_na = col.dropna()
    try:
        pd.to_numeric(dropped_na, errors='raise')
    except (ValueError, TypeError):
        # finally, try to cast to datetime
        if col.dtype.name.find('str') > -1 or col.dtype.name.find('object') > -1:
            try:
                pd.to_datetime(dropped_na, errors='raise')
            except Exception:
                return False
            else:
                return True

    return False


def replace_latlong_nan(values):
    """replace a single `NaN` value with a tuple: `(np.nan, np.nan)`"""
    return values.where(values.notnull(), pd.Series([(np.nan, np.nan)] * len(values)))
