import pandas as pd
import numpy as np
import talib

from .tautils import _gen_operator_decor
from .tautils import PrefixOpMap, SuffixOpMap

prefix = _gen_operator_decor(PrefixOpMap)
suffix = _gen_operator_decor(SuffixOpMap)

@prefix(argc=2)
def MA(df, num=5, col="Close",**kwargs):
    return pd.rolling_mean(df[col], num), num

@prefix(argc=2)
def Log(df, num=0, col="Close",**kwargs):
    if num == 0:
        return np.log(df[kwargs["arg2"]]) - np.log(df[kwargs["arg1"]]), 0
    else:
        return np.log(df[col]) - np.log(df[col].shift(num)), num
@prefix(argc=2)
def Pct(df, num=0, col="Close", **kwargs):
    if num == 0:
        return (df[kwargs["arg2"]] - df[kwargs["arg1"]]) / df[kwargs["arg1"]], 0
    else:
        return df[col].pct_change(num), num
@suffix(argc=2)
def Lag(df, num=1, col="Close",**kwargs):
    return df[col].shift(num), num

@suffix(argc=2)
def Ahead(df, num=1, col="Close",**kwargs):
    return df[col].shift(num), num
