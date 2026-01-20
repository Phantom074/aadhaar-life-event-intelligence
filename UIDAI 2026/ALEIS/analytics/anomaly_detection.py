import numpy as np
from scipy.stats import zscore

def detect_anomalies(series, threshold=2.5):
    """
    Detects outliers using Z-score while handling near-identical data.
    """
    # 1. Check if the series has zero variance (all values are identical)
    if series.std() == 0:
        return [False] * len(series)
    
    # 2. Calculate Z-score only if variance is present
    z = zscore(series)
    
    # 3. Handle potential NaN results from near-zero variance
    return np.abs(np.nan_to_num(z)) > threshold