# def temporal_concentration(series):
#     """
#     Measures how concentrated updates are in time.
#     """
#     return series.std() / series.mean()


def temporal_concentration(series):
    """
    Measures how concentrated updates are over time.
    Returns 0 if there is no activity to avoid division by zero.
    """
    mu = series.mean()
    if mu == 0:
        return 0.0
    return series.std() / mu