def min_max_normalize(df):
    df_norm = (df - df.min ()) / (df.max () - df.min ())
    return df_norm