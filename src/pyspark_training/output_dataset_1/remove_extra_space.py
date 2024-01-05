def remove_extra_spaces(df, column_name):
    df_transformed = df.withColumn(column_name, F.regexp_replace(F.col(column_name), "\\s+", " "))
    return df_transformed