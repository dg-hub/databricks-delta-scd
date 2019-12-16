"""
Databricks SCD Module
Copyright (c) 2019 Daniel Glidden - https://github.com/dg-hub
"""

__author__ = "Daniel Glidden (T815034)"
__version__ = "0.2.0"

import uuid
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import hash, current_timestamp, to_timestamp, lit, concat, when, col, sha1

# Define the scd attribute columns required for the target table
SCD_FLAG_NAME = "scd_latest_flag"
SCD_START_NAME = "scd_start_date"
SCD_END_NAME = "scd_end_date"
SCD_HASHKEY_NAME = "scd_hash_key"
SCD_ATTR_COLUMNS = [SCD_FLAG_NAME, SCD_START_NAME, SCD_END_NAME, SCD_HASHKEY_NAME]

# Define the default data values
SCD_FLAG_ACTIVE = "C"
SCD_FLAG_DELETED = "D"
SCD_FLAG_HISTORY = "H"
SCD_DEFAULT_END_VALUE = "9999-12-31 00:00:00"

def __subtract_list(list_a: list , list_b: list):
    """
    Private Function - removes elements from a list via subtaction

    Parameters:
    list_a (list): The original list
    list_b (list): A list of values to remove from list_a

    Returns:
    list: returns a substracted list of elements
    
    """
    return [x for x in list_a if x not in set(list_b)]

def __sha1_concat(hash_columns):
  """
  Private Function - concatenates columns provided using : delimiter

  Parameters:
  hash_columns (list): A string list containing the list of columns to hash

  Returns:
  str: returns Hash sha1{'column1:column2:'}
  """
  column_ref_list = [when(col(hash_col_name).isNull(),lit(":")).otherwise(concat(col(hash_col_name).cast("string"),lit(":"))) 
                     for hash_col_name in hash_columns]
  concat_string = sha1(concat(*column_ref_list))
  return concat_string


def create_delta_target(source_data_frame: DataFrame, path: str, hash_exclude_columns: list = []):
    """
    Creates a target table using delta from DataFrame provided

    This is required if no data has yet been ingested.
    It will generate a temporary table with uuid then drop it.
    It will overwrite any existing table so use with caution

    Parameters:
    source_data_frame (DataFrame): A source DataFrame
    path (str): The path to write Dataframe to
    hash_exclude_columns (list): A list of columns to ignore changes on. Does not include in hash

    """
    _df = source_data_frame
    _hash_columns =  __subtract_list(_df.columns, hash_exclude_columns) # get a list of columns to hash
    _uuid_value = uuid.uuid4().hex  # unique value used to name temp table.

    # Add SCD Attribute columns and write Delta
    _df = _df.withColumn(SCD_FLAG_NAME, lit(SCD_FLAG_ACTIVE))
    _df = _df.withColumn(SCD_START_NAME, current_timestamp())
    _df = _df.withColumn(SCD_END_NAME, to_timestamp(lit(SCD_DEFAULT_END_VALUE)))
    _df = _df.withColumn(SCD_HASHKEY_NAME, __sha1_concat(_hash_columns))
    _df.write.format("delta").option("overwriteSchema", "true").partitionBy(SCD_FLAG_NAME).saveAsTable(_uuid_value,
                                                                                                       mode="overwrite",
                                                                                                       path=path)
    #Drop the table from metastore - the files remain since path=, makes the table EXTERNAL
    sql("OPTIMIZE {}".format(_uuid_value))
    sql("VACUUM {}".format(_uuid_value))
    sql("DROP TABLE IF EXISTS {}".format(_uuid_value))


def build_merge_sql(primary_key_columns: list, uuid_value: str):
    """
    Builds a SQL MERGE statement

    A UUID is required since databricks delta acts on registered tables.
     source table is s_{uuid}
     target table is t_{uuid}

    Parameters:
    primary_key_columns (list): List containing the primary key column names
    uuid_value (str): a referenece to the source and target tables in Databricks

    Returns:
    str: returns sql MERGE statement

    """
    all_source_columns = [column.name for column in spark.table("s_{}".format(uuid_value)).schema]
    # remove the pk columns so just the value columns are left
    source_columns = __subtract_list(all_source_columns,primary_key_columns)
    source_columns = __subtract_list(source_columns,SCD_ATTR_COLUMNS)

    
    #Build Merge, see: https://docs.databricks.com/_static/notebooks/merge-in-scd-type-2.html
    sql_merge = []
    sql_merge.append("MERGE INTO t_{uuid}".format(uuid=uuid_value))
    sql_merge.append("USING (")
    sql_merge.append("SELECT")
    for idx, key_column_name in enumerate(primary_key_columns):
        sql_merge.append("\t {} as {}_key{}, ".format(key_column_name, uuid_value, idx))
    sql_merge.append("\t source.*, ")
    sql_merge.append("\t current_timestamp as {start_date}".format(start_date=SCD_START_NAME))
    sql_merge.append("FROM s_{uuid} as source".format(uuid=uuid_value))
    sql_merge.append("UNION ALL")
    sql_merge.append("SELECT ")
    for idx, key_column_name in enumerate(primary_key_columns):
        sql_merge.append(
            "\t nvl2(source.{},NULL,j_target.{}) as {}_key{}, ".format(key_column_name, key_column_name, uuid_value,
                                                                       idx))
    sql_merge.append("\t source.*,")
    sql_merge.append("\t current_timestamp as {}".format(SCD_START_NAME))
    sql_merge.append("FROM s_{uuid} as source".format(uuid=uuid_value))
    sql_merge.append("RIGHT JOIN t_{uuid} as j_target".format(uuid=uuid_value))
    sql_merge.append("ON (")
    for idx, key_column_name in enumerate(primary_key_columns):
        if idx == 0:
            sql_merge.append("\t source.{} = j_target.{}".format(key_column_name, key_column_name))
        else:
            sql_merge.append("\t and source.{} = j_target.{}".format(key_column_name, key_column_name))
    sql_merge.append(") ")
    sql_merge.append("WHERE j_target.{latest_flag} = '{active}' ".format(latest_flag=SCD_FLAG_NAME,active=SCD_FLAG_ACTIVE))
    sql_merge.append("\t  and (source.{} <> j_target.{} or source.{} is null)".format(SCD_HASHKEY_NAME,SCD_HASHKEY_NAME,primary_key_columns[0]))
    sql_merge.append(") staged_updates")
    sql_merge.append("ON ")
    for idx, key_column_name in enumerate(primary_key_columns):
        if idx == 0:
            sql_merge.append("\t t_{}.{} = staged_updates.{}_key{}".format(uuid_value, key_column_name, uuid_value, idx))
        else:
            sql_merge.append(
                "\t and t_{}.{} = staged_updates.{}_key{}".format(uuid_value, key_column_name, uuid_value, idx))
    sql_merge.append("WHEN MATCHED AND t_{uuid}.{flag_name} = '{active}'".format(uuid=uuid_value,flag_name=SCD_FLAG_NAME,active=SCD_FLAG_ACTIVE))
    sql_merge.append("\t and (t_{}.{} <> staged_updates.{}".format(uuid_value,SCD_HASHKEY_NAME,SCD_HASHKEY_NAME))
    sql_merge.append("\t  or staged_updates.{primary_key} is null)".format(primary_key=primary_key_columns[0])) #brings in deleted rows (source is null)
    sql_merge.append("THEN UPDATE")
    sql_merge.append("\t SET {latest_flag} = nvl2(staged_updates.{primary_key},'{history}','{deleted}'), ".format(latest_flag=SCD_FLAG_NAME
                                                                                                    ,primary_key=primary_key_columns[0]
                                                                                                    ,history=SCD_FLAG_HISTORY
                                                                                                    ,deleted=SCD_FLAG_DELETED
                                                                                                   )) ##false
    sql_merge.append("\t  {end_date} = staged_updates.{start_date} - INTERVAL '1' SECOND".format(end_date=SCD_END_NAME,start_date=SCD_START_NAME))
    sql_merge.append("WHEN NOT MATCHED ")
    sql_merge.append("\t and staged_updates.{primary_key} is not null THEN ".format(primary_key=primary_key_columns[0]))
    sql_merge.append("INSERT ({}, {}, {}) ".format(", ".join(primary_key_columns), ", ".join(source_columns), ", ".join(SCD_ATTR_COLUMNS)))
    sql_merge.append("VALUES(")
    for idx, key_column_name in enumerate(primary_key_columns):
        if idx == 0:
            sql_merge.append("\t  staged_updates.{primary_key}".format(primary_key=key_column_name))
        else:
            sql_merge.append("\t, staged_updates.{primary_key}".format(primary_key=key_column_name))
    for idx, source_column_name in enumerate(source_columns):
        sql_merge.append("\t, staged_updates.{}".format(source_column_name))
    sql_merge.append("\t, '{active_flag}', staged_updates.{start_date}, '{default_end_date}',{hash_key})".format(active_flag=SCD_FLAG_ACTIVE
                                                                                                  ,start_date=SCD_START_NAME
                                                                                                  ,default_end_date=SCD_DEFAULT_END_VALUE
                                                                                                  ,hash_key=SCD_HASHKEY_NAME)) ## new SCD columns inserted here
    return "\n".join(sql_merge)


def __register_target_path(uuid: str, file_path: str):
    """
    Private Function - registers a temporary target table
    - uses 't_{uuid}' as table names
    """
    spark.sql('CREATE TABLE IF NOT EXISTS t_{uuid} USING DELTA LOCATION "{path}"'.format(uuid=uuid, path=file_path))


def __drop_target_table(uuid: str):
    """
    Private Function - drops the temporary target table
    - Drops 't_{uuid}' as table names
    """
    spark.sql('DROP TABLE IF EXISTS t_{uuid}'.format(uuid=uuid))

def __register_source_dataframe(uuid, data_frame, hash_exclude_columns=[]):
    """
    Private Function - registers a temporary source table
    - uses 's_{uuid}' as table names
    """
    #_hash_columns = list(set(data_frame.columns) - set(hash_exclude_columns))
    _hash_columns = __subtract_list(data_frame.columns, hash_exclude_columns) # The columns which will be hashed
    data_frame.withColumn(SCD_HASHKEY_NAME, __sha1_concat(_hash_columns)).registerTempTable("s_{}".format(uuid))

def __drop_source_table(uuid: str):
    """
    Private Function - drops the temporary source table
    - Drops 's_{uuid}' as table names
    """
    spark.sql('DROP TABLE IF EXISTS s_{uuid}'.format(uuid=uuid))


def get_scd(source_dataframe: DataFrame, target_path: str, primary_key_columns: list, hash_exclude_columns: list = [] ):
    """
    Used for testing - it will return the sql MERGE statement (str) and not execute 
    - uses 'test' as uuid - s_test and t_test will exist after the function is called
    """
    uuid_value = "test"
    sql_string = ""

    # Create source and target temp tables
    __register_source_dataframe(uuid_value, source_dataframe)
    __register_target_path(uuid_value, target_path)

    # Get the MERGE SQL and return it
    sql_string = build_merge_sql(primary_key_columns, uuid_value)
    return sql_string


def exec_scd(source_dataframe: DataFrame, target_path: str, primary_key_columns: list, hash_exclude_columns: list = [] ):
    """
    Executes a SCD merge into target path

    Takes a source dataframe and loads it into the target path as SCD.

    Parameters:
    source_dataframe (DataFrame): a DataFrame containing source data to load into target
    target_path (str): String containing the path to the target delta data.
    primary_key_columns (list): A list<str> containing the names of columns which will be used to match
    hash_exclude_columns (list): A list of columns to ignore changes on. Does not include in hash

    Returns:
    str: returns sql MERGE statement as a string

    """
    uuid_value = uuid.uuid4().hex  # create a unique id so went dont colide with existing column names
    try:
        # Create source and target temp tables.
        __register_source_dataframe(uuid_value, source_dataframe,hash_exclude_columns)
        __register_target_path(uuid_value, target_path)

        # Get the MERGE SQL and execute it
        sql_string = build_merge_sql(primary_key_columns, uuid_value)
        sql(sql_string)

        display(sql("""
            select * 
            from t_{uuid} 
            order by {primary_key}, {start_date}""".format(uuid=uuid_value, 
                                                           primary_key=primary_key_columns[0],
                                                          
                                                           start_date=SCD_START_NAME)))

        # Clean up the Delta tables - Vacuum + Optimize
        sql("vacuum t_{}".format(uuid_value))
        sql("optimize t_{} ZORDER BY ({})".format(uuid_value, primary_key_columns[0]))
    finally:
        __drop_target_table(uuid_value)
        __drop_source_table(uuid_value)
