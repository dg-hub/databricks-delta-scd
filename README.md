# Databricks Delta SCD
A Python module for automating SCD via SQL MERGE using Databricks Delta

# License 
MIT - Copyright (c) 2019 Daniel G - https://github.com/dg-hub

# Features

* **create_delta_target()** -  Creates a target table with attribute columns from DataFrame
* **build_merge_sql()** -  Builds a SQL MERGE statement
* **exec_scd()** - Executes a SCD merge into target path

# Feature requests
##### Please use Git Hub issues to request new features:
https://github.com/dg-hub/databricks-delta-scd/issues


# Release Notes

Version 0.2.0 (December 16, 2019) Add initial code to build and execute SQL MERGE

# Usage

#### A delta target is required before any updates can be merged.

```python
    # Create a new target table from 'source'
    source_path = "/user/hive/warehouse/source/"
    target_path = "/user/hive/warehouse/target"
    
    #One or more primary keys are required
    target_keys = ["id1","id2"]

    #Columns can be excluded from being compared for changes
    hash_exclude_columns = ["value2"]

    #Define a DataFrame by loading some source data
    df_source = spark.read.format("delta").load(source_path)

    #Create target delta files from source DataFrame
    create_delta_target(df_source,target_path,hash_exclude_columns)
```

#### Once a SCD target exists data can be merged.

```python
    # Execute SCD merge of from 'source_update'
    updates_path = "/user/hive/warehouse/source_updates/"
    target_path = "/user/hive/warehouse/target"

    #One or more primary keys are required
    target_keys = ["id1","id2"]
    
    #Columns can be excluded from being compared for changes
    hash_exclude_columns = ["value2"]
    
    #Define a DataFrame by loading some source data
    updates_df = spark.read.load(path= updates_path,format = "delta")
    
    #Create MERGE statement and Execute SCD on target
    exec_scd(updates_df,target_path,target_keys,hash_exclude_columns)
```

## Test Data

The following will generate some test data with two primary keys `["id1","id2"]`

```sql
%sql
-- Test Case 1 - Build dataset for initial table
drop table if exists source;
create table source using delta as (select 1 as id1, 1 as id2, 'a' as value1, 'a' as value2);
insert into source (select  2 as id1, 2 as id2, 'b' as value1, 'b' as value2);
insert into source (select 3 as id1, 3 as id2, 'c' as value1, 'c' as value2);
insert into source (select 4 as id1, 4 as id2, 'd' as value1, 'd' as value2); -- removed in source_changes
insert into source (select 5 as id1, 5 as id2, 'e' as value1, 'e' as value2);
optimize source;

-- Test Case 2 - Build dataset for changes
drop table if exists source_updates;
create table source_updates using delta as (select 1 as id1, 1 as id2, 'a2' as value1, 'a' as value2); -- value1 change
insert into source_updates (select  2 as id1, 2 as id2, null as value1, 'b' as value2); -- value1 -> null change
insert into source_updates (select 3 as id1, 3 as id2, 'c' as value1, 'c' as value2); -- no change
insert into source_updates (select 2 as id1, 1 as id2, 'j' as value1, 'j' as value2); -- variant pk value (new row)
insert into source_updates (select 5 as id1, 5 as id2, 'e' as value1, 'e2' as value2); -- value2 change
optimize source_updates;
```
## Test Output
| id1 | id2 | value1 | value2 | scd_latest_flag | scd_start_date               | scd_end_date                 | scd_hash_key                             |
|-----|-----|--------|--------|-----------------|------------------------------|------------------------------|------------------------------------------|
| 1   | 1   | a      | a      | H               | 2019-06-18T21:26:22.922+0000 | 2019-06-18T21:28:18.658+0000 | ae3747b408d2eec845a9e8a4e4168d889570e884 |
| 1   | 1   | a2     | a      | C               | 2019-06-18T21:28:19.658+0000 | 9999-12-31T00:00:00.000+0000 | 1266a6cc602cdddd1382f456cbbd24927d0b9c2b |
| 2   | 2   | b      | b      | H               | 2019-06-18T21:26:22.922+0000 | 2019-06-18T21:28:18.658+0000 | 2314b42f90e65298dc483e7de55cd4c924989c9e |
| 2   | 1   | j      | j      | C               | 2019-06-18T21:28:19.658+0000 | 9999-12-31T00:00:00.000+0000 | 79ac9ef3a87ccb96326a43bb8fe229426e8a1031 |
| 2   | 2   | null   | b      | C               | 2019-06-18T21:28:19.658+0000 | 9999-12-31T00:00:00.000+0000 | 62272d79b532033d6f6d0380fa12c7dbc52ed89a |
| 3   | 3   | c      | c      | C               | 2019-06-18T21:26:22.922+0000 | 9999-12-31T00:00:00.000+0000 | 088e0fdee1f95fc48278314958b7f3e66ab382de |
| 4   | 4   | d      | d      | D               | 2019-06-18T21:26:22.922+0000 | 2019-06-18T21:28:18.658+0000 | 3ba62d69189bceccab8bd65c2ce3315c481aea81 |
| 5   | 5   | e      | e      | C               | 2019-06-18T21:26:22.922+0000 | 9999-12-31T00:00:00.000+0000 | 271decabf5382431cf868c0e1a9bc753fb6c67fc |
