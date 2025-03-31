from pyspark.sql.functions import col, date_sub, current_date
from pyspark.sql import DataFrame

def investigate_time_travel_data(table_name, date_to_go_back, filter_condition='1=1'):
    """
    Investigates the counts of rows in a Delta table based on a filter condition and a date range.

    Parameters:
    table_name (str): The name of the Delta table.
    filter_condition (str): The condition to filter the rows in the table.
    date_to_go_back (str): The start date to filter the history records.

    Returns:
    DataFrame: A DataFrame containing the version number, timestamp, operation, and count of rows 
               that match the filter condition for each version in the specified date range, 
               ordered by timestamp in descending order.
    """
    history_df = spark.sql(f"DESCRIBE HISTORY {table_name}").filter(
        col("timestamp").cast("date").between(date_to_go_back, current_date()) &
        (~col("operation").contains("VACUUM"))
    )
    result_df = None
    for row in history_df.collect():
        try:
            version_nbr = row['version']
            timestamp = row['timestamp'] 
            operation = row['operation']
            time_travel_qry = f"""select {version_nbr} as version_nbr, '{timestamp}' as timestamp, '{operation}' as operation, count(*) as count 
                                from {table_name} version as of {version_nbr}
                                where {filter_condition}"""
            temp_df = spark.sql(time_travel_qry)
            result_df = temp_df if result_df is None else result_df.union(temp_df)
        except Exception as e:
            print(f"Error processing version {version_nbr}: {e}. The version might be corrupt or the filter condition might not work for the table {table_name}. Please revisit the query or check the catalog explorer for history.\n")
            return None
    return result_df.orderBy(col("timestamp").desc())
    
table_name = "catalog.schema.table" # use actual table here
filter_condition = "promo_id = '1234'" # use actual filter here
date_to_go_back = '2025-03-20'
display(investigate_time_travel_data(table_name, date_to_go_back, filter_condition))