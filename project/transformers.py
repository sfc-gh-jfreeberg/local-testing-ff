from snowflake.snowpark.dataframe import DataFrame, col
from snowflake.snowpark.functions import monthname, avg, cast, count
import datetime


def add_rider_age(df: DataFrame) -> DataFrame:
    current_year = datetime.date.today().year
    return df.with_column('RIDER_AGE', current_year - df['BIRTH_YEAR'])
    

def calc_month_facts(df: DataFrame) -> DataFrame:
    """
    Group by month and return summary stats
    """
    return df.with_column('month', monthname('STARTTIME'))\
        .group_by('month')\
        .agg(
            count('*').alias('COUNT'), 
            avg('TRIPDURATION').alias('AVG_TRIPDURATION'), 
            avg('RIDER_AGE').alias('AVG_RIDER_AGE'))


def calc_bike_facts(df: DataFrame) -> DataFrame:
    """
    Group by bike ID and return summary statistics
    """
    return df.group_by('BIKEID')\
        .agg(
            count('*').alias('COUNT'), 
            avg('TRIPDURATION').alias('AVG_TRIPDURATION'), 
            avg('RIDER_AGE').alias('AVG_RIDER_AGE'))
