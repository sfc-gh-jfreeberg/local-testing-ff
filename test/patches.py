from snowflake.snowpark.mock.functions import patch
from snowflake.snowpark.functions import monthname
from snowflake.snowpark.mock.snowflake_data_type import ColumnEmulator
from snowflake.snowpark.types import TimestampType
import datetime
import calendar

@patch(monthname)
def patch_to_timestamp(column: ColumnEmulator) -> ColumnEmulator:
    ret_column = ColumnEmulator(data=[
        calendar.month_abbr[datetime.datetime.strptime(row, '%Y-%m-%d %H:%M:%S.%f %z').month]
        for row in column])
    ret_column.sf_type = TimestampType()
    return ret_column