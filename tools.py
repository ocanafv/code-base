import datetime as dt

import boto3
import pandas as pd
import psutil
import sys

sys.path.append("/home/fvieira/ft-data-monitoring/")
from config.model_conf import ETLS3Conf


def print_memory_timestamp():
    available_memory_percent = (
        psutil.virtual_memory().available * 100 / psutil.virtual_memory().total
    )
    available_memory_str = round(available_memory_percent, 2)
    total_memory_str = round(psutil.virtual_memory().total / 1000000, 2)
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"Available memory: {available_memory_str}% of {total_memory_str} MB -> Current Time = {now}"


def is_right_weekday(date: str, is_start: bool) -> None:
    weekday = dt.datetime.strptime(date, "%Y-%m-%d").strftime("%A")
    output = True
    out_str = None
    if is_start and weekday != "Sunday":
        out_str = f"`start_date` should be a Sunday (not a {weekday})"
        output = False

    if not is_start and weekday != "Saturday":
        out_str = f"`end_date` should be a Saturday (not a {weekday})"
        output = False

    if not output:
        print(out_str)

    return output


def create_file_name_dates(start_date: str, end_date: str) -> None:
    are_valid_dates = validate_dates(start_date, end_date)
    if are_valid_dates:
        start_dates_list = [
            str(date.date())
            for date in pd.date_range(start_date, end_date, freq="W-SUN")
        ]

        end_dates_list = [
            str(date.date())
            for date in pd.date_range(start_date, end_date, freq="W-SAT")
        ]
    else:
        start_dates_list, end_dates_list = None, None

    return are_valid_dates, start_dates_list, end_dates_list


def treat_time_frame(start_date: str, end_date: str):

    if (start_date is None) or (end_date is None):
        print(
            "No `start_date` and/or `end_date` were passed.\n"
            "The time frame for the last week available will be used"
        )
        raise NotImplementedError(
            "Default valur for time frame is not provided. \
            Please explicitly pass `start_date` and `end_date` arguments."
        )
    # Return (False, None, None) if dates are not valid
    are_valid_dates, start_dates_list, end_dates_list = create_filename_dates(
        start_date, end_date
    )

    return (are_valid_dates, start_dates_list, end_dates_list)


def validate_dates(start_date, end_date):
    # Checking validity of `start_date` and `end_date`
    start_valid = is_right_weekday(start_date, is_start=True)
    end_valid = is_right_weekday(end_date, is_start=False)
    out = start_valid and end_valid

    return out


def create_date_partition_filter(start_date, end_date):
    conf = ETLS3Conf()
    boto3.setup_default_session(profile_name="etlS3user")
    s3 = boto3.resource("s3")
    project_objects = s3.Bucket(conf.s3_ds_bucket_name).objects.filter(
        Prefix=conf.s3_key_pre_process[1:]
    )
    file_name_dates = create_file_name_dates(
        start_date=start_date, end_date=end_date
    )
    are_valid_dates = file_name_dates[0]
    if not are_valid_dates:
        print(
            "FAIL: Please retry with dates that match the weekly partitions."
        )
        return

    out = [
        obj.key
        for obj in project_objects
        if any([date in obj.key for date in file_name_dates[1]])
    ]

    return out
