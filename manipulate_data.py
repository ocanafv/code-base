# from numpy import partition
import yaml
import os
import pandas as pd
import os
import subprocess
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Optional

# from typing import Protocol #TODO add when you have a personal env
from abc import ABC, abstractmethod

# LOAD / SAVE FILES


class SaveData(ABC):
    def __init__(
        self,
        df: pd.DataFrame,
        path: str,
        partitions: Optional[list] = None,
    ):
        self.df = df
        self.path = path
        self.partitions = partitions

    @abstractmethod
    def save(self):
        """Saves data from memory to local folder."""
        pass


class SaveCSV(SaveData):
    def save(self):
        self.df.to_csv(self.path, index=False, header=True, encoding="utf-8")


class SaveParquet(SaveData):
    def save(self):
        data = pa.Table.from_pandas(self.df)
        pq.write_to_dataset(
            data,
            root_path=self.path,
            partition_cols=self.partitions,
        )


def save_data_locally(
    df: pd.DataFrame,
    local_path: str,
    format: str,
    partitions: Optional[list] = None,
) -> None:
    """Saves data localy."""
    file_format = {
        "csv": SaveCSV(df, local_path, partitions).save,
        "parquet": SaveParquet(df, local_path, partitions).save,
    }
    file_format[format]()


def load_config_data_from_local(file_path: str):
    """Loads data from local folders, i.e. from the current project."""
    if os.path.isfile(file_path):
        with open(file_path) as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
        f.close()
        return data
    else:
        raise Exception(f"file {file_path} does not exist.")


def upload_from_local_to_s3(
    source_path: str,
    s3_path: str,
    s3_profile: str,
    is_directory: Optional[bool] = False,
    delete_local: Optional[bool] = True,
) -> None:
    """
    Uploads file to S3.
    :param source_path: from
    :param s3_path: to
    :param s3_profile: profile is a must. Depending on profile you can save data to Feedzai bucket and to ft-ds-bucket
    :is_directory: define if passed path is a directory to be trated recursively; e.g. for partitioned data
    """
    if is_directory:
        cp_command = f"aws s3 cp {source_path} s3://{s3_path} --profile {s3_profile} --recursive"
    else:
        cp_command = (
            f"aws s3 cp {source_path} s3://{s3_path} --profile {s3_profile}"
        )
    exit_code = subprocess.call(
        cp_command,
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )

    if exit_code == 0 and delete_local:  # success + deletion request
        if is_directory:
            # since we don't have shutil in the production venv
            rm_command = f"rm -rf {source_path}"
            os.system(rm_command)
            print(rm_command)
        else:
            os.remove(source_path)


def get_data_from_redshift(query: str, db_conn) -> pd.DataFrame:
    # TODO add Protocol
    """
    Loads data from RedShift using a query.
    :param query: sql query
    :param db_conn: credentials. For now they are available only on the server where Jenkins and Airflow are hosted
    :return: pandas DataFrame
    """
    df = pd.read_sql(query, con=db_conn)
    db_conn.close()
    return df
