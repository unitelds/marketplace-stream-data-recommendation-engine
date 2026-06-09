import math

import numpy as np
import oracledb
import pandas as pd
from sqlalchemy import URL, create_engine, types
from sqlalchemy.sql import text
from trino.auth import BasicAuthentication

from src.module.helper import logging_timer
from src.module.settings import settings

# oracle connection string for SQL alchemy engine
oracledb.init_oracle_client(
    lib_dir="/opt/oracle/instantclient_21_6"  # path to instant client
)

oracle_connection_url = URL.create(
    drivername='oracle+oracledb',
    password=settings.oracle_password,
    username=settings.oracle_username,
    port=settings.oracle_port,
    host=settings.oracle_hostname,
    query={'service_name': settings.oracle_service},
)
TRINO_URL = f"trino://{settings.trino_username}@{settings.trino_hostname}:{settings.trino_port}/{settings.trino_service}"


def get_trino_engine():
    return create_engine(
        TRINO_URL,
        connect_args={
            "auth": BasicAuthentication(
                settings.trino_username, settings.trino_password
            ),
            "http_scheme": "https",
        },
    )


def col_length(str_len):
    """Calculate column lengths. Used for allocating column size"""
    # threshold = 2**(pw-1)
    if (
        str_len is None
        or (isinstance(str_len, float) and np.isnan(str_len))
        or str_len <= 0
    ):
        return 1
    if str_len == 1:
        return 1
    pw = int(math.log(str_len, 2))
    pw = max(pw, 1)
    return int(math.ceil((str_len + 2 ** (pw - 1)) / 2 ** (pw - 1)) * 2 ** (pw - 1))


def sql_col(data_frame):
    """Convert python data types to sql data types"""
    dtypes_dict = {}

    for col in data_frame.columns:
        s = data_frame[col]

        # If duplicate column names exist, df[col] becomes a DataFrame -> take first
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]

        if pd.api.types.is_datetime64_any_dtype(s):
            dtypes_dict[col] = types.DateTime()

        elif pd.api.types.is_float_dtype(s):
            dtypes_dict[col] = types.FLOAT

        elif pd.api.types.is_integer_dtype(s):
            dtypes_dict[col] = types.INT()

        else:
            # object / string / mixed -> store as VARCHAR
            # compute max string length safely
            # (works even if values are lists/dicts/numbers; they get str() for sizing)
            max_len = s.dropna().map(lambda v: len(str(v))).max()
            max_len = col_length(int(max_len) if max_len is not None else 1)
            dtypes_dict[col] = types.VARCHAR(length=max_len)

    return dtypes_dict


def sql_open(filepath):
    """Load sql file
    Do not include ; in a query
    Only one query in a file is allowed
    """
    query = open(filepath, encoding='utf-8').read()
    return query


@logging_timer()
def oracle_export(
    data_frame, table_name, index=False, if_exists='replace', chunksize=5000
):
    engine = create_engine(oracle_connection_url)
    output_dtypes_dict = sql_col(data_frame)

    data_frame.to_sql(
        table_name.lower(),
        con=engine,
        if_exists=if_exists,
        index=index,
        dtype=output_dtypes_dict,
        chunksize=chunksize,  # <-- key fix
    )


@logging_timer()
def oracle_execute(query):
    """Executes query"""
    engine = create_engine(oracle_connection_url)
    with engine.connect() as connection:
        connection.execute(text(query))
        connection.commit()


@logging_timer()
def oracle_import(query):
    """Import from oracle DB"""
    engine = create_engine(oracle_connection_url).raw_connection()
    data_frame = pd.read_sql(query, engine)
    return data_frame


@logging_timer()
def oracle_sysdate(before_today=0):
    """Get current sysdate from Oracle DB"""
    query = f"SELECT TO_CHAR(SYSDATE - {before_today}, 'YYYYMMDD') FROM dual"
    return oracle_import(query).iloc[0, 0]


@logging_timer()
def trino_import(query):
    """Import from trino DB"""
    engine = get_trino_engine()
    data_frame = pd.read_sql(query, engine)
    return data_frame
