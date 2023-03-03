import pyodbc
import psycopg2
import tqdm
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import urllib


def create_db_connection(
    server: str,
    port: int,
    username: str,
    password: str,
    database: str,
    driver: str,
    fast_executemany=True,
):
    """
    ODBC 연결을 위한 engine을 얻는 함수
    """

    server = server
    if (driver == "mssql") | (driver == "sql server"):
        quoted = urllib.parse.quote_plus(
            "DRIVER={" + pyodbc.drivers()[0] + "};"  # For Connection
            f"SERVER={server};"
            f"PORT={port};"
            f"DATABASE={database};"
            f"UId={username};"
            f"PWD={password};"
        )
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={quoted}",
            fast_executemany=fast_executemany,
        )
        result_set = engine.execute("SELECT 'Connected' AS result")
        for row in result_set:
            print(row["result"])
        return engine
    elif driver == "postgresql":
        engine = create_engine(
            f"postgresql+psycopg2://{username}:{password}@{server}:{port}/{database}"
        )
        result_set = engine.execute("SELECT 'Connected' AS result")
        for row in result_set:
            print(row["result"])
        return engine


def chunker(seq, size):
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def insert_with_progress(df, name, schema, chunksize, dtype, engine):
    with tqdm(total=len(df)) as pbar:
        for chunked_df in chunker(df, chunksize):
            chunked_df.to_sql(
                con=engine,
                name=name,
                if_exists="append",
                dtype=dtype,
                index=False,
                chunksize=chunksize,
            )

            pbar.update(chunksize)


def column_transformer(df: pd.DataFrame, domain: str, col_type) -> pd.DataFrame:
    """
    column의 데이터형식에 맞게 변형하는 함수...
    만약에 변형할 수 없는 데이터의 경우 None or NaN으로 바꿔버림
    ex) 1.3t -> NaN
    """
    datetime_error, float_error, int_error, str_error = 0, 0, 0, 0
    for col in df.columns:
        col = col.lower()
        dtype = col_type.get(col)
        if dtype == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)
            datetime_error += 1
        elif dtype in ["float", "int"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(
                dtype, errors="ignore"
            )
            if dtype == "float":
                float_error += 1
            else:
                int_error += 1
        elif dtype == "str":
            df[col] = df[col].astype(str, errors="ignore")
            str_error += 1
        else:
            print(f"col_type not defined : {col} removed")

    print(datetime_error, float_error, int_error, str_error)
    return df


def auto_processing(
    df: pd.DataFrame,
    domain: str = "measurement",
    fulltime=False,
    require_col=None,
    domain_col=None,
    col_type=None,
):
    """
    df의 완결성 유지 위하여
        1. 필수 컬럼에 missing 값 있는 경우, 해당 row 제거
        2. 모든 컬럼에 column_transformer 적용

    fulltime= True시에 모든 기록을 남김. False시에는 지난 40일간의 기록만 남김

    concept_id 0인 것들도 지워버림.
    """
    df.columns = map(str.lower, df.columns)
    required_cols = set(require_col[domain]) & set(df.columns)
    if len(required_cols) != len(require_col[domain]):
        df = df.dropna(subset=required_cols)

    df = column_transformer(df, domain=domain, col_type=col_type)

    starttime, endtime = (
        (datetime.min, datetime.max)
        if fulltime
        else (datetime.now() - timedelta(days=40), datetime.now())
    )
    df = df[
        (df[domain_col[domain]["datetime"]] > starttime)
        & (df[domain_col[domain]["datetime"]] < endtime)
    ]
    return df[df[domain_col[domain]["concept_id"]] != 0]
