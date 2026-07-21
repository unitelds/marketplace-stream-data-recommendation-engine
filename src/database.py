import pandas as pd
from sqlalchemy import create_engine


def get_pg_cred():
    with open("meta/pg.cred", "r") as file:
        lines = file.readlines()

    username = lines[0].strip().replace("\n", "")
    password = lines[1].strip().replace("\n", "")
    ip_address = lines[2].strip().replace("\n", "")

    return username, password, ip_address


def set_pgsql_connection():
    """Build oracle connection string for SQL alchemy engine"""

    username, password, ip_address = get_pg_cred()
    db_engine_path = (
        f"postgresql+psycopg2://{username}:{password}@{ip_address}:5432/postgres"
    )
    return db_engine_path


def pgsql_import(query):
    """Import from oracle DB"""

    engine = create_engine(set_pgsql_connection())
    data_frame = pd.read_sql(query, engine)
    return data_frame


def pgsql_export(
    data,
    table_name="tmp_catalog_sync",
    if_exists="append",
    chunksize=1000,
):
    """
    table export method to postgres
    -----


    """

    engine = create_engine(set_pgsql_connection())

    with engine.begin() as connection:
        data.to_sql(
            table_name,
            con=connection,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
        )

    engine.dispose()

    return data


def execute_sql(query):
    """
    Execute SQL query directly on PostgreSQL database

    Parameters:
    -----------
    query : str
        SQL query to execute (INSERT, UPDATE, DELETE, etc.)

    Returns:
    --------
    dict
        Dictionary containing execution results and row count
    """

    engine = create_engine(set_pgsql_connection())

    try:
        with engine.connect() as connection:
            # Begin a transaction
            trans = connection.begin()
            try:
                # Execute the query
                result = connection.execute(query)

                # Get the number of affected rows
                rowcount = result.rowcount if hasattr(result, 'rowcount') else 0

                # Commit the transaction
                trans.commit()

                return {
                    "success": True,
                    "message": f"Query executed successfully. {rowcount} rows affected.",
                    "rows_affected": rowcount,
                }

            except Exception as e:
                # Rollback the transaction on error
                trans.rollback()
                raise e

    except Exception as e:
        return {
            "success": False,
            "message": f"Error executing query: {e!s}",
            "rows_affected": 0,
        }
