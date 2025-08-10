import duckdb
import polars as pl
import pyarrow as pa


def read_whatever_format_return_arrow() -> pa.Table:
    """
    Reads a DataFrame and returns it as an Arrow Table.
    Now it uses Polars to create the DataFrame, but could be whatever.
    Then this pl.ArrowTable will be passed and used by all other engines
    """
    df = pl.DataFrame({"foo": [1, 2, 3], "bar": ["ham", "spam", "jam"]})
    arrow_table = df.to_arrow()
    return arrow_table


def execute_sql_engine(engine: str, table: pa.Table, query: str) -> bool:
    """
    Executes a SQL query on the specified engine using the provided Arrow Table.
    No need to reach for the data again

    """
    match engine:
        case "duckdb":
            # Execute the query using DuckDB
            con = duckdb.connect()
            con.register("table1", table)  # Register the Arrow Table
            # From Arrow to DuckDB
            try:
                a = con.execute(query)
                print(a.fetchall())  # Fetch and print the results
            except Exception as e:
                print(f"Error executing query: {e}")
            return True
        case "polars":
            # Execute the query using Polars
            df = pl.from_arrow(table)  # Convert Arrow Table to Polars DataFrame
            df = df.lazy().select(pl.all()).collect()
            print(df)
            return True
        case _:
            return False


def main():
    arrow_table = read_whatever_format_return_arrow()
    query = "SELECT * FROM table1"
    execute_sql_engine("duckdb", arrow_table, query)
    execute_sql_engine("polars", arrow_table, query)


if __name__ == "__main__":
    main()
