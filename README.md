# Arrow_as_source
Small demo to show Apache Arrow can be used as a "universal" dataset. In this simple example we "read" a data frame only once in 'read_whatever_format_return_arrow' and then we return the data data set as as pa.Table (pyarrow table)


## Run the project

You can run the main script using the following shell command:

```sh
uv run main.py
```




