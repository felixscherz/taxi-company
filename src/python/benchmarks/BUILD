python_sources(
    name="duckdb_src",
    sources=["duckdb.py"],
)

python_sources(
    name="pandas_src",
    sources=["pandas.py"],
)

python_sources(
    name="polars_src",
    sources=["polars.py"],
)

pex_binary(
    name="bin",
    dependencies=[
        ":duckdb_src",
        "//3rdparty/python:reqs#pandas",
    ],
    entry_point="benchmarks.duckdb",
)

pex_binary(
    name="pandas_bin",
    dependencies=[
        ":pandas_src",
        "//3rdparty/python:reqs#pandas",
        "//3rdparty/python:reqs#pyarrow",
    ],
    entry_point="benchmarks.pandas",
)

pex_binary(
    name="polars_bin",
    dependencies=[
        ":polars_src",
        "//3rdparty/python:reqs#polars",
    ],
    entry_point="benchmarks.polars",
)
