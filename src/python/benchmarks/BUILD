python_sources(name="lib")

pex_binary(
    name="bin",
    dependencies=[
        ":lib",
        "//3rdparty/python:reqs#pandas",
    ],
    entry_point="benchmarks.duckdb",
)

pex_binary(
    name="pandas_bin",
    dependencies=[
        ":lib",
        "//3rdparty/python:reqs#pandas",
    ],
    entry_point="benchmarks.pandas",
)
