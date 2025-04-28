python_sources(name="lib")

pex_binary(
    name="bin",
    dependencies=[
        ":lib",
        "//3rdparty/python:reqs#pandas",
    ],
    entry_point="benchmarks.duckdb",
)
