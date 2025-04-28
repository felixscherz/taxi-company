python_sources(name="lib")

pex_binary(
    name="bin",
    dependencies=[
        ":lib",
        "//thirdparty/python:reqs#pandas",
    ],
    entry_point="benchmarks.duckdb",
)
