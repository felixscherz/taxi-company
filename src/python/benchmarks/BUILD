python_sources(name="lib")
pex_binary(
    name="bin",
    entry_point="benchmarks.duckdb",
    dependencies=[":lib", "//thirdparty/python:reqs#pandas"]
)
