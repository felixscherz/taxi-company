relocated_files(
    name="cassettes",
    files_targets=["./tests/cassettes/test_ingestions:cassette"],
    src="",
    dest=""
)
python_sources(
    overrides={
        "mock.py": {
            "dependencies": [":cassettes"]
        }
    }
)
