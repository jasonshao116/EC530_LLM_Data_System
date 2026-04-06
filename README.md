# EC530_LLM_Data_System

## Environment setup

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

Verify that `pandas` is available:

```bash
python3 -c "import pandas; print(pandas.__version__)"
```

## Run the data loader

A sample CSV is included at `src/example_data.csv`.

Run the loader from the project root:

```bash
python3 src/data_loader.py src/example_data.csv students.db students
```

This command:

- reads `src/example_data.csv`
- creates `students.db` if it does not already exist
- creates a table named `students`
- inserts the CSV rows into that table

If a table already exists with a different schema, the loader now checks the
existing structure with `PRAGMA table_info()` and lets you choose whether to
`overwrite`, `rename`, or `skip`.

Example with an explicit policy:

```bash
python3 src/data_loader.py src/example_data.csv students.db students --if-exists append --on-schema-conflict rename
```

Any runtime errors are appended to `error_log.txt` in the project root.

To use schema matching mode from Part 3, run:

```bash
python3 src/data_loader.py src/example_data.csv students.db students --match-existing-schema
```

In this mode, the loader:

- appends to an existing table if normalized column names and SQLite data types match exactly
- creates a new table if no existing table matches

Newly created tables also include an auto-generated primary key column:

```sql
id INTEGER PRIMARY KEY AUTOINCREMENT
```

You can verify the imported rows with:

```bash
sqlite3 students.db "SELECT * FROM students;"
```

## Basic queries

Once the data is loaded, you can run basic SQLite queries from the command line.

Show all rows:

```bash
sqlite3 students.db "SELECT * FROM students;"
```

Use `WHERE` to filter rows:

```bash
sqlite3 students.db "SELECT name, gpa FROM students WHERE gpa > 3.5;"
```

Use `LIMIT` to return only a few rows:

```bash
sqlite3 students.db "SELECT * FROM students LIMIT 2;"
```

## Query Service

Run the interactive CLI:

```bash
python3 src/query_service.py students.db
```

Available commands:

- `load` to import a CSV into the database
- `tables` to list tables from `sqlite_master`
- `query` to validate and run a SQL query
- `ask` to translate a natural-language request into SQL, validate it, and only then execute it
- `exit` to leave the CLI

The SQL validator only allows a single `SELECT` statement. It rejects:

- non-`SELECT` queries
- unknown tables
- unknown columns
- ambiguous unqualified columns across joined tables

## LLM Adapter

The LLM adapter is implemented in
[src/llm_adapter.py].
It:

- collects table schema context
- builds a prompt from the schema plus the user's request
- proposes SQL through a pluggable generator
- does **not** execute SQL

The query service treats the generated SQL as untrusted input and sends it
through the validator before execution.

Run adapter and validator tests with:

```bash
python3 -m unittest discover -s tests -v
```

## CI

GitHub Actions runs the unit test suite automatically on pushes and pull
requests using
[.github/workflows/tests.yml].

Run the validator unit tests with:

```bash
python3 -m unittest tests/test_sql_validator.py
```

See [VALIDATOR_DEBUG_NOTE.md] for a concrete example where a validator bug was caught by tests and then fixed.

## Re-enter the environment later

When you return to the project, reactivate the virtual environment before running the loader:

```bash
source .venv/bin/activate
```

## System Review Video

[System Review Video](https://youtu.be/oJiVe7irF4I)
