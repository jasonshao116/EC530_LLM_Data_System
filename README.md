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

You can verify the imported rows with:

```bash
sqlite3 students.db "SELECT * FROM students;"
```

## Re-enter the environment later

When you return to the project, reactivate the virtual environment before running the loader:

```bash
source .venv/bin/activate
```
