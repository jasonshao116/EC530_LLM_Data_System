# SQL/DB Validator Debug Note

This note documents one concrete case where the first implementation was wrong,
the tests exposed the bug, and the validator was refined.

## Requirement Demonstration

### 1. The initial implementation was incorrect

The validator originally had a bug in its `JOIN` table/alias parsing logic in
[src/sql_validator.py](/Users/jshao116/Documents/BU/EC530/EC530_LLM_Data_System/src/sql_validator.py).

For this query:

```sql
SELECT name
FROM students
JOIN students_copy
ON students.student_id = students_copy.student_id
```

the validator should reject the query because `name` is ambiguous: both tables
contain a `name` column, so the user must qualify it as `students.name` or
`students_copy.name`.

However, the earlier implementation incorrectly parsed the `JOIN` section and
treated `students_copy` as an unknown alias. That produced the wrong error:

```text
Unknown table or alias referenced: students_copy
```

### 2. The tests caught the issue

The unit test
[tests/test_sql_validator.py](/Users/jshao116/Documents/BU/EC530/EC530_LLM_Data_System/tests/test_sql_validator.py)
includes:

- `test_rejects_ambiguous_column`

That test builds a join between `students` and `students_copy` and expects the
validator to reject the query for ambiguity, not for an unknown alias.

When this test was first run, it failed because the validator returned the wrong
error message.

### 3. The implementation was refined

The fix was to refine the regex used by `_extract_table_aliases(...)` so it
correctly recognizes table names and aliases around `FROM ... JOIN ... ON ...`
clauses.

After the fix:

- the validator correctly recognizes `students_copy` as a real joined table
- the ambiguous unqualified column `name` is detected properly
- the returned message is now:

```text
Ambiguous column referenced: name. Qualify it with a table name.
```

### 4. Final verification

Run:

```bash
python3 -m unittest -v tests/test_sql_validator.py
```

Expected result:

- all validator tests pass
- the ambiguous-column test passes specifically because the refined
  implementation now rejects the query for the correct reason

## Summary

This satisfies the extra requirement because:

- the first implementation was incorrect
- the test suite caught the issue
- the implementation was refined and verified afterward
