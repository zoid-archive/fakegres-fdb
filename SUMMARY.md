# Branch Summary: feature/comprehensive-sql-support

This branch adds comprehensive SQL support to fakegres-fdb.

## Changes

- **Upgraded pg_query_go to v5** with comprehensive test suite and fixed SELECT column ordering
- **SELECT support**: Implemented `SELECT *` and `WHERE` clause support
- **DELETE/UPDATE support**: Implemented `DELETE WHERE` and `UPDATE` statements
- **DROP TABLE support**: Added ability to drop tables
- **ORDER BY and LIMIT/OFFSET**: Implemented result ordering and pagination
- **INSERT enhancements**: Added column list support, NULL values, and boolean support
- **TRUNCATE TABLE support**: Implemented table truncation
- **Test coverage**: Added comprehensive test cases for all new functionality
