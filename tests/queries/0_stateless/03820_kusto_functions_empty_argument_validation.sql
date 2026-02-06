
SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

-- Test bin() function with empty first argument
-- Expected: Should throw SYNTAX_ERROR exception, not crash
print bin(, 1.5); -- { clientError SYNTAX_ERROR }

-- Test extract() function with empty capture group argument
-- Expected: Should throw BAD_ARGUMENTS exception, not crash
print extract("User: ([^,]+)",, "User: James, Email: James@example.com, Age: 29"); -- { clientError BAD_ARGUMENTS }
