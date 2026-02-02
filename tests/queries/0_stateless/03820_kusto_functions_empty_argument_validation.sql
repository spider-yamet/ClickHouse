-- Tags: no-fasttest
-- Test for issue #95509: Kusto functions should validate empty arguments properly
-- This test ensures that functions bin(), bin_at(), extract(), and indexof() 
-- throw proper exceptions instead of crashing when given empty arguments.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

-- Test bin() function with empty bin size argument
-- Expected: Should throw BAD_ARGUMENTS exception, not crash
print bin(4.5,,); -- { clientError BAD_ARGUMENTS }

-- Test bin_at() function with empty bin size argument  
-- Expected: Should throw BAD_ARGUMENTS exception, not crash
print bin_at(datetime(2017-05-15 10:20:00.0), 10.5,, 5.0); -- { clientError BAD_ARGUMENTS }

-- Test extract() function with empty capture group argument
-- Expected: Should throw BAD_ARGUMENTS exception, not crash
print extract("User: ([^,]+)",, "User: James, Email: James@example.com, Age: 29"); -- { clientError BAD_ARGUMENTS }

-- Test indexof() function with empty start_index argument
-- Expected: Should throw BAD_ARGUMENTS exception, not crash
print indexof("hello world", "world",,); -- { clientError BAD_ARGUMENTS }

-- Test indexof() function with empty length argument
-- Expected: Should throw BAD_ARGUMENTS exception, not crash
print indexof("hello world", "world", 0,,); -- { clientError BAD_ARGUMENTS }

-- Test indexof() function with empty occurrence argument
-- Expected: Should throw BAD_ARGUMENTS exception, not crash
print indexof("hello world", "world", 0, 11,,); -- { clientError BAD_ARGUMENTS }
