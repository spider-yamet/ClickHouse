
SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

-- Test bin() function with empty first argument
-- Expected: Should throw NUMBER_OF_ARGUMENTS_DOESNT_MATCH exception, not crash
print bin(, 1.5); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
