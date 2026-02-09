
SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print bin(, 1.5); -- { error NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

print extract("User: ([^,]+)",, "User: James, Email: James@example.com, Age: 29"), -- { clientError SYNTAX_ERROR }


