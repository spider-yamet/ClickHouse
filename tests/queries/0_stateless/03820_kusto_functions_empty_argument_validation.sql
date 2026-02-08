
SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print bin(, 1.5); -- { error SYNTAX_ERROR, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
