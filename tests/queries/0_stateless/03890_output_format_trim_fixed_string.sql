SELECT toFixedString('John', 8) FORMAT TabSeparated;
SELECT toFixedString('John', 8) SETTINGS output_format_trim_fixed_string = 1 FORMAT TabSeparated;

SELECT toFixedString('abc\0def', 10) SETTINGS output_format_trim_fixed_string = 1 FORMAT TabSeparated;
SELECT toTypeName(toFixedString('John', 8)), length(toFixedString('John', 8)) FORMAT TabSeparated;

SELECT toFixedString('John', 8) AS s FORMAT JSONEachRow;
SELECT toFixedString('John', 8) AS s SETTINGS output_format_trim_fixed_string = 1 FORMAT JSONEachRow;
