#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/ParserKQLDateTypeTimespan.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSetQuery.h>
#include <Common/Exception.h>
#include <boost/lexical_cast.hpp>

#include <algorithm>
#include <cctype>
#include <fmt/format.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SYNTAX_ERROR;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace DB
{

bool Bin::convertImpl(String & out, IParser::Pos & pos)
{
    double bin_size;
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    // getKQLFunctionName() already advanced pos to the opening bracket '('
    if (!pos.isValid() || pos->type != TokenType::OpeningRoundBracket)
        return false;

    // Check if first argument is empty (comma or closing bracket immediately after opening bracket)
    IParser::Pos peek_pos = pos;
    ++peek_pos;
    // Validate empty argument BEFORE calling getConvertedArgument
    if (peek_pos.isValid() && (peek_pos->type == TokenType::Comma || peek_pos->type == TokenType::ClosingRoundBracket))
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "The first argument of `{}` should be valid argument.", fn_name);

    // Capture the first token for type checking (before getConvertedArgument advances pos)
    String origal_expr;
    if (peek_pos.isValid() && peek_pos->type != TokenType::Comma && peek_pos->type != TokenType::ClosingRoundBracket)
        origal_expr = String(peek_pos->begin, peek_pos->end);

    // Advance past the opening bracket to the first argument
    ++pos;


    // getConvertedArgument handles argument processing and advances pos to the comma/closing bracket
    String value = getConvertedArgument(fn_name, pos);

    // Validate that the first argument is not empty (getConvertedArgument returns empty string for comma/closing bracket)
    if (value.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "The first argument of `{}` should be valid argument.", fn_name);

    ++pos;

    String round_to = getConvertedArgument(fn_name, pos);

    // Validate that the second argument is not empty (getConvertedArgument returns empty string for comma/closing bracket)
    if (round_to.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "The second argument of `{}` should be valid argument.", fn_name);

    //remove space between minus and number
    round_to.erase(std::remove_if(round_to.begin(), round_to.end(), isspace), round_to.end());

    auto t = fmt::format("toFloat64({})", value);

    try
    {
        bin_size = std::stod(round_to);
    }
    catch (const std::exception &)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second argument of `{}` should be a valid number.", fn_name);
    }

    // validate if bin_size is a positive number
    if (bin_size <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second argument of `{}` should be a positive number.", fn_name);

    if (origal_expr == "datetime" || origal_expr == "date")
    {
        out = fmt::format("toDateTime64(toInt64({0}/{1}) * {1}, 9, 'UTC')", t, bin_size);
    }
    else if (origal_expr == "timespan" || origal_expr == "time" || ParserKQLDateTypeTimespan().parseConstKQLTimespan(origal_expr))
    {
        String bin_value = fmt::format("toInt64({0}/{1}) * {1}", t, bin_size);
        out = fmt::format(
            "concat(toString(toInt32((({}) as x) / 3600)),':', toString(toInt32(x % 3600 / 60)),':',toString(toInt32(x % 3600 % 60)))",
            bin_value);
    }
    else
    {
        out = fmt::format("toInt64({0} / {1}) * {1}", t, bin_size);
    }

    // Validate that we're at the closing bracket
    if (pos->type == TokenType::ClosingRoundBracket)
        return true;

    return false;
}

bool BinAt::convertImpl(String & out, IParser::Pos & pos)
{
    double bin_size;
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;

    // Capture the first token for type checking (before getConvertedArgument advances pos)
    String origal_expr;
    if (pos->type != TokenType::Comma && pos->type != TokenType::ClosingRoundBracket)
        origal_expr = String(pos->begin, pos->end);

    String first_arg = getConvertedArgument(fn_name, pos);

    // Validate that the first argument is not empty (getConvertedArgument returns empty string for comma/closing bracket)
    if (first_arg.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "The first argument of `{}` should be valid argument.", fn_name);

    ++pos;
    // Check if second argument is empty
    if (!pos.isValid() || pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "The second argument of `{}` shouldn't be empty.", fn_name);

    String second_arg = getConvertedArgument(fn_name, pos);

    // Validate that the second argument is not empty (getConvertedArgument returns empty string for comma/closing bracket)
    if (second_arg.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "The second argument of `{}` shouldn't be empty.", fn_name);

    ++pos;
    // Check if third argument is empty
    if (!pos.isValid() || pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires a non-empty bin size argument", fn_name);

    String third_arg = getConvertedArgument(fn_name, pos);

    // Validate that the third argument is not empty (getConvertedArgument returns empty string for comma/closing bracket)
    if (third_arg.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires a non-empty bin size argument", fn_name);

    // Determine if this is 3-arg or 4-arg form
    // getConvertedArgument() leaves pos at the comma (if 4-arg) or closing bracket (if 3-arg)
    if (!pos.isValid())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Function {} requires a valid argument structure", fn_name);

    String expression_str;
    String bin_size_str;
    String fixed_point_str;
    if (pos->type == TokenType::ClosingRoundBracket)
    {
        // 3-argument form: bin_at(expression, bin_size, fixed_point)
        expression_str = first_arg;
        bin_size_str = second_arg;
        fixed_point_str = third_arg;
    }
    else if (pos->type == TokenType::Comma)
    {
        // 4-argument form: bin_at(type_expr, expression, bin_size, fixed_point)
        ++pos; // Skip the comma
        // Check pos.isValid() first before accessing pos->type
        if (!pos.isValid() || pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Function {} requires a non-empty fixed point argument", fn_name);

        String fourth_arg = getConvertedArgument(fn_name, pos);
        if (fourth_arg.empty())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Function {} requires a non-empty fixed point argument", fn_name);

        expression_str = second_arg;
        bin_size_str = third_arg;
        fixed_point_str = fourth_arg;
    }
    else
    {
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Function {} requires a valid argument structure", fn_name);
    }

    auto t1 = fmt::format("toFloat64({})", fixed_point_str);
    auto t2 = fmt::format("toFloat64({})", expression_str);
    int dir = t2 >= t1 ? 0 : -1;

    try
    {
        bin_size = std::stod(bin_size_str);
    }
    catch (const std::exception &)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires a valid numeric bin size argument", fn_name);
    }

    // validate if bin_size is a positive number
    if (bin_size <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires a positive bin size argument", fn_name);

    if (origal_expr == "datetime" || origal_expr == "date")
    {
        out = fmt::format("toDateTime64({} + toInt64(({} - {}) / {} + {}) * {}, 9, 'UTC')", t1, t2, t1, bin_size, dir, bin_size);
    }
    else if (origal_expr == "timespan" || origal_expr == "time" || ParserKQLDateTypeTimespan().parseConstKQLTimespan(origal_expr))
    {
        String bin_value = fmt::format("{} + toInt64(({} - {}) / {} + {}) * {}", t1, t2, t1, bin_size, dir, bin_size);
        out = fmt::format(
            "concat(toString(toInt32((({}) as x) / 3600)),':', toString(toInt32(x % 3600 / 60)), ':', toString(toInt32(x % 3600 % 60)))",
            bin_value);
    }
    else
    {
        out = fmt::format("{} + toInt64(({} - {}) / {} + {}) * {}", t1, t2, t1, bin_size, dir, bin_size);
    }
    return true;
}

bool Iif::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String predicate = getConvertedArgument(fn_name, pos);
    if (predicate.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Number of arguments do not match in function: {}", fn_name);

    ++pos;
    String if_true = getConvertedArgument(fn_name, pos);
    if (if_true.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Number of arguments do not match in function: {}", fn_name);

    ++pos;
    String if_false = getConvertedArgument(fn_name, pos);
    if (if_false.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Number of arguments do not match in function: {}", fn_name);

    out = fmt::format("if({}, {}, {})", predicate, if_true, if_false);
    return true;
}

}
