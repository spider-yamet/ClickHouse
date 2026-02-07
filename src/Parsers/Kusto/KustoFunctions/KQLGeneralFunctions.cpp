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
extern const int SYNTAX_ERROR;
}

namespace DB
{

bool Bin::convertImpl(String & out, IParser::Pos & pos)
{
    double bin_size;
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    if (!pos.isValid() || pos->type != TokenType::OpeningRoundBracket)
        return false;

    IParser::Pos peek_pos = pos;
    ++peek_pos;
    // Validate empty argument BEFORE calling getConvertedArgument
    if (peek_pos->type == TokenType::Comma || peek_pos->type == TokenType::ClosingRoundBracket)
        return false;

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
        return false;

    ++pos;

    String round_to = getConvertedArgument(fn_name, pos);

    // Validate that the second argument is not empty (getConvertedArgument returns empty string for comma/closing bracket)
    if (round_to.empty())
        return false;

    //remove space between minus and number
    round_to.erase(std::remove_if(round_to.begin(), round_to.end(), isspace), round_to.end());

    auto t = fmt::format("toFloat64({})", value);

    try
    {
        bin_size = std::stod(round_to);
    }
    catch (const std::exception &)
    {
        return false;
    }

    // validate if bin_size is a positive number
    if (bin_size <= 0)
        return false;

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
        return false;

    ++pos;
    // Check if second argument is empty
    if (!pos.isValid() || pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket)
        return false;

    String second_arg = getConvertedArgument(fn_name, pos);

    // Validate that the second argument is not empty (getConvertedArgument returns empty string for comma/closing bracket)
    if (second_arg.empty())
        return false;

    ++pos;
    // Check if third argument is empty
    if (!pos.isValid() || pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket)
        return false;

    String third_arg = getConvertedArgument(fn_name, pos);

    // Validate that the third argument is not empty (getConvertedArgument returns empty string for comma/closing bracket)
    if (third_arg.empty())
        return false;

    // Determine if this is 3-arg or 4-arg form
    // getConvertedArgument() leaves pos at the comma (if 4-arg) or closing bracket (if 3-arg)
    if (!pos.isValid())
        return false;

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
            return false;

        String fourth_arg = getConvertedArgument(fn_name, pos);
        if (fourth_arg.empty())
            return false;

        expression_str = second_arg;
        bin_size_str = third_arg;
        fixed_point_str = fourth_arg;
    }
    else
    {
        return false;
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
        return false;
    }

    // validate if bin_size is a positive number
    if (bin_size <= 0)
        return false;

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
