#include <charconv>
#include <string_view>
#include <algorithm>
#include <cctype>
#include <iterator>
#include <vector>
#include <iostream>

#include <Client/TestHint.h>

#include <Parsers/Lexer.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <base/find_symbols.h>

#include <fmt/ranges.h>

namespace DB::ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int OK;
}

namespace DB
{

namespace
{
    /// Check if query looks like KQL by checking for KQL-specific keywords
    /// Note: We only check for keywords that are unique to KQL or very KQL-specific,
    /// not common SQL keywords like "where", "order", "limit" that appear in both.
    bool isKQLQuery(const std::string_view & query)
    {
        static const std::vector<std::string_view> kql_keywords = {
            "print", "project", "extend", "summarize", "take", "make-series", "render"
        };

        String query_lower;
        query_lower.reserve(query.size());
        std::transform(query.begin(), query.end(), std::back_inserter(query_lower),
                       [](char c) { return std::tolower(static_cast<unsigned char>(c)); });

        for (const auto & keyword : kql_keywords)
        {
            size_t pos = query_lower.find(keyword);
            if (pos != String::npos)
            {
                if ((pos == 0 || !std::isalnum(static_cast<unsigned char>(query_lower[pos - 1]))) &&
                    (pos + keyword.size() >= query_lower.size() ||
                     !std::isalnum(static_cast<unsigned char>(query_lower[pos + keyword.size()]))))
                {
                    return true;
                }
            }
        }

        // Check for functions that are truly unique to KQL
        static const std::vector<std::string_view> kql_unique_functions = {
            "bin", "bin_at", "ago", "now", "datetime_diff", "parse_json"
        };

        for (const auto & func : kql_unique_functions)
        {
            String func_with_paren = String(func) + "(";
            size_t pos = query_lower.find(func_with_paren);
            if (pos != String::npos)
            {
                if (pos == 0 || !std::isalnum(static_cast<unsigned char>(query_lower[pos - 1])))
                {
                    return true;
                }
            }
        }

        // Special handling for "extract" - it exists in both SQL and KQL
        {
            String extract_with_paren = "extract(";
            size_t pos = query_lower.find(extract_with_paren);
            if (pos != String::npos)
            {
                if (pos == 0 || !std::isalnum(static_cast<unsigned char>(query_lower[pos - 1])))
                {
                    size_t after_paren = pos + extract_with_paren.size();
                    if (after_paren < query.size())
                    {
                        while (after_paren < query.size() &&
                               (query[after_paren] == ' ' || query[after_paren] == '\t'))
                            ++after_paren;
                        if (after_paren < query.size() &&
                            (query[after_paren] == '\'' || query[after_paren] == '"'))
                        {
                            return true;  // KQL extract with regex pattern
                        }
                    }
                }
            }
        }

        return false;
    }

    /// Extract comments from query string using simple string search (fallback for malformed queries)
    void extractCommentsFromString(const std::string_view & query, std::vector<String> & comments)
    {
        const char * pos = query.data();
        const char * end = query.data() + query.size();

        while (pos < end)
        {
            // Look for -- comment
            pos = find_first_symbols<'-'>(pos, end);
            if (pos >= end)
                break;

            if (pos + 1 < end && pos[0] == '-' && pos[1] == '-')
            {
                const char * comment_start = pos;
                pos += 2;

                while (pos < end && (*pos == ' ' || *pos == '\t'))
                    ++pos;

                // Find end of line
                const char * comment_end = find_first_symbols<'\n'>(pos, end);
                if (comment_end == end)
                    comment_end = end;

                String comment(comment_start, comment_end - comment_start);
                if (!comment.empty())
                    comments.push_back(comment);

                pos = comment_end;
            }
            else
            {
                ++pos;
            }
        }
    }
}

TestHint::TestHint(const std::string_view & query)
{
    // Don't parse error hints in leading comments, because it feels weird.
    // Leading 'echo' hint is OK.
    bool is_leading_hint = true;

    // Original Lexer-based approach - this must run first and completely
    // to preserve existing behavior for all current tests
    Lexer lexer(query.data(), query.data() + query.size());

    for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
    {
        if (token.type != TokenType::Comment
            && token.type != TokenType::Whitespace)
        {
            is_leading_hint = false;
        }
        else if (token.type == TokenType::Comment)
        {
            String comment(token.begin, token.begin + token.size());

            if (!comment.empty())
            {
                size_t pos_start = comment.find('{', 0);
                if (pos_start != String::npos)
                {
                    size_t pos_end = comment.find('}', pos_start);
                    if (pos_end != String::npos)
                    {
                        Lexer comment_lexer(comment.c_str() + pos_start + 1, comment.c_str() + pos_end, 0);
                        parse(comment_lexer, is_leading_hint);
                    }
                }
            }
        }
    }

    // KQL fallback: Always try string-based extraction for KQL queries
    // because Lexer might fail on malformed queries before reaching comments
    if (isKQLQuery(query))
    {
        std::vector<String> comments;
        extractCommentsFromString(query, comments);

        std::cerr << "[TestHint] KQL query detected, found " << comments.size() << " comment(s) via string extraction" << std::endl;

        for (const auto & comment : comments)
        {
            // Find where this comment starts in the original query
            size_t comment_pos = query.find(comment);
            if (comment_pos == std::string_view::npos)
                continue;

            // Check if there's any non-whitespace before this comment
            bool is_leading = true;
            for (size_t i = 0; i < comment_pos; ++i)
            {
                char c = query[i];
                if (c != ' ' && c != '\t' && c != '\n' && c != '\r')
                {
                    is_leading = false;
                    break;
                }
            }

            size_t pos_start = comment.find('{', 0);
            if (pos_start != String::npos)
            {
                size_t pos_end = comment.find('}', pos_start);
                if (pos_end != String::npos)
                {
                    std::cerr << "[TestHint] KQL: Found hint in comment: " << comment << std::endl;
                    size_t old_client_errors = client_errors.size();
                    size_t old_server_errors = server_errors.size();

                    Lexer comment_lexer(comment.c_str() + pos_start + 1, comment.c_str() + pos_end, 0);
                    parse(comment_lexer, is_leading);

                    size_t new_client_errors = client_errors.size();
                    size_t new_server_errors = server_errors.size();

                    if (new_client_errors > old_client_errors || new_server_errors > old_server_errors)
                    {
                        std::cerr << "[TestHint] KQL: Successfully parsed hint - client_errors: " << client_errors.size()
                                  << ", server_errors: " << server_errors.size() << std::endl;
                    }
                }
            }
        }

        std::cerr << "[TestHint] KQL: Final state - client_errors: " << client_errors.size()
                  << ", server_errors: " << server_errors.size() << std::endl;
    }
}

bool TestHint::hasExpectedClientError(int error)
{
    return std::find(client_errors.begin(), client_errors.end(), error) != client_errors.end();
}

bool TestHint::hasExpectedServerError(int error)
{
    return std::find(server_errors.begin(), server_errors.end(), error) != server_errors.end();
}

bool TestHint::needRetry(const std::unique_ptr<Exception> & server_exception, size_t * retries_counter)
{
    chassert(retries_counter);
    if (max_retries <= *retries_counter)
        return false;

    ++*retries_counter;

    int error = ErrorCodes::OK;
    if (server_exception)
        error = server_exception->code();


    if (retry_until)
        return !hasExpectedServerError(error);  /// retry until we get the expected error
    return hasExpectedServerError(error); /// retry while we have the expected error
}

void TestHint::parse(Lexer & comment_lexer, bool is_leading_hint)
{
    std::unordered_set<std::string_view> commands{"echo", "echoOn", "echoOff", "retry"};

    std::unordered_set<std::string_view> command_errors{
        "serverError",
        "clientError",
        "error",
    };

    for (Token token = comment_lexer.nextToken(); !token.isEnd(); token = comment_lexer.nextToken())
    {
        if (token.type == TokenType::Whitespace)
            continue;

        String item = String(token.begin, token.end);
        if (token.type == TokenType::BareWord && commands.contains(item))
        {
            if (item == "echo")
                echo.emplace(true);
            if (item == "echoOn")
                echo.emplace(true);
            if (item == "echoOff")
                echo.emplace(false);

            if (item == "retry")
            {
                token = comment_lexer.nextToken();
                while (token.type == TokenType::Whitespace)
                    token = comment_lexer.nextToken();

                if (token.type != TokenType::Number)
                    throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_TEXT, "Could not parse the number of retries: {}",
                                        std::string_view(token.begin, token.end));

                max_retries = std::stoul(std::string(token.begin, token.end));

                token = comment_lexer.nextToken();
                while (token.type == TokenType::Whitespace)
                    token = comment_lexer.nextToken();

                if (token.type != TokenType::BareWord ||
                    (std::string_view(token.begin, token.end) != "until" &&
                    std::string_view(token.begin, token.end) != "while"))
                    throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_TEXT, "Expected 'until' or 'while' after the number of retries, got: {}",
                                        std::string_view(token.begin, token.end));
                retry_until = std::string_view(token.begin, token.end) == "until";
            }
        }
        else if (!is_leading_hint && token.type == TokenType::BareWord && command_errors.contains(item))
        {
            /// Everything after this must be a list of errors separated by comma
            ErrorVector error_codes;
            while (!token.isEnd())
            {
                token = comment_lexer.nextToken();
                if (token.type == TokenType::Whitespace)
                    continue;
                if (token.type == TokenType::Number)
                {
                    int code;
                    auto [p, ec] = std::from_chars(token.begin, token.end, code);
                    if (p == token.begin)
                        throw DB::Exception(
                            DB::ErrorCodes::CANNOT_PARSE_TEXT,
                            "Could not parse integer number for errorcode: {}",
                            std::string_view(token.begin, token.end));
                    error_codes.push_back(code);
                }
                else if (token.type == TokenType::BareWord)
                {
                    int code = DB::ErrorCodes::getErrorCodeByName(std::string_view(token.begin, token.end));
                    error_codes.push_back(code);
                }
                else
                    throw DB::Exception(
                        DB::ErrorCodes::CANNOT_PARSE_TEXT,
                        "Could not parse error code in {}: {}",
                        getTokenName(token.type),
                        std::string_view(token.begin, token.end));
                do
                {
                    token = comment_lexer.nextToken();
                } while (!token.isEnd() && token.type == TokenType::Whitespace);

                if (!token.isEnd() && token.type != TokenType::Comma)
                    throw DB::Exception(
                        DB::ErrorCodes::CANNOT_PARSE_TEXT,
                        "Could not parse error code. Expected ','. Got '{}'",
                        std::string_view(token.begin, token.end));
            }

            if (item == "serverError")
            {
                server_errors = error_codes;
            }
            else if (item == "clientError")
            {
                client_errors = error_codes;
            }
            else
            {
                server_errors = error_codes;
                client_errors = error_codes;
            }
            break;
        }
    }

    if (max_retries && server_errors.size() != 1)
        throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_TEXT, "Expected one serverError after the 'retry N while|until' command");
}

}
