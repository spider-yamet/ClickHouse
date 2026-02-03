#include <charconv>
#include <string_view>

#include <Client/TestHint.h>

#include <Parsers/Lexer.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

#include <fmt/ranges.h>

namespace DB::ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int OK;
}

namespace DB
{

TestHint::TestHint(const std::string_view & query)
{
    // Don't parse error hints in leading comments, because it feels weird.
    // Leading 'echo' hint is OK.
    size_t first_non_whitespace_pos = std::string::npos;

    // Try to use Lexer first (works for SQL queries)
    Lexer lexer(query.data(), query.data() + query.size());
    bool lexer_found_comments = false;

    for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
    {
        if (token.type != TokenType::Comment
            && token.type != TokenType::Whitespace)
        {
            if (first_non_whitespace_pos == std::string::npos)
                first_non_whitespace_pos = static_cast<size_t>(token.begin - query.data());
        }
        else if (token.type == TokenType::Comment)
        {
            lexer_found_comments = true;
            String comment(token.begin, token.begin + token.size());

            if (!comment.empty())
            {
                size_t pos_start = comment.find('{', 0);
                if (pos_start != String::npos)
                {
                    size_t pos_end = comment.find('}', pos_start);
                    if (pos_end != String::npos)
                    {
                        size_t token_pos = static_cast<size_t>(token.begin - query.data());
                        bool is_leading = (first_non_whitespace_pos == std::string::npos) ||
                                         (token_pos < first_non_whitespace_pos);
                        Lexer comment_lexer(comment.c_str() + pos_start + 1, comment.c_str() + pos_end, 0);
                        parse(comment_lexer, is_leading);
                    }
                }
            }
        }
    }

    // Fallback: If Lexer didn't find any comments (e.g., for KQL queries that confuse the SQL lexer),
    // use simple string-based comment extraction
    if (!lexer_found_comments)
    {
        String query_str(query);
        size_t pos = 0;

        // Find first non-whitespace character
        while (pos < query_str.size() && (query_str[pos] == ' ' || query_str[pos] == '\t' || query_str[pos] == '\n' || query_str[pos] == '\r'))
            pos++;

        if (pos < query_str.size())
            first_non_whitespace_pos = pos;

        // Now search for comments
        pos = 0;
        while (pos < query_str.size())
        {
            // Look for SQL-style comment (-- ...)
            if (pos + 1 < query_str.size() && query_str[pos] == '-' && query_str[pos + 1] == '-')
            {
                // Found a comment, extract it
                size_t comment_start = pos;
                pos += 2; // Skip "--"

                // Find the end of the line (or end of string)
                size_t comment_end = pos;
                while (comment_end < query_str.size() && query_str[comment_end] != '\n' && query_str[comment_end] != '\r')
                    comment_end++;

                String comment = query_str.substr(comment_start, comment_end - comment_start);

                // Check if this is a leading hint
                bool is_leading = (first_non_whitespace_pos == std::string::npos) ||
                                 (comment_start < first_non_whitespace_pos);

                // Extract hint from comment
                size_t hint_start = comment.find('{', 0);
                if (hint_start != String::npos)
                {
                    size_t hint_end = comment.find('}', hint_start);
                    if (hint_end != String::npos)
                    {
                        String hint_content = comment.substr(hint_start + 1, hint_end - hint_start - 1);
                        // Create a lexer for the hint content (this should work since hint syntax is simple)
                        Lexer comment_lexer(hint_content.data(), hint_content.data() + hint_content.size(), 0);
                        parse(comment_lexer, is_leading);
                    }
                }

                pos = comment_end;
            }
            else
            {
                pos++;
            }
        }
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
