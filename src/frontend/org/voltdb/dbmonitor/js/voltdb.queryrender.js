function QueryUI(queryString) {
    "use strict";
    var CommandParser,
        queryToRun=queryString,
        DataSource = $.cookie('connectionkey') == undefined ? '' : $.cookie('connectionkey');

    function ICommandParser() {
        var MatchEndOfLineComments = /^\s*(?:\/\/|--).*$/gm,
            MatchOneQuotedString = /'[^']*'/m,
            MatchQuotedQuotes = /''/g,
            QuotedQuoteLiteral = "''",
            MatchDoubleQuotes = /\"/g,
            EscapedDoubleQuoteLiteral = '\\"',
            MatchDisguisedQuotedQuotes = /#COMMAND_PARSER_DISGUISED_QUOTED_QUOTE#/g,
            DisguisedQuotedQuoteLiteral = "#COMMAND_PARSER_DISGUISED_QUOTED_QUOTE#",
            QuotedStringNonceLiteral = "#COMMAND_PARSER_REPLACED_STRING#",
            // Generate fixed-length 5-digit nonce values from 100000 - 999999.
            // That's 900000 strings per statement batch -- that should be enough.
            MatchOneQuotedStringNonce = /#COMMAND_PARSER_REPLACED_STRING#(\d\d\d\d\d\d)/,
            QuotedStringNonceBase = 100000,

            // TODO: drop the remaining vars when semi-colon injection is no longer supported

            // Normally, statement boundaries are guessed by the parser, which inserts semicolons as needed.
            // The guessing is based on the assumption that the user is smart enough to not use SQL statement
            // keywords as schema names.  To err on the safe side, avoid splitting (require a semicolon) before
            // VoltDB proprietary non-SQL statement keywords, ("partition", "explain", "explainproc", "exec",
            // and "execute") because they could theoretically occur mid-statement as (legacy?) names in user
            // schema. Take a chance that they are not using SQL statement keywords like "select" and "delete"
            // as unquoted names in queries.
            // Similarly, do not enable statement splitting before "alter" or "drop" because it would be more
            // trouble than it is worth to disable it when these keywords occur in the
            // middle of an "alter table ... alter|drop column ..." statement.
            // The intent is to avoid writing another full sql parser, here.
            // Any statement keyword that does not get listed here simply requires an explicit semicolon before
            // it to mark the end of the preceding statement.
            // Note on            (?!\s+on) :
            // This subpattern consumes no input itself but ensures that the next
            // character is not 'on'.
            MatchStatementStarts =
                /\s((?:(?:\s\()*select)|insert|update|upsert|delete|truncate|create|partition(?!\s+on)|exec|execute|explain|explainproc)\s/gim,
            //     ($1--------------------------------------------------------------------------------------------------------------------)
            GenerateSplitStatements = ';$1 ',
            // Stored procedure parameters can be separated by commas or whitespace.
            // Multiple commas like "execute proc a,,b" are merged into one separator because that's easy.
            MatchParameterSeparators = /[\s,]+/g,

            // There are some easily recognizable patterns that contain statement keywords mid-statement.
            // As suggested above, cases like "alter" and "drop" that are not so easily recognized are
            // always ignored by the statement splitter -- the user must separate them from the prior
            // statement with a semicolon.
            // For these other keywords, the usual statement splitting can be easily disabled in special
            // cases:
            // - Any "select" that occurs in "insert into ... select"
            //   -- handled with its own more elaborate pattern: insert into <table-identifier> [(<column-list>)] select
            // - Any SQL statement keyword after "explain ".
            // - Any "select " that follows open parentheses (with optional whitespace)
            //   -- these could either be subselects or the select statement arguments to a setop
            //      (e.g. union).
            // - Any "select " that follows a trailing setop keyword:
            //   "union", "intersect", "except", or "all"
            //   -- actually for ease of implementation (pattern simplicity) also disable command
            //      splitting for the unlikely case of a setop followed by other statements:
            //      "insert", "update", "delete", "truncate"
            //
            // The pattern grouping uses "(?:" anonymous pattern groups to preserve $1 as the prefix
            // pattern and $2 as the suffix keyword. The intent is to temporarily disguise the suffix
            // keyword to prevent a statement-splitting semicolon from getting inserted before it.
            // If "explain" on ddl statements (?) (create|partition) is ever supported,
            // add them as options to the $2 suffix keyword pattern.
            MatchNonBreakingInsertIntoSelect =
                /(\s*(?:insert|upsert)\s+into(?=\"|\s)\s*(?:[a-z][a-z0-9_]*|\"(?:[^\"]|\"\")+\")\s*(?:\((?:\"(?:[^\"]|\"\")+\"|[^\")])+\))?[(\s]*)(select)/gim,
            //   ($1-----------------------------------------------------------------------------------------------------------------------------)($2----)
            // Note on            (?=\"|\s) :
            // This subpattern consumes no input itself but ensures that the next
            // character is either whitespace or a double quote. This is handy
            // when a keyword is followed by an identifier:
            //   INSERT INTO"Foo"SELECT ...
            // HSQL doesn't require whitespace between keywords and quoted
            // identifiers.
            // A more detailed explanation of the MatchNonBreakingInsertIntoSelect pattern
            // can be found in the comments for the functionally identical InsertIntoSelect
            // variable and related pattern variables in SQLCommand.java.
            MatchNonBreakingCompoundKeywords =
                /(\s+(?:explain|union|intersect|except|all)\s|(?:\())\s*((?:(?:\s\()*select)|insert|update|upsert|delete|truncate)\s+/gim,
            //   ($1------------------------------------------------)   ($2------------------------------------------------------)
            // Note on           ([\s\S])
            // It matches the any character including the new line character
            MatchCreateView = /(\s*(?:create\s+view\s+)(?:(?!create\s+(?:view|procedure))[\s\S])*\s+as\s+)(select)/gim,
            //                 ($1-----------------------------------------------------------------------)($2----)
            MatchCreateSingleQueryProcedure =
                /(\s*(?:create\s+procedure\s+)(?:(?!create\s+(?:view|procedure))[\s\S])*\s+as\s+)((?:(?:\s\()*select)|insert|update|upsert|delete|truncate)\s+/gim,
            //   ($1----------------------------------------------------------------------------)($2------------------------------------------------------)

            // LIMIT PARTITION ROWS <n> EXECUTE (DELETE ...)
            // There are three keywords that may start statements to escape:
            //
            //   partition, execute, and delete
            //
            // They require escaping when they are immediately preceded by
            // "limit", "rows <n>", and "execute(", respectively.
            MatchLimitPartition = /(\s*limit\s+)(partition)/gim,
            MatchRowcountExecute = /(\s*rows\s+\d+\s+)(execute)/gim,
            MatchExecuteDelete = /(\s*execute\s*\(\s*)(delete)/gim,

            MatchCompoundKeywordDisguise = /#NON_BREAKING_SUFFIX_KEYWORD#/g,
            GenerateDisguisedCompoundKeywords = ' $1 #NON_BREAKING_SUFFIX_KEYWORD#$2 ';

        // Avoid false positives for statement grammar inside quoted strings by
        // substituting a nonce for each string.
        function disguiseQuotedStrings(src, stringBankOut) {
            var nonceNum, nextString;
            // Temporarily disguise quoted quotes as non-quotes to simplify the work of
            // extracting quoted strings.
            src = src.replace(MatchQuotedQuotes, DisguisedQuotedQuoteLiteral);

            // Extract quoted strings to keep their content from getting confused with interesting
            // statement syntax.
            nonceNum = QuotedStringNonceBase;
            while (true) {
                nextString = MatchOneQuotedString.exec(src);
                if (nextString === null) {
                    break;
                }
                stringBankOut.push(nextString);
                src = src.replace(nextString, QuotedStringNonceLiteral + nonceNum);
                nonceNum += 1;
            }
            return src;
        }

        // Restore quoted strings by replcaing each nonce with its original quoted string.
        function undisguiseQuotedStrings(src, stringBank) {
            var nextNonce, nonceNum;
            // Clean up by restoring the replaced quoted strings.
            while (true) {
                nextNonce = MatchOneQuotedStringNonce.exec(src);
                if (nextNonce === null) {
                    break;
                }
                nonceNum = parseInt(nextNonce[1], 10);
                src = src.replace(QuotedStringNonceLiteral + nonceNum,
                                  stringBank[nonceNum - QuotedStringNonceBase]);
            }
            // Clean up by restoring the replaced quoted quotes.
            src = src.replace(MatchDisguisedQuotedQuotes, QuotedQuoteLiteral);
            return src;
        }

        // break down a multi-statement string into a statement array.
        function parseUserInputMethod(src) {
            var splitStmts, stmt, ii, len,
                stringBank = [],
                statementBank = [];
            // Eliminate line comments permanently.
            src = src.replace(MatchEndOfLineComments, '');

            // Extract quoted strings to keep their content from getting confused with interesting
            // statement syntax. This is required for statement splitting even if only at explicit
            // semi-colon boundaries -- semi-colns might appear in quoted text.
            src = disguiseQuotedStrings(src, stringBank);

            // TODO: drop the following section when semi-colon injection is no longer supported

            // Disguise compound keywords temporarily to avoid triggering statement splits.
            //* Enable this to debug in the browser */ console.log("pre-processed queries:'" + src + "'");
            src = src.replace(MatchNonBreakingInsertIntoSelect, GenerateDisguisedCompoundKeywords);
            src = src.replace(MatchNonBreakingCompoundKeywords, GenerateDisguisedCompoundKeywords);
            src = src.replace(MatchCreateView, GenerateDisguisedCompoundKeywords);
            src = src.replace(MatchCreateSingleQueryProcedure, GenerateDisguisedCompoundKeywords);

            src = src.replace(MatchLimitPartition, GenerateDisguisedCompoundKeywords);
            src = src.replace(MatchRowcountExecute, GenerateDisguisedCompoundKeywords);
            src = src.replace(MatchExecuteDelete, GenerateDisguisedCompoundKeywords);

            if (!src.match("^explain")) {
                // Start a new statement before each remaining statement keyword.
                src = src.replace(MatchStatementStarts, GenerateSplitStatements);
                //* Enable this to debug in the browser */ console.log("mid-processed queries:'" + src + "'");
            }

            // Restore disguised compound keywords post-statement-split.
            src = src.replace(MatchCompoundKeywordDisguise, '');
            //* Enable this to debug in the browser */ console.log("post-processed queries:'" + src + "'");

            // TODO: drop the preceding section when semi-colon injection is no longer supported

            // Finally, get to work -- break the input into separate statements for processing.
            splitStmts = src.split(';');

            statementBank = [];
            for (ii = 0, len = splitStmts.length; ii < len; ii += 1) {
                stmt = splitStmts[ii].trim();
                if (stmt !== '') {
                    // Clean up by restoring the replaced quoted strings.
                    stmt = undisguiseQuotedStrings(stmt, stringBank);

                    // Prepare double-quotes for HTTP request formatting by \-escaping them.
                    // NOTE: This is NOT a clean up of any mangling done inside this function.
                    // It just needs doing at some point, so why not here?
                    stmt = stmt.replace(MatchDoubleQuotes, EscapedDoubleQuoteLiteral);

                    statementBank.push(stmt);
                }
            }
            return statementBank;
        }

        // break down a multi-parameter proc call into an array of (1) proc name and any parameters.
        function parseProcedureCallParametersMethod(src) {
            // Extract quoted strings to keep their content from getting confused with interesting
            // statement syntax.
            var splitParams, param, ii, len,
                stringBank = [],
                parameterBank = [];
            src = disguiseQuotedStrings(src, stringBank);

            splitParams = src.split(MatchParameterSeparators);

            for (ii = 0, len = splitParams.length; ii < len; ii += 1) {
                param = splitParams[ii].trim();
                if (param !== '') {
                    if (param.toLowerCase() === 'null') {
                        parameterBank.push(null);
                    } else {
                        if (param.indexOf(QuotedStringNonceLiteral) == 0) {
                            // Clean up by restoring the replaced quoted strings.
                            param = undisguiseQuotedStrings(param, stringBank);
                        }
                        parameterBank.push(param);
                    }
                }
            }
            return parameterBank;
        }

        this.parseProcedureCallParameters = parseProcedureCallParametersMethod
        this.parseUserInput = parseUserInputMethod;
    }

    CommandParser = new ICommandParser();

    //TODO: Apply reasonable coding standards to the code below...

    function executeCallback(format, target, id) {
        var Format = format;
        var targetHtml = target.find('#resultHtml');
        var targetCsv = target.find('#resultCsv');
        var targetMonospace = target.find('#resultMonospace');
        var Id = id;
        $(targetHtml).html('');
        $(targetCsv).html('');
        $(targetMonospace).html('');

        function callback(response) {
            processResponse('HTML', targetHtml, Id + '_html', response);
            processResponse('CSV', targetCsv, Id + '_csv', response);
            processResponse('MONOSPACE', targetMonospace, Id + '_mono', response);
        }
        this.Callback = callback;
    }

    function executeMethod() {
        var target = $('.queryResult');
        var format = $('#exportType').val();
        
        if (!VoltDBCore.connections.hasOwnProperty(DataSource)) {
            $(target).html('Connect to a datasource first.');
            return;
        }

        var connection = VoltDBCore.connections[DataSource];
        var source = '';
        source = queryToRun;
        source = source.replace(/^\s+|\s+$/g, '');
        if (source == '')
            return;

        $("#runBTn").attr('disabled', 'disabled');
        $("#runBTn").addClass("graphOpacity");

        var statements = CommandParser.parseUserInput(source);
        var start = (new Date()).getTime();
        var connectionQueue = connection.getQueue();
        connectionQueue.Start();
        for (var i = 0; i < statements.length; i++) {
            var id = 'r' + i;
            var callback = new executeCallback(format, target, id);
            if (/^execute /i.test(statements[i]))
                statements[i] = 'exec ' + statements[i].substr(8);
            if (/^exec /i.test(statements[i])) {
                var params = CommandParser.parseProcedureCallParameters(statements[i].substr(5));
                var procedure = params.splice(0, 1)[0];
                connectionQueue.BeginExecute(procedure, params, callback.Callback);
            }
            else
                if (/^explain /i.test(statements[i])) {
                    connectionQueue.BeginExecute('@Explain', statements[i].substr(8).replace(/[\r\n]+/g, " ").replace(/'/g, "''"), callback.Callback);
                }
                else
                    if (/^explainproc /i.test(statements[i])) {
                        connectionQueue.BeginExecute('@ExplainProc', statements[i].substr(12).replace(/[\r\n]+/g, " ").replace(/'/g, "''"), callback.Callback);
                    }
                    else {
                        connectionQueue.BeginExecute('@AdHoc', statements[i].replace(/[\r\n]+/g, " ").replace(/'/g, "''"), callback.Callback);
                    }
        }
        function atEnd(state, success) {
            var totalDuration = (new Date()).getTime() - state;
            if (success) {
                $('#queryResults').removeClass('errorValue');
                $('#queryResults').html('Query Duration: ' + (totalDuration / 1000.0) + 's');
            } else {
                $('#queryResults').addClass('errorValue');
                $('#queryResults').html('Query error | Query Duration: ' + (totalDuration / 1000.0) + 's');
            }
            $("#runBTn").removeAttr('disabled');
            $("#runBTn").removeClass("graphOpacity");
        }
        connectionQueue.End(atEnd, start);
    }

    function processResponse(format, target, id, response) {
        if (response.status == 1) {
            var tables = response.results;
            for (var j = 0; j < tables.length; j++)
                printResult(format, target, id + '_' + j, tables[j]);

        } else {
            target.append('<span class="errorValue">Error: ' + response.statusstring + '\r\n</span>');
        }
    }

    function printResult(format, target, id, table) {
        switch (format.toUpperCase()) {
            case 'CSV'.toUpperCase():
                printCSV(target, id, table);
                break;
            case 'MONOSPACE':
                printMonoSpace(target, id, table);
                 break;
            default:
                printGrid(target, id, table);
                break;
        }
    }

    function isUpdateResult(table) {
        return ((table.schema[0].name.length == 0 || table.schema[0].name == "modified_tuples") && table.data.length == 1 && table.schema.length == 1 && table.schema[0].type == 6);
    }

    function applyFormat(val) {
        // Formatting for explain proc.  Only format on objects that have a replace function
        if (null != val && val.replace != null) {
            val = val.replace(/\t/g, '&nbsp;&nbsp;&nbsp;&nbsp;');
            val = val.replace(/ /g, '&nbsp;');
            val = val.replace(/\n/g, '<br>');
        }
        return val;
    }

    function lPadZero(v, len) {
        // return a string left padded with zeros to length 'len'
        v = v + "";
        if (v.length < len) {
            v = Array(len - v.length + 1).join("0") + v;
        }
        return v;
    }

    function printGrid(target, id, table) {
        var src = '<table id="table_'+id+'" width="100%" border="0" cellspacing="0" cellpadding="0" class="dbTbl sortable tablesorter" id="queryResultTbl"><thead class="ui-widget-header noborder"><tr>';
        if (isUpdateResult(table))
            src += '<th>modified_tuples</th>';
        else {
            for (var j = 0; j < table.schema.length; j++)
                src += '<th width="' + 100/table.schema.length + '%">' + (table.schema[j].name == "" ? ("Column " + (j + 1)) : table.schema[j].name) + '</th>';
        }
        src += '</tr></thead><tbody>';
        for (var j = 0; j < table.data.length; j++) {
            src += '<tr>';
            for (var k = 0; k < table.data[j].length; k++) {
                var val = table.data[j][k];
                var typ = table.schema[k].type;
                if (typ == 11) {
                    var us = val % 1000;
                    var dt = new Date(val / 1000);
                    val = lPadZero(dt.getUTCFullYear(), 4) + "-"
                        + lPadZero((dt.getUTCMonth()) + 1, 2) + "-"
                        + lPadZero(dt.getUTCDate(), 2) + " "
                        + lPadZero(dt.getUTCHours(), 2) + ":"
                        + lPadZero(dt.getUTCMinutes(), 2) + ":"
                        + lPadZero(dt.getUTCSeconds(), 2) + "."
                        + lPadZero((dt.getUTCMilliseconds()) * 1000 + us, 6);
                    typ = 9;  //code for varchar
                }
                val = applyFormat(val);
                src += '<td align="left">' + val + '</td>';
            }
            src += '</tr>';
        }
        src += '</tbody></table>';
        $(target).append(src);
        sorttable.makeSortable(document.getElementById('table_'+id));
    }

    function printMonoSpace(target, id, table) {
        if (isUpdateResult(table)) {
            $(target).append('\r\n\r\n(' + table.data[0][0] + ' row(s) affected)\r\n\r\n');
            return;
        }
        var src = '';
        for (var j = 0; j < table.schema.length; j++) {
            if (j > 0) src += '\t';
            src += table.schema[j].name;
        }
        src += '</br>\r\n';
        for (var j = 0; j < table.data.length; j++) {
            for (var k = 0; k < table.data[j].length; k++) {
                if (k > 0) src += '\t';
                src += table.data[j][k];
            }
            src += '</br>\r\n';
        }
        src += '</br>\r\n(' + j + ' row(s) affected)\r\n\r\n</br></br>';
        $(target).append(src);
    }

    function printCSV(target, id, table) {
        if (isUpdateResult(table)) {
            $(target).append('\r\n\r\n(' + table.data[0][0] + ' row(s) affected)\r\n\r\n');
            return;
        }
        var src = '<pr>';
        var colModeData = [];
        for (var j = 0; j < table.schema.length; j++) {
            if (j > 0) src += ', ';
            src += table.schema[j].name;
        }
        src += '</br>\r\n';
        for (var j = 0; j < table.data.length; j++) {
            for (var k = 0; k < table.data[j].length; k++) {
                if (k > 0) src += ', ';
                src += table.data[j][k];
            }
            src += '</br>\r\n';
        }
        src += '</br>\r\n(' + j + ' row(s) affected)\r\n\r\n</pr></br></br>';
        $(target).append(src);
    }

    this.execute = executeMethod;
}
