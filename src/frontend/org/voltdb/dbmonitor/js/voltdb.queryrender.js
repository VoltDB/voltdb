function QueryUI(queryString, userName) {
    "use strict";
    var CommandParser,
        queryToRun = queryString;

    function ICommandParser() {
        var MatchEndOfLineComments = /^\s*(?:\/\/|--).*$/gm,
            MatchOneQuotedString = /'[^']*'/m,
            MatchDoubleQuotes = /\"/g,
            EscapedDoubleQuoteLiteral = '\\"',
            QuotedStringNonceLiteral = "#COMMAND_PARSER_REPLACED_STRING#",
            // Generate fixed-length 5-digit nonce values from 100000 - 999999.
            // That's 900000 strings per statement batch -- that should be enough.
            MatchOneQuotedStringNonce = /#COMMAND_PARSER_REPLACED_STRING#(\d\d\d\d\d\d)/,
            QuotedStringNonceBase = 100000,

            // Stored procedure parameters can be separated by commas or whitespace.
            // Multiple commas like "execute proc a,,b" are merged into one separator because that's easy.
            MatchParameterSeparators = /[\s,]+/g;

        // Avoid false positives for statement grammar inside quoted strings by
        // substituting a nonce for each string.
        function disguiseQuotedStrings(src, stringBankOut) {
            var nonceNum, nextString;

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
            return src;
        }

        // break down a multi-statement string into a statement array.
        function parseUserInputMethod(src) {
            var splitStmts, stmt, ii, len,
                stringBank = [],
                statementBank = [];
            // Eliminate line comments permanently.
            src = src.replace(MatchEndOfLineComments, '');

            // Extract quoted strings to keep their content from getting confused with
            // interesting statement syntax. This is required for statement splitting at 
            // semicolon boundaries -- semicolons might appear in quoted text.
            src = disguiseQuotedStrings(src, stringBank);

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

            var processResponseForAllViews = function() {
                processResponse('HTML', targetHtml, Id + '_html', response);
                processResponse('CSV', targetCsv, Id + '_csv', response);
                processResponse('MONOSPACE', targetMonospace, Id + '_mono', response);
            };
            
            var handlePortSwitchingOption = function (isPaused) {
                
                if (isPaused) {
                    //Show error message with an option to allow admin port switching
                    $("#queryDatabasePausedErrorPopupLink").click();
                } else {
                    processResponseForAllViews();
                }
            };
            
            //Handle the case when Database is paused
            if (response.status == -5 && VoltDbAdminConfig.isAdmin && !SQLQueryRender.useAdminPortCancelled) {

                if (!VoltDbAdminConfig.isDbPaused) {

                    //Refresh cluster state to display latest status.
                    var loadAdminTabPortAndOverviewDetails = function(portAndOverviewValues) {
                        VoltDbAdminConfig.displayPortAndRefreshClusterState(portAndOverviewValues);
                        handlePortSwitchingOption(VoltDbAdminConfig.isDbPaused);
                    };
                    voltDbRenderer.GetSystemInformation(function() {}, loadAdminTabPortAndOverviewDetails, function(data) {});
                } else {
                    handlePortSwitchingOption(true);
                }
                
            } else {
                processResponseForAllViews();
            }
            SQLQueryRender.useAdminPortCancelled = false;
        }
        this.Callback = callback;
    }

    function executeMethod() {
        var target = $('.queryResult');
        var format = $('#exportType').val();

        var dataSource = $.cookie('connectionkey') == undefined ? '' : $.cookie('connectionkey');
        if (!VoltDBCore.connections.hasOwnProperty(dataSource)) {
            $(target).html('Connect to a datasource first.');
            return;
        }

        var connection = VoltDBCore.connections[dataSource];
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
            // This inline encoder hack is intended to use html's &#nnnn; character encoding to
            // properly escape characters that would otherwise mean something as html
            // -- including angle brackets and such that are commonly used to suggest that the
            // user correct their ddl grammar. Angle-bracketed place-holders were being
            // rendered as invisible meaningless html tags.
            // See http://stackoverflow.com/questions/18749591/encode-html-entities-in-javascript#18750001
            var encodedStatus = response.statusstring.replace(/[\u00A0-\u9999<>\&]/gim,
                function(i) { return '&#'+i.charCodeAt(0)+';'; });
            target.append('<span class="errorValue">Error: ' + encodedStatus + '\r\n</span>');
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
