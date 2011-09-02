var QueryUI = (function(queryTab){

		var QueryTab = queryTab;
		var DataSource = queryTab.find('select.datasource');
        var ISQLParser = (function()
        {
			var SingleLineComments = /^\s*(\/\/|--).*$/gm;
			var Extract = new RegExp(/'[^']*'/gm);
			var AutoSplit = /\s(select|insert|update|delete|exec|execute)\s/gim;
			var AutoSplitParameters = /[\s,]+/gm;
			this.parse = function(src)
			{
                var command = ["exec", "execute"];
                var keyword = ["select", "insert", "update", "delete"];
                for(var i = 0;i<command.length;i++)
                {
                    for(var j = 0;j<command.length;j++)
                    {
                        var r = new RegExp("\\s*(" + command[i].replace(" ","\\s+") + ")\\s+(" + keyword[j] + ")\\s*", "gmi");
                        src = src.replace(r, " $1 #SQL_PARSER_STRING_KEYWORD#$2 ");
                    }
                }
				src = src.replace(SingleLineComments,'');
				src = src.replace(/''/g, '$(SQL_PARSER_ESCAPE_SINGLE_QUOTE)');
				var k = Extract.exec(src);
				var i = 0;
				var frag = new Array();
				while(k != null)
				{
					frag.push(k);
					src = src.replace(k,'$(SQL_PARSER_STRING_FRAGMENT#' + (i++) + ')');
					k = Extract.exec(src);
				}
				src = src.replace(AutoSplit,';$1 ');
				var sqlFrag = src.split(';');
				var statements = new Array();
				for(var i = 0; i< sqlFrag.length; i++)
				{
					var sql = sqlFrag[i].replace(/^\s+|\s+$/g,"");
					if (sql != '')
					{
						if(sql.indexOf('$(SQL_PARSER_STRING_FRAGMENT#') > -1)
							for(var j = 0; j< frag.length; j++)
								sql = sql.replace('$(SQL_PARSER_STRING_FRAGMENT#' + j + ')', frag[j]);
						sql = sql.replace(/\$\(SQL_PARSER_ESCAPE_SINGLE_QUOTE\)/g,"''");
                        sql = sql.replace("#SQL_PARSER_STRING_KEYWORD#","");
						statements.push(sql);
					}
				}
				return statements;
			}
			this.parseProcedureCallParameters = function(src)
			{
				src = src.replace(/''/g, '$(SQL_PARSER_ESCAPE_SINGLE_QUOTE)');
				var k = Extract.exec(src);
				var i = 0;
				var frag = new Array();
				while(k != null)
				{
					frag.push(k);
					src = src.replace(k,'$(SQL_PARSER_STRING_FRAGMENT#' + (i++) + ')');
					k = Extract.exec(src);
				}
				src = src.replace(AutoSplitParameters,',');
				var sqlFrag = src.split(',');
				var statements = new Array();
				for(var i = 0; i< sqlFrag.length; i++)
				{
					var sql = sqlFrag[i].replace(/^\s+|\s+$/g,'');
					if (sql != '')
					{
						if(sql.indexOf('$(SQL_PARSER_STRING_FRAGMENT#') > -1)
							for(var j = 0; j< frag.length; j++)
								sql = sql.replace('$(SQL_PARSER_STRING_FRAGMENT#' + j + ')', frag[j]);
						sql = sql.replace(/^\s+|\s+$/g,'');
						sql = sql.replace(/\$\(SQL_PARSER_ESCAPE_SINGLE_QUOTE\)/g,"''");
						statements.push(sql);
					}
				}
				return statements;
			}
        });
		var SQLParser = new ISQLParser();

function executeCallback(format, target, id)
{
	var Format = format;
	var Target = target;
	var Id = id;
	this.Callback = function(response)
	{
		processResponse(Format, Target, Id, response);
	}
}
this.execute = function()
{
	var statusBar = QueryTab.find('.workspacestatusbar span.status');
	if (DataSource.val() == 'Disconnected')
	{
		statusBar.text('Connect to a datasource first.');
		statusBar.addClass('error');
		return;
	}
	else
		statusBar.removeClass('error');
	var connection = VoltDB.Connections[DataSource.val()];
	var source = '';
	var source = QueryTab.find('.querybox').getSelectedText();
	if (source != null)
	{
		source = source.replace(/^\s+|\s+$/g,'');
		if (source == '')
			source = QueryTab.find('.querybox').val();
	}
	else
		source = QueryTab.find('.querybox').val();

	source = source.replace(/^\s+|\s+$/g,'');
	if (source == '')
		return;

	var format = $('#'+$('#result-format label[aria-pressed=true]').attr('for'))[0].value;
	var target = QueryTab.find('.resultbar');
	$("#execute-query").button("disable");
	if (format == 'grd')
	{
		target.html('<div class="wrapper gridwrapper"></div>');
		target = target.find('.wrapper');
	}
	else
	{
		target.html('<div class="wrapper"><textarea></textarea></div>');
		target = target.find('textarea');
	}
	var statements = SQLParser.parse(source);
	var start = (new Date()).getTime();
    var connectionQueue = connection.getQueue();
	connectionQueue.Start();
	for(var i = 0; i < statements.length; i++)
	{
		var id = 'r' + i;
		var callback = new executeCallback(format, target, id);
		if (/^execute /i.test(statements[i]))
			statements[i] = 'exec ' + statements[i].substr(8);
		if (/^exec /i.test(statements[i]))
		{
			var params = SQLParser.parseProcedureCallParameters(statements[i].substr(5));
			var procedure = params.splice(0,1)[0];
			if ((procedure.charAt(0) == '@') && (!(procedure in connection.Metadata['sysprocs'])))
				continue;
			connectionQueue.BeginExecute(procedure, params, callback.Callback);
		}
		else
		{
			connectionQueue.BeginExecute('@AdHoc', statements[i].replace(/[\r\n]+/g, " ").replace(/'/g,"''"), callback.Callback);
		}
	}
	connectionQueue.End(function(state,success) {
		var totalDuration = (new Date()).getTime() - state;
        if (success)
            statusBar.text('Query Duration: ' + (totalDuration/1000.0) + 's');
        else
        {
    		statusBar.addClass('error');
            statusBar.text('Query error | Query Duration: ' + (totalDuration/1000.0) + 's');
        }
		$("#execute-query").button("enable");
	}, start);
}
function processResponse(format, target, id, response)
{
	if (response.status == 1)
	{
		var tables = response.results;
		for(var j = 0; j < tables.length; j++)
			printResult(format, target, id + '_' + j, tables[j]);
	}
	else
		target.append("Error: " + response.statusstring + "\r\n");
}
function printResult(format, target, id, table)
{
	switch(format)
	{
		case 'csv':
			printCSV(target, id, table);
			break;
		case 'tab':
			printTab(target, id, table);
			break;
		case 'fix':
			printFixed(target, id, table);
			break;
		default:
			printGrid(target, id, table);
			break;
	}
}
function isUpdateResult(table)
{
	return ((table.schema[0].name.length == 0 || table.schema[0].name == "modified_tuples") && table.data.length == 1 && table.schema.length == 1 && table.schema[0].type == 6);
}

function printGrid(target, id, table)
{
	var src = '<table id="resultset-' + id + '" class="sortable tablesorter resultset-' + id + '" border="0" cellpadding="0" cellspacing="1"><thead class="ui-widget-header noborder"><tr>';
    if (isUpdateResult(table))
		src += '<th>modified_tuples</th>';
    else
    {
    	for(var j = 0; j < table.schema.length; j++)
	    	src += '<th>' + ( table.schema[j].name == "" ? ("Column " + (j+1)) : table.schema[j].name ) + '</th>';
    }
	src += '</tr></thead><tbody>';
	for(var j = 0; j < table.data.length; j++)
	{
		src += '<tr>';
		for(var k = 0; k < table.data[j].length; k++)
			src += '<td align="' + (table.schema[k].type == 9 ? 'left' : 'right') + '">' + table.data[j][k] + '</td>';
		src += '</tr>';
	}
	src += '</tbody></table>';
	$(target).append(src);
    sorttable.makeSortable(document.getElementById('resultset-' + id));
}
function printFixed(target, id, table)
{
	if (isUpdateResult(table))
	{
		$(target).append('\r\n\r\n(' + table.data[0][0] + ' row(s) affected)\r\n\r\n');
		return;
	}
    var padding = [];
    var fmt = [];
    for (var i = 0; i < table.schema.length; i++)
    {
        padding[i] = table.schema[i].name.length;
		for(var j = 0; j < table.data.length; j++)
			if ((''+table.data[j][i]).length > padding[i]) padding[i] = (''+table.data[j][i]).length;
        padding[i] += 1;

		var pad = '';
		while(pad.length < padding[i])
			pad += ' ';
        fmt[i] = pad;
    }
	var src = '';
	for(var j = 0; j < table.schema.length; j++)
	{
		if (j > 0) src += ' ';
		src += (table.schema[j].name + fmt[j]).substr(0,padding[j]);
	}
	src += '\r\n';
	for(var j = 0; j < table.schema.length; j++)
	{
		if (j > 0) src += ' ';
		src += fmt[j].replace(/ /g,'-');
	}
	src += '\r\n';
	for(var j = 0; j < table.data.length; j++)
	{
		for(var k = 0; k < table.data[j].length; k++)
		{
			if (k > 0) src += ' ';
			if (table.schema[k].type == 9)
				src += ('' + table.data[j][k] + fmt[k]).substr(0,padding[k]);
			else
			{
				var val = ''+ fmt[k] + table.data[j][k];
				src += val.substr(val.length-padding[k],padding[k]);
			}
			src += ' ';
		}
		src += '\r\n';
	}
	src += '\r\n(' + j + ' row(s) affected)\r\n\r\n';
	$(target).append(src);
}
function printTab(target, id, table)
{
	if (isUpdateResult(table))
	{
		$(target).append('\r\n\r\n(' + table.data[0][0] + ' row(s) affected)\r\n\r\n');
		return;
	}
	var src = '';
	var colModeData = [];
	for(var j = 0; j < table.schema.length; j++)
	{
		if (j > 0) src += '\t';
		src += table.schema[j].name;
	}
	src += '\r\n';
	for(var j = 0; j < table.data.length; j++)
	{
		for(var k = 0; k < table.data[j].length; k++)
		{
			if (k > 0) src += '\t';
			src += table.data[j][k];
		}
		src += '\r\n';
	}
	src += '\r\n(' + j + ' row(s) affected)\r\n\r\n';
	$(target).append(src);
}
function printCSV(target, id, table)
{
	if (isUpdateResult(table))
	{
		$(target).append('\r\n\r\n(' + table.data[0][0] + ' row(s) affected)\r\n\r\n');
		return;
	}
	var src = '';
	var colModeData = [];
	for(var j = 0; j < table.schema.length; j++)
	{
		if (j > 0) src += ',';
		src += table.schema[j].name;
	}
	src += '\r\n';
	for(var j = 0; j < table.data.length; j++)
	{
		for(var k = 0; k < table.data[j].length; k++)
		{
			if (k > 0) src += ',';
			src += table.data[j][k];
		}
		src += '\r\n';
	}
	src += '\r\n(' + j + ' row(s) affected)\r\n\r\n';
	$(target).append(src);
}
});

