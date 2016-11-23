mkdir -p src/frontend/org/voltdb/dbmonitor/js-min/
mkdir -p src/frontend/org/voltdb/dbmonitor/js-min/d3/
cp src/frontend/org/voltdb/dbmonitor/js/d3/d3.v3.min.js src/frontend/org/voltdb/dbmonitor/js-min/d3/
echo '.'
cp src/frontend/org/voltdb/dbmonitor/js/d3/nv.d3.min\(modified\).js src/frontend/org/voltdb/dbmonitor/js-min/d3/
echo '.'
cp src/frontend/org/voltdb/dbmonitor/js/FileSaver.min.js src/frontend/org/voltdb/dbmonitor/js-min/
echo '.'
cp src/frontend/org/voltdb/dbmonitor/js/jquery.dataTables.min.js src/frontend/org/voltdb/dbmonitor/js-min/
echo '.'
cp src/frontend/org/voltdb/dbmonitor/js/jquery.getSelectedText.min.js src/frontend/org/voltdb/dbmonitor/js-min/
echo '.'
cp src/frontend/org/voltdb/dbmonitor/js/jquery.slimscroll.min.js src/frontend/org/voltdb/dbmonitor/js-min/ 
echo '.'
cp src/frontend/org/voltdb/dbmonitor/js/jquery.validate.min.js src/frontend/org/voltdb/dbmonitor/js-min/
echo '.'
cp src/frontend/org/voltdb/dbmonitor/js/jquery-1.11.1.min.js src/frontend/org/voltdb/dbmonitor/js-min/
echo '.'
cp src/frontend/org/voltdb/dbmonitor/js/sha256.js src/frontend/org/voltdb/dbmonitor/js-min/
echo '.'
cp src/frontend/org/voltdb/dbmonitor/js/voltdb.config.js src/frontend/org/voltdb/dbmonitor/js-min/
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/bignumber.js -o src/frontend/org/voltdb/dbmonitor/js-min/bignumber.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/bootstrap.js -o src/frontend/org/voltdb/dbmonitor/js-min/bootstrap.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/icheck.js -o src/frontend/org/voltdb/dbmonitor/js-min/icheck.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.contextMenu.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.contextMenu.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.cookie.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.cookie.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.dataTables.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.dataTables.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.popup.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.popup.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.tablesorter.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.tablesorter.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.tablesorter.widgets.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.tablesorter.widgets.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.tooltipster.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.tooltipster.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jqueryDatatablePlugins.js -o src/frontend/org/voltdb/dbmonitor/js-min/jqueryDatatablePlugins.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery-ui.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery-ui.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jsonParse.js -o src/frontend/org/voltdb/dbmonitor/js-min/jsonParse.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/long.js -o src/frontend/org/voltdb/dbmonitor/js-min/long.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/shortcut.js -o src/frontend/org/voltdb/dbmonitor/js-min/shortcut.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/sorttable.js -o src/frontend/org/voltdb/dbmonitor/js-min/sorttable.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/split.js -o src/frontend/org/voltdb/dbmonitor/js-min/split.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/template.js -o src/frontend/org/voltdb/dbmonitor/js-min/template.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.admin.ui.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.admin.ui.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.core.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.core.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.graph.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.graph.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.render.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.render.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.queryrender.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.queryrender.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.service.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.service.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.sqlquery.ui.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.sqlquery.ui.min.js
echo '.'
java -jar yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.ui.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.ui.min.js
echo '.'
