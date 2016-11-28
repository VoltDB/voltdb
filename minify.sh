mkdir -p src/frontend/org/voltdb/dbmonitor/js-min/
mkdir -p src/frontend/org/voltdb/dbmonitor/js-min/d3/
cp src/frontend/org/voltdb/dbmonitor/js/d3/d3.v3.min.js src/frontend/org/voltdb/dbmonitor/js-min/d3/

cp src/frontend/org/voltdb/dbmonitor/js/FileSaver.min.js src/frontend/org/voltdb/dbmonitor/js-min/

cp src/frontend/org/voltdb/dbmonitor/js/jquery.dataTables.min.js src/frontend/org/voltdb/dbmonitor/js-min/

cp src/frontend/org/voltdb/dbmonitor/js/jquery.getSelectedText.min.js src/frontend/org/voltdb/dbmonitor/js-min/

cp src/frontend/org/voltdb/dbmonitor/js/jquery.slimscroll.min.js src/frontend/org/voltdb/dbmonitor/js-min/

cp src/frontend/org/voltdb/dbmonitor/js/jquery.validate.min.js src/frontend/org/voltdb/dbmonitor/js-min/

cp src/frontend/org/voltdb/dbmonitor/js/jquery-1.11.1.min.js src/frontend/org/voltdb/dbmonitor/js-min/

cp src/frontend/org/voltdb/dbmonitor/js/sha256.js src/frontend/org/voltdb/dbmonitor/js-min/

cp src/frontend/org/voltdb/dbmonitor/js/split.min.js src/frontend/org/voltdb/dbmonitor/js-min/


java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery-ui.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery-ui.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.config.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.config.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/d3/nv.d3.min\(modified\).js -o src/frontend/org/voltdb/dbmonitor/js-min/d3/nv.d3.min\(modified\).js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/bignumber.js -o src/frontend/org/voltdb/dbmonitor/js-min/bignumber.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/bootstrap.js -o src/frontend/org/voltdb/dbmonitor/js-min/bootstrap.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/icheck.js -o src/frontend/org/voltdb/dbmonitor/js-min/icheck.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.contextMenu.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.contextMenu.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.cookie.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.cookie.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.dataTables.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.dataTables.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.popup.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.popup.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.tablesorter.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.tablesorter.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.tablesorter.widgets.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.tablesorter.widgets.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jquery.tooltipster.js -o src/frontend/org/voltdb/dbmonitor/js-min/jquery.tooltipster.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jqueryDatatablePlugins.js -o src/frontend/org/voltdb/dbmonitor/js-min/jqueryDatatablePlugins.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/jsonParse.js -o src/frontend/org/voltdb/dbmonitor/js-min/jsonParse.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/long.js -o src/frontend/org/voltdb/dbmonitor/js-min/long.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/shortcut.js -o src/frontend/org/voltdb/dbmonitor/js-min/shortcut.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/sorttable.js -o src/frontend/org/voltdb/dbmonitor/js-min/sorttable.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/template.js -o src/frontend/org/voltdb/dbmonitor/js-min/template.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.admin.ui.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.admin.ui.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.core.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.core.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.graph.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.graph.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.render.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.render.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.queryrender.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.queryrender.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.service.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.service.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.sqlquery.ui.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.sqlquery.ui.min.js

java -jar lib/yuicompressor-2.4.8.jar src/frontend/org/voltdb/dbmonitor/js/voltdb.ui.js -o src/frontend/org/voltdb/dbmonitor/js-min/voltdb.ui.min.js

