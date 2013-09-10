-- Run the basic-template against a string-heavy table
<configure-for-string.sql>
<basic-template.sql>

SELECT * FROM @fromtables WHERE _inonestring
SELECT * FROM @fromtables WHERE _inpairofstrings
--just too slow for now SELECT * FROM @fromtables WHERE _insomestrings
