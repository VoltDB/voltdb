<configure-for-ints.sql>
{@jointype = "_jointype"}

-- INSERT
-- INSERT some common values
INSERT INTO @dmltable VALUES (100, 100, 100, 100)
INSERT INTO @dmltable VALUES (110, 100, 100, 100)
INSERT INTO @dmltable VALUES (120, 120, 120, 120)
INSERT INTO @dmltable VALUES (130, 120, 120, 120)
INSERT INTO @dmltable VALUES (140, 110, 120, 130)
INSERT INTO @dmltable VALUES (150, 120, 120, 140)

<join-template.sql>

{@jointype = "_jointype"}
--- Aggregate with join
<join-aggregate-template.sql>
