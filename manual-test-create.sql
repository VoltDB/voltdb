CREATE STREAM rabbit EXPORT TO TARGET dolphin ( blob VARCHAR(30), num BIGINT );
INSERT INTO rabbit VALUES ('asdasdahjkas', 123891);
CREATE STREAM beaver EXPORT TO TARGET dolphin ( name VARCHAR(32) );
INSERT INTO beaver VALUES ('shishkebab');
