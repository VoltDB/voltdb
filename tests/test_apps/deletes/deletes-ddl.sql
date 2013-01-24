CREATE TABLE big_table (
    fullname VARCHAR(16) NOT NULL,
    age BIGINT DEFAULT 0 NOT NULL,
    weight BIGINT DEFAULT 0 NOT NULL,
    desc1 VARCHAR(256) DEFAULT '' NOT NULL,
    desc2 VARCHAR(256) DEFAULT '' NOT NULL,
    addr1 VARCHAR(36) DEFAULT '' NOT NULL,
    addr2 VARCHAR(128) NOT NULL,
    addr3 VARCHAR(64),
    text1 VARCHAR(128) DEFAULT '' NOT NULL,
    text2 VARCHAR(36),
    sig VARCHAR(16) NOT NULL,
    ts TIMESTAMP,
    seconds TIMESTAMP,
    company VARCHAR(64),
    co_addr VARCHAR(256),
    deceased TINYINT DEFAULT 0,
    CONSTRAINT NO_PK_TREE PRIMARY KEY (fullname, sig, addr3, addr2)
);

CREATE INDEX treeBigTableFullnameCompany ON big_table (fullname, sig, addr3, company, ts);
CREATE INDEX treeBigTableFullnameTs ON big_table (fullname, ts);

-- This index uses string concatenation to exercise index-managed out-of-line storage.
-- It purposely avoids NULL-able strings that would produce lots of duplicate NULL 
-- concatenation results, since (NULL||x) == (x||NULL) == NULL,
-- that would have to be serially scanned on deletes.
CREATE INDEX treeBigTableConcatNonNullStrings ON big_table (text1 || fullname || sig || addr1 || addr2);

CREATE VIEW view1(fullname, deceased,weight,seconds,text2,addr1, total)
    AS SELECT fullname, deceased,weight,seconds,text2,addr1, COUNT(*)
    FROM big_table
    GROUP BY fullname, deceased,weight,seconds,text2,addr1;

CREATE VIEW view2(fullname, deceased, weight, seconds, sig, total)
    AS SELECT fullname, deceased, weight, seconds, sig, COUNT(*)
    FROM big_table
    GROUP BY fullname, deceased, weight, seconds, sig;

CREATE VIEW view3(fullname, deceased, sig, addr3, seconds, text1, addr1, text2, total)
    AS SELECT fullname, deceased, sig, addr3, seconds, text1, addr1, text2, COUNT(*)
    FROM big_table
    GROUP BY fullname, deceased, sig, addr3, seconds, text1, addr1, text2;

CREATE VIEW view4(fullname, deceased, sig, addr3, seconds, addr1, text2, age, total)
    AS SELECT fullname, deceased, sig, addr3, seconds, addr1, text2, age, COUNT(*)
    FROM big_table
    GROUP BY fullname, deceased, sig, addr3, seconds, addr1, text2, age;

CREATE VIEW view5(fullname, deceased, sig, addr3, seconds, addr1, text2, company, total)
    AS SELECT fullname, deceased, sig, addr3, seconds, addr1, text2, company, COUNT(*)
    FROM big_table
    GROUP BY fullname, deceased, sig, addr3, seconds, addr1, text2, company;

CREATE VIEW view6(fullname, deceased, total)
    AS SELECT fullname, deceased, COUNT(*)
    FROM big_table
    GROUP BY fullname, deceased;
