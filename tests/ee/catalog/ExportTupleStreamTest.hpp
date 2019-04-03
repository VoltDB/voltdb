/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

/*
CREATE TABLE a (
    last_update timestamp default now not null
) USING TTL 1 SECONDS ON COLUMN last_update BATCH_SIZE 1
    MIGRATE TO TARGET archive;
*/
const char *catalogPayloadBasic =
    "add / clusters cluster\n"
    "set /clusters#cluster localepoch 1199145600\n"
    "set $PREV securityEnabled false\n"
    "set $PREV httpdportno 0\n"
    "set $PREV jsonapi true\n"
    "set $PREV networkpartition true\n"
    "set $PREV heartbeatTimeout 90\n"
    "set $PREV useddlschema true\n"
    "set $PREV drConsumerEnabled false\n"
    "set $PREV drProducerEnabled false\n"
    "set $PREV drRole \"none\"\n"
    "set $PREV drClusterId 0\n"
    "set $PREV drProducerPort 0\n"
    "set $PREV drMasterHost \"\"\n"
    "set $PREV drConsumerSslPropertyFile \"\"\n"
    "set $PREV drFlushInterval 0\n"
    "set $PREV preferredSource 0\n"
    "add /clusters#cluster databases database\n"
    "set /clusters#cluster/databases#database schema \"lQJUNDM1MjQ1NDE1NDQ1MjA1NDQxNDI0QwEMHDYxMjAyODIwEQJ8NkM2MTczNzQ1Rjc1NzA2NDYxNzQ2NTIwNzQ2OTZENjUBHBg2MTZENzAyAR5INTY2NjE3NTZDNzQyMDZFNkY3Nw0IBRABGkQ2QzIwMjkyMDU1NTM0OTRFNDcBgiw1NDRDMjAzMTIwNEQBFgQ1NQGcTDUzMjA0RjRFMjA0MzRGNEM1NTREAQ5elgARtgVECDc1Mh3iAEYBZiA0MTUyNDc0NTUBjjwxNzI2MzY4Njk3NjY1M0IK\"\n"
    "set $PREV isActiveActiveDRed false\n"
    "set $PREV securityprovider \"hash\"\n"
    "add /clusters#cluster/databases#database groups administrator\n"
    "set /clusters#cluster/databases#database/groups#administrator admin true\n"
    "set $PREV defaultproc true\n"
    "set $PREV defaultprocread true\n"
    "set $PREV sql true\n"
    "set $PREV sqlread true\n"
    "set $PREV allproc true\n"
    "add /clusters#cluster/databases#database groups user\n"
    "set /clusters#cluster/databases#database/groups#user admin false\n"
    "set $PREV defaultproc true\n"
    "set $PREV defaultprocread true\n"
    "set $PREV sql true\n"
    "set $PREV sqlread true\n"
    "set $PREV allproc true\n"
    "add /clusters#cluster/databases#database tables A\n"
    "set /clusters#cluster/databases#database/tables#A isreplicated true\n"
    "set $PREV partitioncolumn null\n"
    "set $PREV estimatedtuplecount 0\n"
    "set $PREV materializer null\n"
    "set $PREV signature \"A|p\"\n"
    "set $PREV tuplelimit 2147483647\n"
    "set $PREV isDRed false\n"
    "set $PREV tableType 3\n"
    "add /clusters#cluster/databases#database/tables#A columns LAST_UPDATE\n"
    "set /clusters#cluster/databases#database/tables#A/columns#LAST_UPDATE index 0\n"
    "set $PREV type 11\n"
    "set $PREV size 8\n"
    "set $PREV nullable false\n"
    "set $PREV name \"LAST_UPDATE\"\n"
    "set $PREV defaultvalue \"CURRENT_TIMESTAMP:43\"\n"
    "set $PREV defaulttype 11\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#A timeToLive ttl\n"
    "set /clusters#cluster/databases#database/tables#A/timeToLive#ttl ttlValue 1\n"
    "set $PREV ttlUnit \"MINUTES\"\n"
    "set $PREV ttlColumn /clusters#cluster/databases#database/tables#A/columns#LAST_UPDATE\n"
    "set $PREV batchSize 1\n"
    "set $PREV maxFrequency 1\n"
    "set $PREV migrationTarget \"ARCHIVE\"\n"
    "add /clusters#cluster/databases#database connectors ARCHIVE\n"
    "set /clusters#cluster/databases#database/connectors#ARCHIVE loaderclass \"\"\n"
    "set $PREV enabled false\n"
    "add /clusters#cluster/databases#database/connectors#ARCHIVE tableInfo A\n"
    "set /clusters#cluster/databases#database/connectors#ARCHIVE/tableInfo#A table /clusters#cluster/databases#database/tables#A\n"
    "set $PREV appendOnly true\n"
    "add /clusters#cluster/databases#database snapshotSchedule default\n"
    "set /clusters#cluster/databases#database/snapshotSchedule#default enabled false\n"
    "set $PREV frequencyUnit \"h\"\n"
    "set $PREV frequencyValue 24\n"
    "set $PREV retain 2\n"
    "set $PREV prefix \"AUTOSNAP\"\n"
    "add /clusters#cluster deployment deployment\n"
    "set /clusters#cluster/deployment#deployment kfactor 0\n"
    "add /clusters#cluster/deployment#deployment systemsettings systemsettings\n"
    "set /clusters#cluster/deployment#deployment/systemsettings#systemsettings temptablemaxsize 100\n"
    "set $PREV snapshotpriority 6\n"
    "set $PREV elasticduration 50\n"
    "set $PREV elasticthroughput 2\n"
    "set $PREV querytimeout 10000\n"
    "add /clusters#cluster logconfig log\n"
    "set /clusters#cluster/logconfig#log enabled false\n"
    "set $PREV synchronous false\n"
    "set $PREV fsyncInterval 200\n"
    "set $PREV maxTxns 2147483647\n"
    "set $PREV logSize 1024\n";

// ALTER TABLE a ADD COLUMN a INT NOT NULL;
const char *catalogPayloadAddColumn =
    "set /clusters#cluster/databases#database schema \"vANUNDM1MjQ1NDE1NDQ1MjA1NDQxNDI0QwEMHDQxMjAyODIwCQI8NEM0MTUzNTQ1RjU1NTA0NBEySDc0Njk2RDY1NzM3NDYxNkQ3MDIBHkg1NDY0MTU1NEM1NDIwNDM1NTUyBWgARQVCGDQ0OTRENDUBUBA0MTRENQE0CEU0RgUsAEUBNkQ0QzIwMjkyMDU1NTM0OTRFNDcBnAQ1NAEYFDMxMjA0RAEWDDU1NTQBRhQyMDRGNEUBaBw0RjRDNTU0RAUOWrIABDQyBbwoMzQ4NUY1MzQ5NUEB7gwzMTMwAQIFXBgxNTg1RjQ2AbIANQHGHDU0RTQzNTkyBXwFIgw5NDc1CUgpNgxGMjA1IRQQMjQ3NDUFyAgxNTIBZAw0OTU2ASQQM0IKNDEhDww0NTUyCS0xaQA2AdEQMTQ0NDQ+wwAFGgg5NEUFUTohAQgzQgo=\"\n"
    "set /clusters#cluster/databases#database/tables#A signature \"A|pi\"\n"
    "add /clusters#cluster/databases#database/tables#A columns A\n"
    "set /clusters#cluster/databases#database/tables#A/columns#A index 1\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable false\n"
    "set $PREV name \"A\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n";

// CREATE UNIQUE INDEX idx_a ON a (last_update);
const char *catalogPayloadCreateIndex =
    "set /clusters#cluster/databases#database schema \"9ANUNDM1MjQ1NDE1NDQ1MjA1NDQxNDI0QwEMHDQxMjAyODIwCQI8NEM0MTUzNTQ1RjU1NTA0NBEySDc0Njk2RDY1NzM3NDYxNkQ3MDIBHkg1NDY0MTU1NEM1NDIwNDM1NTUyBWgARQVCGDQ0OTRENDUBUBA0MTRENQE0CEU0RgUsAEUBNgw0QzJDCXgAMgWIPDY5NkU3NDY1Njc2NTcyMjBCLgA4MDI5MjA1NTUzNDk0RTQ3AcosNTQ0QzIwMzEyMDREARYMNTU1NAF0FDIwNEY0RQGWHDRGNEM1NTREBQ5a4AAENDIF6igzNDg1RjUzNDk1QSEcDDMxMzABAgVcGDE1ODVGNDYB4AA1AfQYNTRFNDM1OQl8BSIMOTQ3NQlIKWQMRjIwNSFCEDI0NzQ1BfYIMTUyAWQMNDk1NgEkCDNCCjqZAQFVADkJXwgyMDQB5QHNBDgyIRsUNDc4NUY2AekF2QEKcDI4NkM2MTczNzQ1Rjc1NzA2NDYxNzQ2NTI5M0IK\"\n"
    "add /clusters#cluster/databases#database/tables#A indexes IDX_A\n"
    "set /clusters#cluster/databases#database/tables#A/indexes#IDX_A unique true\n"
    "set $PREV assumeUnique false\n"
    "set $PREV migrating false\n"
    "set $PREV countable true\n"
    "set $PREV type 1\n"
    "set $PREV expressionsjson \"\"\n"
    "set $PREV predicatejson \"\"\n"
    "set $PREV isSafeWithNonemptySources true\n"
    "add /clusters#cluster/databases#database/tables#A/indexes#IDX_A columns LAST_UPDATE\n"
    "set /clusters#cluster/databases#database/tables#A/indexes#IDX_A/columns#LAST_UPDATE index 0\n"
    "set $PREV column /clusters#cluster/databases#database/tables#A/columns#LAST_UPDATE\n";
