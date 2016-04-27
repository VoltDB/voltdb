/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <cstdlib>
#include <ctime>
#include <unistd.h>
#include <boost/shared_ptr.hpp>
#include "harness.h"
#include "common/common.h"
#include "expressions/abstractexpression.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "execution/VoltDBEngine.h"
#include "plannodes/abstractplannode.h"
#include "indexes/tableindex.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/tableutil.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/table.h"
#include "catalog/database.h"
#include "catalog/constraint.h"

#include "test_utils/LoadTableFrom.hpp"

using namespace std;
using namespace voltdb;

#define NUM_OF_COLUMNS 4
#define NUM_OF_INDEXES 3
#define NUM_OF_TUPLES 10 //must be multiples of 2 for Update test.

//
// Define the column information for the main test table
// This is useful because it will allow us to check different types and other
// configurations without having to dig down into the code
//
ValueType COLUMN_TYPES[NUM_OF_COLUMNS]  = { VALUE_TYPE_INTEGER,
                                                    VALUE_TYPE_VARCHAR,
                                                    VALUE_TYPE_VARCHAR,
                                                    VALUE_TYPE_INTEGER };
int COLUMN_SIZES[NUM_OF_COLUMNS]                = { 4, 8, 8, 4};
bool COLUMN_ALLOW_NULLS[NUM_OF_COLUMNS]         = { false, true, true, false };

typedef int64_t fragmentId_t;

/**
 * This Topend allows us to get fragments by fragment id.  Other
 * than that, this is just a DummyTopend.
 */
class EngineTestTopend : public DummyTopend {
    typedef std::map<fragmentId_t, std::string> fragmentMap;
    fragmentMap m_fragments;
public:
    void addPlan(fragmentId_t fragmentId, const std::string &planStr) {
        m_fragments[fragmentId] = planStr;
    }
    std::string planForFragmentId(fragmentId_t fragmentId) {
        fragmentMap::iterator it = m_fragments.find(fragmentId);
        if (it == m_fragments.end()) {
            return "";
        } else {
            return it->second;
        }
    }
};

class ExecutionEngineTest : public Test {
public:
    /**
     * This constructor lets us set the global random seed for the
     * random number generator.  It would be better to have a seed
     * just for this test.  But that is not easily done.
     */
    ExecutionEngineTest(uint32_t random_seed = (unsigned int)time(NULL))
            : m_cluster_id(1),
              m_site_id(1),
              m_constraint(NULL)
    {
        srand(random_seed);
        /*
         * The schema for this catalog can be found in the
         * sql file voltdb/tests/frontend/org/voltdb/planner/testplans-eng10022.sql.
         * To generate it, start voltdb and load that schema into the database.
         * The file voltdbroot/config_log/catalog.jar will contain a file
         * named catalog.txt, whose contents are this string.  It will need some
         * cleanup in emacs to make it suitable to be a C++ string.
         * All that will be needed is to escape double quotes with a backslash,
         * and surround each line with unescaped double quotes.  But you
         * knew that already.
         */
        m_catalog_string =
            "add / clusters cluster\n"
            "set /clusters#cluster localepoch 1199145600\n"
            "set $PREV securityEnabled false\n"
            "set $PREV httpdportno 0\n"
            "set $PREV jsonapi false\n"
            "set $PREV networkpartition false\n"
            "set $PREV voltRoot \"\"\n"
            "set $PREV exportOverflow \"\"\n"
            "set $PREV drOverflow \"\"\n"
            "set $PREV adminport 0\n"
            "set $PREV adminstartup false\n"
            "set $PREV heartbeatTimeout 0\n"
            "set $PREV useddlschema false\n"
            "set $PREV drConsumerEnabled false\n"
            "set $PREV drProducerEnabled false\n"
            "set $PREV drClusterId 0\n"
            "set $PREV drProducerPort 0\n"
            "set $PREV drMasterHost \"\"\n"
            "set $PREV drFlushInterval 0\n"
            "add /clusters#cluster databases database\n"
            "set /clusters#cluster/databases#database schema \"eJylU1tuwzAM+99pbIn043N1nPsfabJRtGtXdYmLIFFgWSJFgVAKiEiAEuwbIWjj3w52KEm10x0bSJEgxd75vMqjAhJSTT0jMeXEbDXo2IkRSevdpD30SKiU2aUb+jawc0oxS9JURpSiokGhRepTbTPmS5X8NtQANeZz8ifOt7sMc6rNcITVai82Zx06uPOPbNXLFx6ktVQfUCxXAjep7xmNltvRffF90D+ApnW3GaPFo/CyDN/cXTZnU4sK6SGKn9D47QFr8tYDr/JnPTB7LHpg1i55YFR+7gF3/v89MAm89YAnvg96wgMuvCzDN3eXzdnUokJ6iOI5Gj8YBELa\"\n"
            "set $PREV isActiveActiveDRed false\n"
            "add /clusters#cluster/databases#database groups administrator\n"
            "set $PREV securityprovider \"\"\n"
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
            "add /clusters#cluster/databases#database tables D_CUSTOMER\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER isreplicated false\n"
            "set $PREV partitioncolumn /clusters#cluster/databases#database/tables#D_CUSTOMER/columns#D_CUSTOMERID\n"
            "set $PREV estimatedtuplecount 0\n"
            "set $PREV materializer null\n"
            "set $PREV signature \"D_CUSTOMER|ivvi\"\n"
            "set $PREV tuplelimit 2147483647\n"
            "set $PREV isDRed false\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER columns D_CUSTOMERID\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/columns#D_CUSTOMERID index 0\n"
            "set $PREV type 5\n"
            "set $PREV size 4\n"
            "set $PREV nullable false\n"
            "set $PREV name \"D_CUSTOMERID\"\n"
            "set $PREV defaultvalue null\n"
            "set $PREV defaulttype 0\n"
            "set $PREV matview null\n"
            "set $PREV aggregatetype 0\n"
            "set $PREV matviewsource null\n"
            "set $PREV inbytes false\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER columns D_FIRSTNAME\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/columns#D_FIRSTNAME index 1\n"
            "set $PREV type 9\n"
            "set $PREV size 2048\n"
            "set $PREV nullable true\n"
            "set $PREV name \"D_FIRSTNAME\"\n"
            "set $PREV defaultvalue null\n"
            "set $PREV defaulttype 0\n"
            "set $PREV matview null\n"
            "set $PREV aggregatetype 0\n"
            "set $PREV matviewsource null\n"
            "set $PREV inbytes false\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER columns D_LASTNAME\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/columns#D_LASTNAME index 2\n"
            "set $PREV type 9\n"
            "set $PREV size 2048\n"
            "set $PREV nullable true\n"
            "set $PREV name \"D_LASTNAME\"\n"
            "set $PREV defaultvalue null\n"
            "set $PREV defaulttype 0\n"
            "set $PREV matview null\n"
            "set $PREV aggregatetype 0\n"
            "set $PREV matviewsource null\n"
            "set $PREV inbytes false\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER columns D_ZIPCODE\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/columns#D_ZIPCODE index 3\n"
            "set $PREV type 5\n"
            "set $PREV size 4\n"
            "set $PREV nullable true\n"
            "set $PREV name \"D_ZIPCODE\"\n"
            "set $PREV defaultvalue null\n"
            "set $PREV defaulttype 0\n"
            "set $PREV matview null\n"
            "set $PREV aggregatetype 0\n"
            "set $PREV matviewsource null\n"
            "set $PREV inbytes false\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER indexes D_TABLEINDEX1\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX1 unique false\n"
            "set $PREV assumeUnique false\n"
            "set $PREV countable true\n"
            "set $PREV type 1\n"
            "set $PREV expressionsjson \"\"\n"
            "set $PREV predicatejson \"\"\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX1 columns D_CUSTOMERID\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX1/columns#D_CUSTOMERID index 0\n"
            "set $PREV column /clusters#cluster/databases#database/tables#D_CUSTOMER/columns#D_CUSTOMERID\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER indexes D_TABLEINDEX2\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX2 unique true\n"
            "set $PREV assumeUnique false\n"
            "set $PREV countable true\n"
            "set $PREV type 1\n"
            "set $PREV expressionsjson \"\"\n"
            "set $PREV predicatejson \"\"\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX2 columns D_CUSTOMERID\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX2/columns#D_CUSTOMERID index 0\n"
            "set $PREV column /clusters#cluster/databases#database/tables#D_CUSTOMER/columns#D_CUSTOMERID\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX2 columns D_FIRSTNAME\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX2/columns#D_FIRSTNAME index 1\n"
            "set $PREV column /clusters#cluster/databases#database/tables#D_CUSTOMER/columns#D_FIRSTNAME\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX2 columns D_LASTNAME\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX2/columns#D_LASTNAME index 2\n"
            "set $PREV column /clusters#cluster/databases#database/tables#D_CUSTOMER/columns#D_LASTNAME\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER indexes D_TABLEINDEX3\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX3 unique false\n"
            "set $PREV assumeUnique false\n"
            "set $PREV countable true\n"
            "set $PREV type 1\n"
            "set $PREV expressionsjson \"\"\n"
            "set $PREV predicatejson \"\"\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX3 columns D_FIRSTNAME\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX3/columns#D_FIRSTNAME index 0\n"
            "set $PREV column /clusters#cluster/databases#database/tables#D_CUSTOMER/columns#D_FIRSTNAME\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX3 columns D_LASTNAME\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#D_TABLEINDEX3/columns#D_LASTNAME index 1\n"
            "set $PREV column /clusters#cluster/databases#database/tables#D_CUSTOMER/columns#D_LASTNAME\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER indexes VOLTDB_AUTOGEN_IDX_PK_D_CUSTOMER_D_CUSTOMERID\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#VOLTDB_AUTOGEN_IDX_PK_D_CUSTOMER_D_CUSTOMERID unique true\n"
            "set $PREV assumeUnique false\n"
            "set $PREV countable true\n"
            "set $PREV type 1\n"
            "set $PREV expressionsjson \"\"\n"
            "set $PREV predicatejson \"\"\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#VOLTDB_AUTOGEN_IDX_PK_D_CUSTOMER_D_CUSTOMERID columns D_CUSTOMERID\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#VOLTDB_AUTOGEN_IDX_PK_D_CUSTOMER_D_CUSTOMERID/columns#D_CUSTOMERID index 0\n"
            "set $PREV column /clusters#cluster/databases#database/tables#D_CUSTOMER/columns#D_CUSTOMERID\n"
            "add /clusters#cluster/databases#database/tables#D_CUSTOMER constraints VOLTDB_AUTOGEN_IDX_PK_D_CUSTOMER_D_CUSTOMERID\n"
            "set /clusters#cluster/databases#database/tables#D_CUSTOMER/constraints#VOLTDB_AUTOGEN_IDX_PK_D_CUSTOMER_D_CUSTOMERID type 4\n"
            "set $PREV oncommit \"\"\n"
            "set $PREV index /clusters#cluster/databases#database/tables#D_CUSTOMER/indexes#VOLTDB_AUTOGEN_IDX_PK_D_CUSTOMER_D_CUSTOMERID\n"
            "set $PREV foreignkeytable null\n"
            "add /clusters#cluster/databases#database tables R_CUSTOMER\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER isreplicated true\n"
            "set $PREV partitioncolumn null\n"
            "set $PREV estimatedtuplecount 0\n"
            "set $PREV materializer null\n"
            "set $PREV signature \"R_CUSTOMER|ivvi\"\n"
            "set $PREV tuplelimit 2147483647\n"
            "set $PREV isDRed false\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER columns R_CUSTOMERID\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/columns#R_CUSTOMERID index 0\n"
            "set $PREV type 5\n"
            "set $PREV size 4\n"
            "set $PREV nullable false\n"
            "set $PREV name \"R_CUSTOMERID\"\n"
            "set $PREV defaultvalue null\n"
            "set $PREV defaulttype 0\n"
            "set $PREV matview null\n"
            "set $PREV aggregatetype 0\n"
            "set $PREV matviewsource null\n"
            "set $PREV inbytes false\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER columns R_FIRSTNAME\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/columns#R_FIRSTNAME index 1\n"
            "set $PREV type 9\n"
            "set $PREV size 2048\n"
            "set $PREV nullable true\n"
            "set $PREV name \"R_FIRSTNAME\"\n"
            "set $PREV defaultvalue null\n"
            "set $PREV defaulttype 0\n"
            "set $PREV matview null\n"
            "set $PREV aggregatetype 0\n"
            "set $PREV matviewsource null\n"
            "set $PREV inbytes false\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER columns R_LASTNAME\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/columns#R_LASTNAME index 2\n"
            "set $PREV type 9\n"
            "set $PREV size 2048\n"
            "set $PREV nullable true\n"
            "set $PREV name \"R_LASTNAME\"\n"
            "set $PREV defaultvalue null\n"
            "set $PREV defaulttype 0\n"
            "set $PREV matview null\n"
            "set $PREV aggregatetype 0\n"
            "set $PREV matviewsource null\n"
            "set $PREV inbytes false\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER columns R_ZIPCODE\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/columns#R_ZIPCODE index 3\n"
            "set $PREV type 5\n"
            "set $PREV size 4\n"
            "set $PREV nullable true\n"
            "set $PREV name \"R_ZIPCODE\"\n"
            "set $PREV defaultvalue null\n"
            "set $PREV defaulttype 0\n"
            "set $PREV matview null\n"
            "set $PREV aggregatetype 0\n"
            "set $PREV matviewsource null\n"
            "set $PREV inbytes false\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER indexes R_TABLEINDEX1\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX1 unique false\n"
            "set $PREV assumeUnique false\n"
            "set $PREV countable true\n"
            "set $PREV type 1\n"
            "set $PREV expressionsjson \"\"\n"
            "set $PREV predicatejson \"\"\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX1 columns R_CUSTOMERID\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX1/columns#R_CUSTOMERID index 0\n"
            "set $PREV column /clusters#cluster/databases#database/tables#R_CUSTOMER/columns#R_CUSTOMERID\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER indexes R_TABLEINDEX2\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX2 unique true\n"
            "set $PREV assumeUnique false\n"
            "set $PREV countable true\n"
            "set $PREV type 1\n"
            "set $PREV expressionsjson \"\"\n"
            "set $PREV predicatejson \"\"\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX2 columns R_CUSTOMERID\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX2/columns#R_CUSTOMERID index 0\n"
            "set $PREV column /clusters#cluster/databases#database/tables#R_CUSTOMER/columns#R_CUSTOMERID\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX2 columns R_FIRSTNAME\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX2/columns#R_FIRSTNAME index 1\n"
            "set $PREV column /clusters#cluster/databases#database/tables#R_CUSTOMER/columns#R_FIRSTNAME\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX2 columns R_LASTNAME\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX2/columns#R_LASTNAME index 2\n"
            "set $PREV column /clusters#cluster/databases#database/tables#R_CUSTOMER/columns#R_LASTNAME\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER indexes R_TABLEINDEX3\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX3 unique false\n"
            "set $PREV assumeUnique false\n"
            "set $PREV countable true\n"
            "set $PREV type 1\n"
            "set $PREV expressionsjson \"\"\n"
            "set $PREV predicatejson \"\"\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX3 columns R_FIRSTNAME\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX3/columns#R_FIRSTNAME index 0\n"
            "set $PREV column /clusters#cluster/databases#database/tables#R_CUSTOMER/columns#R_FIRSTNAME\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX3 columns R_LASTNAME\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#R_TABLEINDEX3/columns#R_LASTNAME index 1\n"
            "set $PREV column /clusters#cluster/databases#database/tables#R_CUSTOMER/columns#R_LASTNAME\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER indexes VOLTDB_AUTOGEN_IDX_PK_R_CUSTOMER_R_CUSTOMERID\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#VOLTDB_AUTOGEN_IDX_PK_R_CUSTOMER_R_CUSTOMERID unique true\n"
            "set $PREV assumeUnique false\n"
            "set $PREV countable true\n"
            "set $PREV type 1\n"
            "set $PREV expressionsjson \"\"\n"
            "set $PREV predicatejson \"\"\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#VOLTDB_AUTOGEN_IDX_PK_R_CUSTOMER_R_CUSTOMERID columns R_CUSTOMERID\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#VOLTDB_AUTOGEN_IDX_PK_R_CUSTOMER_R_CUSTOMERID/columns#R_CUSTOMERID index 0\n"
            "set $PREV column /clusters#cluster/databases#database/tables#R_CUSTOMER/columns#R_CUSTOMERID\n"
            "add /clusters#cluster/databases#database/tables#R_CUSTOMER constraints VOLTDB_AUTOGEN_IDX_PK_R_CUSTOMER_R_CUSTOMERID\n"
            "set /clusters#cluster/databases#database/tables#R_CUSTOMER/constraints#VOLTDB_AUTOGEN_IDX_PK_R_CUSTOMER_R_CUSTOMERID type 4\n"
            "set $PREV oncommit \"\"\n"
            "set $PREV index /clusters#cluster/databases#database/tables#R_CUSTOMER/indexes#VOLTDB_AUTOGEN_IDX_PK_R_CUSTOMER_R_CUSTOMERID\n"
            "set $PREV foreignkeytable null\n";

        /*
         * Initialize the engine.  We create our own
         * topend, to make sure we can supply fragments
         * by id, and then make sure we know where the
         * shared buffers are.  Note that calling setBuffers
         * sets the shared buffer pointers, and calling
         * resetReusedResultOutputBuffer causes the engine to
         * use them.
         */
        m_topend.reset(new EngineTestTopend());
        m_engine.reset(new VoltDBEngine(m_topend.get()));
        m_parameter_buffer.reset(new char [4 * 1024]);
        m_result_buffer.reset(new char [1024 * 1024 * 2]);
        m_exception_buffer.reset(new char [4 * 1024]);
        m_engine->setBuffers(m_parameter_buffer.get(), 4 * 1024,
                             m_result_buffer.get(), 1024 * 1024 * 2,
                             m_exception_buffer.get(), 4096);
        m_engine->resetReusedResultOutputBuffer();
        int partitionCount = 3;
        ASSERT_TRUE(m_engine->initialize(this->m_cluster_id, this->m_site_id, 0, 0, "", 0, 1024, DEFAULT_TEMP_TABLE_MEMORY, false));
        m_engine->updateHashinator( HASHINATOR_LEGACY, (char*)&partitionCount, NULL, 0);
        ASSERT_TRUE(m_engine->loadCatalog( -2, m_catalog_string));

        /*
         * Get a link to the catalog and pull out information about it
         */
        m_catalog = m_engine->getCatalog();
        m_cluster = m_catalog->clusters().get("cluster");
        m_database = m_cluster->databases().get("database");
        m_database_id = m_database->relativeIndex();
        catalog::Table *partitioned_catalog_table_customer = m_database->tables().get("D_CUSTOMER");
        m_partitioned_customer_table_id = partitioned_catalog_table_customer->relativeIndex();
        m_partitioned_customer_table = m_engine->getTable(m_partitioned_customer_table_id);
        catalog::Table *replicated_catalog_table_customer = m_database->tables().get("R_CUSTOMER");
        m_replicated_customer_table_id = replicated_catalog_table_customer->relativeIndex();
        m_replicated_customer_table = m_engine->getTable(m_replicated_customer_table_id);

        //
        // Fill in tuples.  The IndexOrder test does not use
        // the contents of the tables.  The ExecutionEngineTest does
        // use them, in a somewhat trivial way.  It would be good if
        // there was some way to fill these in deterministically.
        // Random tuples make debugging unnecessarily difficult,
        // especially if the random see is the time the test
        // started.
        //
        ASSERT_TRUE(m_partitioned_customer_table);
        ASSERT_TRUE(tableutil::addRandomTuples(m_partitioned_customer_table, NUM_OF_TUPLES));
        ASSERT_TRUE(m_replicated_customer_table);
        ASSERT_TRUE(tableutil::addRandomTuples(m_replicated_customer_table, NUM_OF_TUPLES));
    }
    ~ExecutionEngineTest() {
            //
            // When we delete the VoltDBEngine
            // it will cleanup all the tables for us.
            //
        }

    protected:
        CatalogId m_cluster_id;
        CatalogId m_database_id;
        CatalogId m_site_id;
        string m_catalog_string;
        catalog::Catalog *m_catalog; //This is not the real catalog that the VoltDBEngine uses. It is a duplicate made locally to get GUIDs
        catalog::Cluster *m_cluster;
        catalog::Database *m_database;
        catalog::Constraint *m_constraint;
        boost::scoped_ptr<VoltDBEngine>     m_engine;
        boost::scoped_ptr<EngineTestTopend> m_topend;
        Table* m_partitioned_customer_table;
        int m_partitioned_customer_table_id;

        Table* m_replicated_customer_table;
        int m_replicated_customer_table_id;
        void compareTables(Table *first, Table* second);
        boost::shared_array<char>m_result_buffer;
        boost::shared_array<char>m_exception_buffer;
        boost::shared_array<char>m_parameter_buffer;
};

/* Check the order of index vector
 * Index vector should follow the order of primary key first, all unique indices afterwards, and all the non-unique indices at the end.
 */
TEST_F(ExecutionEngineTest, IndexOrder) {
    ASSERT_TRUE(m_partitioned_customer_table->primaryKeyIndex() == m_partitioned_customer_table->allIndexes()[0]);
    ASSERT_TRUE(m_partitioned_customer_table->allIndexes()[1]->isUniqueIndex());
    ASSERT_FALSE(m_partitioned_customer_table->allIndexes()[2]->isUniqueIndex());
    ASSERT_FALSE(m_partitioned_customer_table->allIndexes()[3]->isUniqueIndex());
}

// ------------------------------------------------------------------
// Execute_PlanFragmentInfo
// ------------------------------------------------------------------
// This plan is the plan generated from the query in the test
// org.voltdb.planner.TestPlansENG10022.testPlanENG10022.  It
// can be generated with this onerously complicated procedure.
// 1. Run the test with the JVM command line parameter -Dmumble=compilerdebug.
//    It doesn't matter what property name you use, mumble in this
//    case.  But the property value must be compilerdebug.  An
//    easy way to do this in Eclipse is to create a JRE configuration
//    with -Dmumble=compilerdebug and run the testENG10022 test with
//    this configuration.  Perhaps setting VOLTDB_OPTS when running
//    the ant target for the test works as well.
// 2. When the test runs there will be a folder called debugoutput
//    wherever the test is run.  In the Eclipse method above this will
//    be the project folder, which is the root of the source folder.
//    There you will find a file with name something like
//           debugoutput/statement_plans/ENG-10022-stmt-0_json.txt.
//    The contents of this file should be this string.
// 3. Some emacs cleanup will be required to paste this into
//    the source code as a string.  Escape double quotes and add
//    initial double quotes, '\n' lines and terminal double quotes
//    as usual.
namespace {
std::string plan =
        "{\n"
        "    \"EXECUTE_LIST\": [\n"
        "        2,\n"
        "        1\n"
        "    ],\n"
        "    \"PLAN_NODES\": [\n"
        "        {\n"
        "            \"CHILDREN_IDS\": [2],\n"
        "            \"ID\": 1,\n"
        "            \"PLAN_NODE_TYPE\": \"SEND\"\n"
        "        },\n"
        "        {\n"
        "            \"ID\": 2,\n"
        "            \"INLINE_NODES\": [{\n"
        "                \"ID\": 3,\n"
        "                \"OUTPUT_SCHEMA\": [\n"
        "                    {\n"
        "                        \"COLUMN_NAME\": \"CID\",\n"
        "                        \"EXPRESSION\": {\n"
        "                            \"COLUMN_IDX\": 0,\n"
        "                            \"TYPE\": 32,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        }\n"
        "                    },\n"
        "                    {\n"
        "                        \"COLUMN_NAME\": \"CID2\",\n"
        "                        \"EXPRESSION\": {\n"
        "                            \"LEFT\": {\n"
        "                                \"ISNULL\": false,\n"
        "                                \"TYPE\": 30,\n"
        "                                \"VALUE\": 2,\n"
        "                                \"VALUE_TYPE\": 5\n"
        "                            },\n"
        "                            \"RIGHT\": {\n"
        "                                \"COLUMN_IDX\": 0,\n"
        "                                \"TYPE\": 32,\n"
        "                                \"VALUE_TYPE\": 5\n"
        "                            },\n"
        "                            \"TYPE\": 3,\n"
        "                            \"VALUE_TYPE\": 6\n"
        "                        }\n"
        "                    }\n"
        "                ],\n"
        "                \"PLAN_NODE_TYPE\": \"PROJECTION\"\n"
        "            }],\n"
        "            \"LOOKUP_TYPE\": \"GTE\",\n"
        "            \"PLAN_NODE_TYPE\": \"INDEXSCAN\",\n"
        "            \"PURPOSE\": 3,\n"
        "            \"SORT_DIRECTION\": \"ASC\",\n"
        "            \"TARGET_INDEX_NAME\": \"VOLTDB_AUTOGEN_IDX_PK_R_CUSTOMER_R_CUSTOMERID\",\n"
        "            \"TARGET_TABLE_ALIAS\": \"R_CUSTOMER\",\n"
        "            \"TARGET_TABLE_NAME\": \"R_CUSTOMER\"\n"
        "        }\n"
        "    ]\n"
        "}\n"
        "\n";
}

namespace {
/*
 * Set this to true to dump the result buffer as an
 * array of bytes.  This is very helpful to figure out
 * problems with wire protocol understanding.
 */
bool debug_dump  = false;

void dumpResultTable(const char *buffer, size_t size) {
    const char *start = "";
    const char *sep = "";
    printf("      ");
    for (int idx = 0; idx < 8; idx += 1) {
        printf("%02d  ", idx);
    }
    printf("\n");
    for (int idx = 0; idx < 500 && idx < size; idx += 1) {
        if (idx % 8 == 0) {
            printf("%s%03d.) ", start, idx);
            start = "\n";
            sep = "";
        }
        printf("%s%02x", sep, 0xff & static_cast<unsigned int>(buffer[idx]));
        sep = ", ";
    }
    printf("\n");
}

}

TEST_F(ExecutionEngineTest, Execute_PlanFragmentInfo) {
    //
    // Given a PlanFragmentInfo data object, make the m_engine execute it,
    // and validate the results.
    //
    // Load the plan in the top end.  We'll use fragmentId as
    // a length one array below.
    //
    m_topend->addPlan(100, plan);
    fragmentId_t fragmentId = 100;

    // Make sure the parameter buffer is filled
    // with healthful zeros, and then create an input
    // deserializer.
    memset(m_parameter_buffer.get(), 0, 4 * 1024);
    ReferenceSerializeInputBE emptyParams(m_parameter_buffer.get(), 4 * 1024);

    //
    // Execute the plan.  You'd think this would be more
    // impressive.
    //
    m_engine->executePlanFragments(1, &fragmentId, NULL, emptyParams, 1000, 1000, 1000, 1000, 1);

    // Fetch the results.  We have forced them to be written
    // to our own buffer in the local engine.  But we don't
    // know how much of the buffer is actually used.  So we
    // need to query the engine.
    size_t result_size = m_engine->getResultsSize();
    if (debug_dump) {
        dumpResultTable(m_result_buffer.get(), result_size);
    }

    boost::scoped_ptr<TempTable> result(loadTableFrom(m_result_buffer.get(), result_size));
    assert(result.get() != NULL);
    ASSERT_TRUE(result != NULL);

    const TupleSchema* res_schema = result->schema();
    TableTuple tuple(res_schema);
    boost::scoped_ptr<TableIterator> iter(result->makeIterator());
    if (!iter->hasNext()) {
        printf("No results!!\n");
    }
    int32_t count;
    for (count = 0; iter->next(tuple); count += 1) {
        /*
         * This is true because of collusion between the query
         * and the test.  The query selects two values, both
         * integral, and the second is twice the first.
         */
        int64_t v0 = ValuePeeker::peekAsBigInt(tuple.getNValue(0));
        int64_t v1 = ValuePeeker::peekAsBigInt(tuple.getNValue(1));
        ASSERT_TRUE(2*v0 == v1);
    }
    if (debug_dump) {
        printf("Success after inspecting %d rows\n", count);
    }
}

int main() {
     return TestSuite::globalInstance()->runAll();
}
