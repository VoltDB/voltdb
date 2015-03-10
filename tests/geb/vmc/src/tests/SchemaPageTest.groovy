/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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


package vmcTest.tests

import org.junit.Test

import java.util.List;
import spock.lang.*
import vmcTest.pages.*

/**
 * This class tests navigation between pages (or tabs), of the the VoltDB
 * Management Center (VMC), which is the VoltDB (new) web UI.
 */
class SchemaPageTest extends TestBase {

    static final String DDL_SOURCE_FILE = 'src/resources/expectedDdlSource.txt';
    static Boolean runningGenqa = null;
    @Shared def ddlSourceFile = new File(DDL_SOURCE_FILE)
    @Shared def ddlExpectedSourceLines = []
    //@Shared def fileLinesPairs = [ [ddlSourceFile, ddlExpectedSourceLines] ]
    //@Shared def slurper = new JsonSlurper()

    def setupSpec() { // called once, before any tests
        // Move contents of the specified file into memory
        ddlExpectedSourceLines = getFileLines(ddlSourceFile)
        //fileLinesPairs.each { file, lines -> lines.addAll(getFileLines(file)) }
    }

    def setup() { // called before each test
        // TestBase.setup gets called first (automatically)
        when: 'click the Schema (page) link'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage
    }

    /**
     * Returns whether or not we are currently running the 'genqa' test app,
     * based on whether the expected DDL Source is listed on the page.
     * @param spdst - the SchemaPageDdlSourceTab from which to get the DDL source.
     * @return true if we are currently running the 'genqa' test app.
     */
    static boolean isRunningGenqa(SchemaPageDdlSourceTab spdst) {
        if (runningGenqa == null) {
            def ddlSource = spdst.getDdlSource()
            runningGenqa = true
            for (table in ['EXPORT_MIRROR_PARTITIONED_TABLE', 'PARTITIONED_TABLE', 'REPLICATED_TABLE']) {
                if (!ddlSource.contains(table)) {
                    runningGenqa = false
                    break
                }
            }
        }
        return runningGenqa
    }

    def 'confirm Overview tab open initially'() {
        expect: 'Overview tab open initially'
        page.isSchemaPageOverviewTabOpen()
    }

    def navigateSchemaPageTabs() {
        when: 'click the Schema (sub-)link (from Overview tab)'
        page.openSchemaPageSchemaTab()
        then: 'should be on Schema tab (of Schema page)'
        at SchemaPageSchemaTab

        when: 'click the Procedures & SQL link (from Schema tab)'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'should be on Procedures & SQL tab'
        at SchemaPageProceduresAndSqlTab

        when: 'click the Size Worksheet link (from Procedures & SQL tab)'
        page.openSchemaPageSizeWorksheetTab()
        then: 'should be on Size Worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click the DDL Source link (from Size Worksheet tab)'
        page.openSchemaPageDdlSourceTab()
        then: 'should be on DDL Source tab'
        at SchemaPageDdlSourceTab

        when: 'click the Overview link (from DDL Source tab)'
        page.openSchemaPageOverviewTab()
        then: 'should be on Overview tab'
        at SchemaPageOverviewTab

        when: 'click the DDL Source link (from Overview tab)'
        page.openSchemaPageDdlSourceTab()
        then: 'should be on DDL Source tab'
        at SchemaPageDdlSourceTab

        when: 'click the Size Worksheet link (from DDL Source tab)'
        page.openSchemaPageSizeWorksheetTab()
        then: 'should be on Size Worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click the Procedures & SQL link (from Size Worksheet tab)'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'should be on Procedures & SQL tab'
        at SchemaPageProceduresAndSqlTab

        when: 'click the Schema (sub-)link (from Procedures & SQL tab)'
        page.openSchemaPageSchemaTab()
        then: 'should be on Schema tab (of Schema page)'
        at SchemaPageSchemaTab

        when: 'click the Overview link (from Schema tab)'
        page.openSchemaPageOverviewTab()
        then: 'should be on Overview tab'
        at SchemaPageOverviewTab
    }

    /**
     * Check that the DDL Source displayed on the DDL Source tab matches the
     * expected list (for the 'genqa' test app).
     */
    def checkSchemaTab() {
        when: 'click the Schema tab link'
        page.openSchemaPageSchemaTab()

        then: 'should be on Schema tab'
        at SchemaPageSchemaTab

        // TODO: should make these into tests, not just debug print
        cleanup: 'for now, just print the table, row-wise'
        debugPrint "\nSchema (tab) table, row-wise:\n" + page.getSchemaTableByRow()

        and: 'for now, just print the table, column-wise'
        debugPrint "\nSchema (tab) table, column-wise:\n" + page.getSchemaTableByColumn()
    }

    /**
     * Check that the DDL Source displayed on the DDL Source tab matches the
     * expected list (for the 'genqa' test app).
     */
    def checkDdlSourceTab() {
        when: 'click the DDL Source tab link'
        page.openSchemaPageDdlSourceTab()

        then: 'should be on DDL Source tab'
        at SchemaPageDdlSourceTab

        when: 'get the DDL Source (lines of text), from the DDL Source tab'
        List<String> ddlActualSourceLines = page.getDdlSourceLines()

        then: 'test & remove the first few lines, which are descriptive text'
        ddlActualSourceLines.remove(0).startsWith('-- This file was generated by VoltDB version 5.')
        ddlActualSourceLines.remove(0).equals('-- This file represents the current database schema.')
        ddlActualSourceLines.remove(0).equals('-- Use this file as input to reproduce the current database structure in another database instance.')
        ddlActualSourceLines.remove(0).isEmpty()

        and: 'DDL Source should match expected text'
        printAndCompare('DDL Source', DDL_SOURCE_FILE, isRunningGenqa(page), ddlExpectedSourceLines, ddlActualSourceLines)
    }


    //schema (SYSTEM OVERVIEW)


    def "check refresh button"(){

        when:
        at SchemaPage
        schema.refreshbutton.isDisplayed()
        then:
        schema.refreshbutton.click()
    }


    def "check system overview title"(){

        when:
        at SchemaPage
        schema.systemoverviewTitle.isDisplayed()
        then:
        schema.systemoverviewTitle.text().toLowerCase().equals("System Overview".toLowerCase())
    }


    def "check system overview content i.e, mode"(){
        when:
        at SchemaPage
        schema.modeTitle.isDisplayed()
        then:
        schema.modeTitle.text().toLowerCase().equals("Mode".toLowerCase())

    }

    def "check system overview content i.e, voltDBversion"(){
        when:
        at SchemaPage
        schema.voltdbversion.isDisplayed()
        then:
        schema.voltdbversion.text().toLowerCase().equals("VoltDB Version".toLowerCase())

    }

    def "check system overview content i.e, BuildString"(){
        when:
        at SchemaPage
        schema.buildstring.isDisplayed()
        then:
        schema.buildstring.text().toLowerCase().equals("Buildstring".toLowerCase())

    }

    def "check system overview content i.e, Cluster composition"(){
        when:
        at SchemaPage
        schema.clustercomposition.isDisplayed()
        then:
        schema.clustercomposition.text().toLowerCase().equals("Cluster Composition".toLowerCase())

    }

    def "check system overview content i.e, Running Since"(){
        when:
        at SchemaPage
        schema.runningsince.isDisplayed()
        then:
        schema.runningsince.text().toLowerCase().equals("Running Since".toLowerCase())

    }


       def "check system overview content i.e, mode-value"(){
           when:
           at SchemaPage
           schema.modevalue.isDisplayed()
           then:
           schema.modevalue.text().toLowerCase().equals("RUNNING".toLowerCase())

       }

       def "check system overview content i.e, voltDBversion-value"(){
           when:
           at SchemaPage
           schema.versionvalue.isDisplayed()
           then:
           schema.versionvalue.text().toLowerCase().equals("5.1".toLowerCase())

       }

       def "check system overview content i.e, buildstring-value"(){
           when:
           at SchemaPage
           schema.buildstringvalue.isDisplayed()
           then:
           schema.buildstringvalue.text().toLowerCase().equals("voltdb-4.7-2198-g23683d1-dirty-local".toLowerCase())

       }


       def "check system overview content i.e, clusterComposition-value"(){
           when:
           at SchemaPage
           schema.clustercompositionvalue.isDisplayed()
           then:
           schema.clustercompositionvalue.text().toLowerCase().equals("1 hosts with 8 sites (8 per host)".toLowerCase())

       }


     def "check system overview content i.e, RunningSince-value"(){
         when:
         at SchemaPage
         schema.runningsincevalue.isDisplayed()
         then:
        // schema.runningsincevalue.text().toLowerCase().equals("Tue Feb 24 08:20:31 GMT+00:00 2015 (0d 3h 56m)".toLowerCase())
         schema.runningsincevalue.isDisplayed()
     }

    //schema CATALOG OVERVIEW STATISTICS


       def "check Catalog Overview Statistics title"(){
           when:
           at SchemaPage
           schema.catalogoverviewstatistic.isDisplayed()
           then:
           schema.catalogoverviewstatistic.text().toLowerCase().equals("Catalog Overview Statistics".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Compiled by VoltDB Version"(){
           when:
           at SchemaPage
           schema.compiledversion.isDisplayed()
           then:
           schema.compiledversion.text().toLowerCase().equals("Compiled by VoltDB Version".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Compiled on"(){
           when:
           at SchemaPage
           schema.compiledonTitle.isDisplayed()
           then:
           schema.compiledonTitle.text().toLowerCase().equals("Compiled on".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Table Count"(){
           when:
           at SchemaPage
           schema.tablecount.isDisplayed()
           then:
           schema.tablecount.text().toLowerCase().equals("Table Count".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Materialized View Count"(){
           when:
           at SchemaPage
           schema.materializedviewcount.isDisplayed()
           then:
           schema.materializedviewcount.text().toLowerCase().equals("Materialized View Count".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Index Count"(){
           when:
           at SchemaPage
           schema.indexcount.isDisplayed()
           then:
           schema.indexcount.text().toLowerCase().equals("Index Count".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Procedure Count"(){
           when:
           at SchemaPage
           schema.procedurecount.isDisplayed()
           then:
           schema.procedurecount.text().toLowerCase().equals("Procedure Count".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, SQL Statement Count"(){
           when:
           at SchemaPage
           schema.sqlstatementcount.isDisplayed()
           then:
           schema.sqlstatementcount.text().toLowerCase().equals("SQL Statement Count".toLowerCase())

       }


     def "check Catalog Overview Statistics content i.e, compiled by voltdb version-value"(){
         when:
         at SchemaPage
         schema.compiledversionvalue.isDisplayed()
         then:
         schema.compiledversionvalue.text().toLowerCase().equals("5.1".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, compiled on-value"(){
         when:
         at SchemaPage
         schema.compiledonTitlevalue.isDisplayed()
         then:
         schema.compiledonTitlevalue.text().toLowerCase().equals("Tue, 24 Feb 2015 08:20:28 GMT+00:00".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, table count-value"(){
         when:
         at SchemaPage
         schema.tablecountvalue.isDisplayed()
         then:
         schema.tablecountvalue.text().toLowerCase().equals("3 (1 partitioned / 2 replicated)".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, materilized view count-value"(){
         when:
         at SchemaPage
         schema.materializedviewcountvalue.isDisplayed()
         then:
         schema.materializedviewcountvalue.text().toLowerCase().equals("2".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, index count-value"(){
         when:
         at SchemaPage
         schema.indexcountvalue.isDisplayed()
         then:
         schema.indexcountvalue.text().toLowerCase().equals("4".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, procedure count-value"(){
         when:
         at SchemaPage
         schema.procedurecountvalue.isDisplayed()
         then:
         schema.procedurecountvalue.text().toLowerCase().equals("5 (1 partitioned / 4 replicated) (3 read-only / 2 read-write)".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, sql statement count-value"(){
         when:
         at SchemaPage
         schema.sqlstatementcountvalue.isDisplayed()
         then:
         schema.sqlstatementcountvalue.text().toLowerCase().equals("10".toLowerCase())

     }


    //documentation


       def "check documentation right footer"(){
           when:
           at SchemaPage
           schema.documentationrightlabel.isDisplayed()
           then:
           schema.documentationrightlabel.text().toLowerCase().equals("Generated by VoltDB 5.1 on 24 Feb 2015 08:20:28 GMT+00:00".toLowerCase())

       }

       def "check documentation url"(){
           when:
           at SchemaPage
           schema.documentationurl.isDisplayed()
           then:
           schema.documentationurl.click()

       }


       def "check DDL source Title"(){
           when:
           at SchemaPage
           schema.ddlsourceTitle.isDisplayed()
           then:
           schema.ddlsourceTitle.isDisplayed()
       }

       def "check DDL source download"() {
           when:
           at SchemaPage
           schema.ddlsourcedownload.isDisplayed()
           then:
           schema.ddlsourcedownload.click()
       }

       def "check DDL source bunch of queries"(){
           when:
           at SchemaPage
           schema.ddlsourcequeries.isDisplayed()
           then:
           schema.ddlsourcequeries.isDisplayed()

       }




    def "check schema sub Title in schema sub page"(){
        when:
        at SchemaPage
        schema.schemasubTitle.isDisplayed()
        then:
        schema.schemasubTitle.text().toLowerCase().equals("Schema".toLowerCase())
    }


    def "check expand all check box"(){
        when:
        at SchemaPage
        schema.expandallcheck.isDisplayed()

        schema.expandallcheck.click()
        then:
        at SchemaPage
        schema.expandallcheck.click()
       // at SchemaPage
    }

    def "check name sorting button"(){
        when:
        at SchemaPage
        schema.namesort.isDisplayed()

        schema.namesort.click()
        then:
        at SchemaPage
        schema.namesort.click()
       // at SchemaPage
        }


    def "check clicking vote button"(){
        when:
        at SchemaPage
        schema.clickvote.isDisplayed()

        schema.clickvote.click()
        then:
        at SchemaPage
        schema.clickvote.click()
       // at SchemaPage
    }


    def "check clicking vote by phone number button"(){
        when:
        at SchemaPage
        schema.clickvotebyno.isDisplayed()

        schema.clickvotebyno.click()
        then:
        at SchemaPage
        schema.clickvotebyno.click()
      //  at SchemaPage
    }



    def "check clicking vote by contestant number button"(){
        when:
        at SchemaPage
        schema.clickvotebycontestantno.isDisplayed()

        schema.clickvotebycontestantno.click()
        then:
         at SchemaPage
        schema.clickvotebycontestantno.click()
      //  at SchemaPage
    }


    def "check clicking contestant button"(){
        when:
        at SchemaPage
        schema.clickcontestant.isDisplayed()

        schema.clickcontestant.click()
        then:
        at SchemaPage
        schema.clickcontestant.click()
        //  at SchemaPage
    }


    def "check clicking area code state button"(){
        when:
        at SchemaPage
        schema.clickareacodestate.isDisplayed()

        schema.clickareacodestate.click()
        then:
        at SchemaPage
        schema.clickareacodestate.click()
        //  at SchemaPage
    }


    def "check clicking area code state button in further process"(){
        when:
        at SchemaPage
        schema.clickareacodestate.isDisplayed()

        schema.clickareacodestate.click()
        then:
        at SchemaPage
        schema.clickautogenconstraint.click()
          at SchemaPage
        schema.clickvoteinautogen.click()
        at SchemaPage
        schema.backtoautogen.click()
    }


     def "check for clicking ddlsource button"(){
         when:
         at SchemaPage
         schema.viewddlsourcebutton.isDisplayed()
         then:
         at SchemaPage
         schema.viewddlsourcebutton.click()
         at SchemaPage
         schema.schemasubbutton.click()

         //  at SchemaPage
     }



      def "check type sorting button"(){
          when:
          at SchemaPage
          schema.typesort.isDisplayed()

          schema.typesort.click()
          then:
          at SchemaPage
          schema.typesort.click()

          at SchemaPage
      }


     def "check partitioning sort button"(){
         when:
         at SchemaPage
         schema.partitioningsort.isDisplayed()

         schema.partitioningsort.click()
         then:
         at SchemaPage
         schema.partitioningsort.click()

         at SchemaPage
     }


     def "check column sort button"(){
         when:
         at SchemaPage
         schema.columnsort.isDisplayed()

         schema.columnsort.click()
         then:
         at SchemaPage
         schema.columnsort.click()

         at SchemaPage
     }


     def "check index sort button"(){
         when:
         at SchemaPage
         schema.indexsort.isDisplayed()

         schema.indexsort.click()
         then:
         at SchemaPage
         schema.indexsort.click()

         at SchemaPage
     }


     def "check pkey sort button"(){
         when:
         at SchemaPage
         schema.pkeysort.isDisplayed()

         schema.pkeysort.click()
         then:
         at SchemaPage
         schema.pkeysort.click()

         at SchemaPage
     }


     def "check tuple limit sort button"(){
         when:
         at SchemaPage
         schema.tuplelimitsort.isDisplayed()

         schema.tuplelimitsort.click()
         then:
         at SchemaPage
         schema.tuplelimitsort.click()

         at SchemaPage
     }

    //schema for input


     def "check input in name sort"(){
         when:
         at SchemaPage
         schema.nameinput.isDisplayed()

         then:
         schema.nameinput.value("Area")
         schema.nameinput.value("")
         at SchemaPage

     }


     def "check input in type sort"(){
         when:
         at SchemaPage
         schema.typeinput.isDisplayed()

         then:
         schema.typeinput.value("Mat")

         at SchemaPage
         schema.typeinput.value("")
     }


     def "check input in partitioning sort"(){
         when:
         at SchemaPage
         schema.partitioninput.isDisplayed()

         then:
         schema.partitioninput.value("Pat")

         at SchemaPage
         schema.partitioninput.value("")
     }


     def "check input in columns sort"(){
         when:
         at SchemaPage
         schema.columninput.isDisplayed()

         then:
         schema.columninput.value("2")

         at SchemaPage
         schema.columninput.value("")
     }


     def "check input in Indexes sort"(){
         when:
         at SchemaPage
         schema.indexinput.isDisplayed()

         then:
         schema.indexinput.value("1")

         at SchemaPage
         schema.indexinput.value("")
     }


     def "check input in Pkey sort"(){
         when:
         at SchemaPage
         schema.pkeyinput.isDisplayed()

         then:
         schema.pkeyinput.value("Has")

         at SchemaPage
         schema.pkeyinput.value("")
     }


    def "check input in Tuple sort"(){
        when:
        at SchemaPage
        schema.tupleinput.isDisplayed()

        then:
        schema.tupleinput.value("No")

        at SchemaPage
        schema.tupleinput.value("")
    }



    def "click SQL Query"(){

        when:
        at SchemaPage
        schema.clicksqlquery.isDisplayed()
        then:
        schema.clicksqlquery.click()
    }




     def "check Procedure & SQL Title in Procedure & SQL page"(){
         when:
         at SchemaPage
         schema.proceduresqlTitle.isDisplayed()
         then:
         schema.proceduresqlTitle.text().toLowerCase().equals("Procedures & SQL".toLowerCase())
     }



     def "check expand all check box in procedure & SQL"(){
         when:
         at SchemaPage
         schema.expandall_check.isDisplayed()

         schema.expandall_check.click()
         then:
         at SchemaPage
         schema.expandall_check.click()
         // at SchemaPage
     }



    def "check procedure name sort in procedure & SQL"(){
        when:
        at SchemaPage
        schema.procedurenamesort.isDisplayed()

        schema.procedurenamesort.click()
        then:
        at SchemaPage
        schema.procedurenamesort.click()
        // at SchemaPage
    }


    def "check parameters sort in procedure & SQL"(){
        when:
        at SchemaPage
        schema.parameterssort.isDisplayed()

        schema.parameterssort.click()
        then:
        at SchemaPage
        schema.parameterssort.click()
        // at SchemaPage
    }


    def "check partition sort in procedure & SQL"(){
        when:
        at SchemaPage
        schema.partitionsort.isDisplayed()

        schema.partitionsort.click()
        then:
        at SchemaPage
        schema.partitionsort.click()
        // at SchemaPage
    }


    def "check rw sort in procedure & SQL"(){
        when:
        at SchemaPage
        schema.rwsort.isDisplayed()

        schema.rwsort.click()
        then:
        at SchemaPage
        schema.rwsort.click()
        // at SchemaPage
    }


    def "check access sort in procedure & SQL"(){
        when:
        at SchemaPage
        schema.accesssort.isDisplayed()

        schema.accesssort.click()
        then:
        at SchemaPage
        schema.accesssort.click()
        // at SchemaPage
    }


    def "check attributes sort in procedure & SQL"(){
        when:
        at SchemaPage
        schema.attributessort.isDisplayed()

        schema.attributessort.click()
        then:
        at SchemaPage
        schema.attributessort.click()
        // at SchemaPage
    }





      def "check Title on size worksheet page"(){
          when:
          at SchemaPage
          schema.memoryusedbyTitle.isDisplayed()
          then:
          schema.memoryusedbyTitle.text().toLowerCase().equals("Estimate Memory Used by User Data".toLowerCase())
          at SchemaPage
      }



      def "check paragraph on size worksheet page"(){
          when:
          at SchemaPage

          then:
          schema.checkparagraph.isDisplayed()
          at SchemaPage
      }


    def "check List on size without unit on size worksheet page"(){
        when:
        at SchemaPage
        schema.sizewithoutunittxt.isDisplayed()
        then:
        schema.sizewithoutunittxt.text().toLowerCase().equals("Sizes without units are in bytes.".toLowerCase())
        at SchemaPage
    }


    def "check List on useful result on size worksheet page"(){
        when:
        at SchemaPage
        schema.usefulresultstxt.isDisplayed()
        then:
        schema.usefulresultstxt.text().toLowerCase().equals("For more useful results replace row counts with better estimates.".toLowerCase())
        at SchemaPage
    }


    def "check List on calculated fields on size worksheet page"(){
        when:
        at SchemaPage
        schema.calculatedfieldstxt.isDisplayed()
        then:
        schema.calculatedfieldstxt.text().toLowerCase().equals("All calculated fields update after leaving any input field.".toLowerCase())
        at SchemaPage
    }


    def "check warning page on size worksheet page"(){
        when:
        at SchemaPage
        schema.warningpagetxt.isDisplayed()
        then:
        schema.warningpagetxt.text().toLowerCase().equals("Warning: Page refresh resets all input fields.".toLowerCase())
        at SchemaPage
    }


    def "check varchar column page on size worksheet page"(){
        when:
        at SchemaPage

        then:
        schema.varcharcolumntxt.isDisplayed()
        at SchemaPage
    }


     def "check size analysis Title on size worksheet page"(){
         when:
         at SchemaPage
         schema.sizeanalysissummaryTitle.isDisplayed()
         then:
         schema.sizeanalysissummaryTitle.text().toLowerCase().equals("Size Analysis Summary".toLowerCase())
         at SchemaPage
     }


     def "check list of tables text on size worksheet page of size analysis"(){
         when:
         at SchemaPage

         then:
         schema.tablestxt.isDisplayed()
        // schema.tablestxt.text().toLowerCase().equals("tables whose row data is expected to use between".toLowerCase())
         at SchemaPage
     }


     def "check list of materialized views text on size worksheet page of size analysis"(){
         when:
         at SchemaPage

         then:
         schema.materializedtxt.isDisplayed()
         //schema.materializedtxt.text().toLowerCase().equals("materialized views whose row data is expected to use about".toLowerCase())
         at SchemaPage
     }


     def "check list of indexes text on size worksheet page of size analysis"(){
         when:
         at SchemaPage

         then:
         schema.indexestxt.isDisplayed()
         //schema.indexestxt.text().toLowerCase().equals("indexes whose key data and overhead is expected to use about".toLowerCase())
         at SchemaPage
     }


     def "check list of total user data text on size worksheet page of size analysis"(){
         when:
         at SchemaPage
         schema.totalusertxt.isDisplayed()
         then:
         schema.totalusertxt.text().toLowerCase().equals("Total user data is expected to use between".toLowerCase())
         at SchemaPage
     }


     def "check first value list of tables text on size worksheet page of size analysis"(){
         when:
         at SchemaPage
         schema.tablestxtvalue1.isDisplayed()
         then:
         schema.tablestxtvalue1.text().toLowerCase().equals("132 K".toLowerCase())
         at SchemaPage
     }


     def "check second value list of tables text on size worksheet page of size analysis"(){
         when:
         at SchemaPage
         schema.tabletxtvalue2.isDisplayed()
         then:
         schema.tabletxtvalue2.text().toLowerCase().equals("324 K".toLowerCase())
         at SchemaPage
     }


     def "check  value list of materialized views text on size worksheet page of size analysis"(){
         when:
         at SchemaPage
         schema.materializedtxtvalue.isDisplayed()
         then:
         schema.materializedtxtvalue.text().toLowerCase().equals("29 K".toLowerCase())
         at SchemaPage
     }


     def "check  value list of indexes text on size worksheet page of size analysis"(){
         when:
         at SchemaPage
         schema.indexestxtvalue.isDisplayed()
         then:
         schema.indexestxtvalue.text().toLowerCase().equals("187 K".toLowerCase())
         at SchemaPage
     }


     def "check  first value list of total user text on size worksheet page of size analysis"(){
         when:
         at SchemaPage
         schema.totalusertxtvalue1.isDisplayed()
         then:
         schema.totalusertxtvalue1.text().toLowerCase().equals("348 K".toLowerCase())
         at SchemaPage
     }


     def "check  second value list of total user text on size worksheet page of size analysis"(){
         when:
         at SchemaPage
         schema.totalusertxtvalue2.isDisplayed()
         then:
         schema.totalusertxtvalue2.text().toLowerCase().equals("540 K".toLowerCase())
         at SchemaPage
     }

    //sort in sizeworksheet tab


    def "check name sorting button in size worksheet"(){
        when:
        at SchemaPage
        schema.namesortinsizeworksheet.isDisplayed()

        schema.namesortinsizeworksheet.click()
        then:
        at SchemaPage
        schema.namesortinsizeworksheet.click()
        // at SchemaPage
    }



    def "check type sorting button in size worksheet"(){
        when:
        at SchemaPage
        schema.typesortinsizeworksheet.isDisplayed()

        schema.typesortinsizeworksheet.click()
        then:
        at SchemaPage
        schema.typesortinsizeworksheet.click()
        // at SchemaPage
    }


    def "check count sorting button in size worksheet"(){
        when:
        at SchemaPage
        schema.countsortinsizeworksheet.isDisplayed()

        schema.countsortinsizeworksheet.click()
        then:
        at SchemaPage
        schema.countsortinsizeworksheet.click()
        // at SchemaPage
    }


    def "check Row min sorting button in size worksheet"(){
        when:
        at SchemaPage
        schema.rowminsortinsizeworksheet.isDisplayed()

        schema.rowminsortinsizeworksheet.click()
        then:
        at SchemaPage
        schema.rowminsortinsizeworksheet.click()
        // at SchemaPage
    }


    def "check Row max sorting button in size worksheet"(){
        when:
        at SchemaPage
        schema.rowmaxsortinsizeworksheet.isDisplayed()

        schema.rowmaxsortinsizeworksheet.click()
        then:
        at SchemaPage
        schema.rowmaxsortinsizeworksheet.click()
        // at SchemaPage
    }


    def "check index min sorting button in size worksheet"(){
        when:
        at SchemaPage
        schema.indexminsortinsizeworksheet.isDisplayed()

        schema.indexminsortinsizeworksheet.click()
        then:
        at SchemaPage
        schema.indexminsortinsizeworksheet.click()
        // at SchemaPage
    }


    def "check index max sorting button in size worksheet"(){
        when:
        at SchemaPage
        schema.indexmaxsortinsizeworksheet.isDisplayed()

        schema.indexmaxsortinsizeworksheet.click()
        then:
        at SchemaPage
        schema.indexmaxsortinsizeworksheet.click()
        // at SchemaPage
    }



    def "check table min sorting button in size worksheet"(){
        when:
        at SchemaPage
        schema.tableminsortinsizeworksheet.isDisplayed()

        schema.tableminsortinsizeworksheet.click()
        then:
        at SchemaPage
        schema.tableminsortinsizeworksheet.click()
        // at SchemaPage
    }


    def "check table max sorting button in size worksheet"(){
        when:
        at SchemaPage
        schema.tablemaxsortinsizeworksheet.isDisplayed()

        schema.tablemaxsortinsizeworksheet.click()
        then:
        at SchemaPage
        schema.tablemaxsortinsizeworksheet.click()
        // at SchemaPage
    }


    def "Add a table in Tables and check it"() {
        String createQuery = page.getQueryToCreateTable()
        String deleteQuery = page.getQueryToDeleteTable()
        String tablename = page.getTablename()

        when: 'sql query is clicked'
        page.gotoSqlQuery()
        then: 'at sql query'
        at SqlQueryPage

        when: 'set create query in the box'
        page.setQueryText(createQuery)
        then: 'run the query'
        page.runQuery()

        when: 'Schema tab is clicked'
        page.gotoSchema()
        then: 'at Schema Page'
        at SchemaPage

        when: 'click schema sub tab'
        page.gotoSchemaSubTab()
        then: 'at schema sub tab'
        at SchemaPageSchemaTab

        when: 'send tablename for searching'
        page.sendTablename(tablename)
        then: 'row is displayed'
        waitFor(20,10) { page.test.isDisplayed() }
        if ( page.test.isDisplayed() == false ){
            println("VMC-108:Auto Test Code for Schema in Schema Tab FAILED")
            assert false
        }

        when: 'sql query is clicked'
        page.gotoSqlQuery()
        then: 'at sql query'
        at SqlQueryPage

        when: 'set delete query in the box'
        page.setQueryText(deleteQuery)
        then: 'run the query'
        page.runQuery()
        println("VMC-108:Auto Test Code for Schema in Schema Tab PASSED")
    }


    def "check ascending and descending on Name"() {
        when: 'click schema sub tab'
        page.gotoSchemaSubTab()
        then: 'at schema sub tab'
        at SchemaPageSchemaTab

        when: 'click on the name'
        page.name.click()
        then: 'check the ascending class'
        if ( page.checkAscending() ){

        }
        else{
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on Name FAILED")
        }

        when: 'click on the name'
        page.name.click()
        then: 'check the descending class'
        if ( page.checkDescending() )
            println("VMC-108:Auto Test Code for ascending and descending on Name PASSED")
        else {
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on Name FAILED")
        }
    }

    def "check ascending and descending on Type"() {
        when: 'click schema sub tab'
        page.gotoSchemaSubTab()
        then: 'at schema sub tab'
        at SchemaPageSchemaTab

        when: 'click on the type'
        page.type.click()
        then: 'check the ascending class'
        if ( page.checkAscending() ){

        }
        else{
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on Type FAILED")
        }

        when: 'click on the type'
        page.type.click()
        then: 'check the descending class'
        if ( page.checkDescending() )
            println("VMC-108:Auto Test Code for ascending and descending on Type PASSED")
        else {
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on Type FAILED")
        }
    }

    def "check ascending and descending on Partitioning"() {
        when: 'click schema sub tab'
        page.gotoSchemaSubTab()
        then: 'at schema sub tab'
        at SchemaPageSchemaTab

        when: 'click on the partitioning'
        page.partitioning.click()
        then: 'check the ascending class'
        if ( page.checkAscending() ){

        }
        else{
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on Partitioning FAILED")
        }

        when: 'click on the partitioning'
        page.partitioning.click()
        then: 'check the descending class'
        if ( page.checkDescending() )
            println("VMC-108:Auto Test Code for ascending and descending on Partitioning PASSED")
        else {
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on Partitioning FAILED")
        }
    }

    def "check ascending and descending on Columns"() {
        when: 'click schema sub tab'
        page.gotoSchemaSubTab()
        then: 'at schema sub tab'
        at SchemaPageSchemaTab

        when: 'click on the columns'
        page.columns.click()
        then: 'check the ascending class'
        if ( page.checkAscending() ){

        }
        else{
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on Columns FAILED")
        }

        when: 'click on the columns'
        page.columns.click()
        then: 'check the descending class'
        if ( page.checkDescending() )
            println("VMC-108:Auto Test Code for ascending and descending on Columns PASSED")
        else {
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on Columns FAILED")
        }
    }

    def "check ascending and descending on Indexes"() {
        when: 'click schema sub tab'
        page.gotoSchemaSubTab()
        then: 'at schema sub tab'
        at SchemaPageSchemaTab

        when: 'click on the indexes'
        page.indexes.click()
        then: 'check the ascending class'
        if ( page.checkAscending() ){

        }
        else{
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on Indexes FAILED")
        }

        when: 'click on the indexes'
        page.indexes.click()
        then: 'check the descending class'
        if ( page.checkDescending() )
            println("VMC-108:Auto Test Code for ascending and descending on Indexes PASSED")
        else {
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on Indexes FAILED")
        }
    }

    def "check ascending and descending on PKey"() {
        when: 'click schema sub tab'
        page.gotoSchemaSubTab()
        then: 'at schema sub tab'
        at SchemaPageSchemaTab

        when: 'click on the pkey'
        page.pkey.click()
        then: 'check the ascending class'
        if ( page.checkAscending() ){

        }
        else{
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on PKey FAILED")
        }

        when: 'click on the pkey'
        page.pkey.click()
        then: 'check the descending class'
        if ( page.checkDescending() )
            println("VMC-108:Auto Test Code for ascending and descending on PKey PASSED")
        else {
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on PKey FAILED")
        }
    }

    def "check ascending and descending on TupleLimit"() {
        when: 'click schema sub tab'
        page.gotoSchemaSubTab()
        then: 'at schema sub tab'
        at SchemaPageSchemaTab

        when: 'click on the tuple limit'
        page.tuplelimit.click()
        then: 'check the ascending class'
        if ( page.checkAscending() ){

        }
        else{
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on TupleLimit FAILED")
        }

        when: 'click on the tuple limit'
        page.tuplelimit.click()
        then: 'check the descending class'
        if ( page.checkDescending() )
            println("VMC-108:Auto Test Code for ascending and descending on TupleLimit PASSED")
        else {
            assert false
            println("VMC-108:Auto Test Code for ascending and descending on TupleLimit FAILED")
        }
    }
}
