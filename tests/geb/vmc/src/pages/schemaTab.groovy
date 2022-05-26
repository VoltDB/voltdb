/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package vmcTest.pages

import geb.Module

import java.util.List;
import java.util.Map;

import geb.*
import geb.navigator.Navigator
import geb.waiting.WaitTimeoutException
import org.openqa.selenium.JavascriptExecutor


/**
 * Created by lavthaiba on 2/24/2015.
 */

class schemaTab extends Module{

    static content = {

        // system overview
        schemaTabbutton { $("#navSchema > a") }
        systemoverviewTitle { $("#o > div:nth-child(1) > div > h4") }
        modeTitle { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(1) > td:nth-child(1)") }
        voltdbversion { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(2) > td:nth-child(1)") }
        buildstring { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(3) > td:nth-child(1)") }
        clustercomposition { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(4) > td:nth-child(1)") }
        runningsince { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(5) > td:nth-child(1)") }
        modevalue { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(1) > td:nth-child(2)") }
        versionvalue { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(2) > td:nth-child(2)") }
        buildstringvalue { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(3) > td:nth-child(2)") }
        clustercompositionvalue { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(4) > td:nth-child(2)") }
        runningsincevalue { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(5) > td:nth-child(2)") }

        // catalog overview

        catalogoverviewstatistic { $("#o > div:nth-child(2) > div.dataBlockHeading > h3") }
        compiledversion {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(1) > td:nth-child(1)")
        }
        compiledonTitle {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(2) > td:nth-child(1)")
        }
        tablecount {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(3) > td:nth-child(1)")
        }
        materializedviewcount {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(4) > td:nth-child(1)")
        }
        indexcount {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(5) > td:nth-child(1)")
        }
        procedurecount {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(6) > td:nth-child(1)")
        }
        sqlstatementcount {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(7) > td:nth-child(1)")
        }
        compiledversionvalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(1) > td:nth-child(2)")
        }
        compiledonTitlevalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(2) > td:nth-child(2)")
        }
        tablecountvalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(3) > td:nth-child(2)")
        }
        materializedviewcountvalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(4) > td:nth-child(2)")
        }
        indexcountvalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(5) > td:nth-child(2)")
        }
        procedurecountvalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(6) > td:nth-child(2)")
        }
        sqlstatementcountvalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(7) > td:nth-child(2)")
        }

        //  documentation
        documentationurl { $("#iconDoc") }
        documentationrightlabel { $("#catalogContainer > div.documentation > span") }

        //refreshbutton
        refreshbutton { $("#MenuCatalog > div > button") }

        //DDL source page
        ddlsourcebutton { $("#d-nav > a") }
        ddlsourceTitle {$("#d > div > div.dataBlockHeading > h1")}
        ddlsourcequeries{$("#d > div > div.dataBlockContent > pre")}
        ddlsourcedownload{$("#downloadDDL")}

        //schema sub tab page
        schemasubbutton{$("#s-nav > a")}
        schemasubTitle{$("#s > div > div.dataBlockHeading > h1")}
        expandallcheck{$("#s > div > div.dataBlockContent > div > div > label > input")}
        namesort{$("#schematable > thead > tr.tablesorter-headerRow > th:nth-child(1) > div > div")}
        clickvote{$("#s-votes")}
        clickvotebyno{$("#s-v_votes_by_phone_number")}
        clickvotebycontestantno{$("#s-v_votes_by_contestant_number_state")}
        clickcontestant{$("#s-contestants")}
        clickareacodestate{$("#s-area_code_state")}
        clickautogenconstraint{$("#s-area_code_state-voltdb_autogen_constraint_idx_pk_area_code_state")}
        clickvoteinautogen{$("#s-area_code_state-voltdb_autogen_constraint_idx_pk_area_code_state--dropdown > p > a")}
        clicksubareacode{$("#s-area_code_state-voltdb_autogen_constraint_idx_pk_area_code_state")}
        backtoautogen{$("#p-vote--dropdown > p:nth-child(4) > a:nth-child(1)")}
        typesort{$("#schematable > thead > tr.tablesorter-headerRow > th:nth-child(2) > div > div")}
        partitioningsort{$("#schematable > thead > tr.tablesorter-headerRow > th:nth-child(3) > div > div")}
        columnsort{$("#schematable > thead > tr.tablesorter-headerRow > th:nth-child(4) > div > div")}
        indexsort{$("#schematable > thead > tr.tablesorter-headerRow > th:nth-child(5) > div > div")}
        pkeysort{$("#schematable > thead > tr.tablesorter-headerRow > th:nth-child(6) > div > div")}
        tuplelimitsort{$("#schematable > thead > tr.tablesorter-headerRow > th:nth-child(7) > div > div")}
        viewddlsourcebutton{$("#ddlSource")}


        //schema sub page for input
        nameinput{$("#schematable > thead > tr.tablesorter-filter-row.hideme > td:nth-child(1) > input")}
        typeinput{$("#schematable > thead > tr.tablesorter-filter-row > td:nth-child(2) > input")}
        partitioninput{$("#schematable > thead > tr.tablesorter-filter-row > td:nth-child(3) > input")}
        columninput{$("#schematable > thead > tr.tablesorter-filter-row > td:nth-child(4) > input")}
        indexinput{$("#schematable > thead > tr.tablesorter-filter-row > td:nth-child(5) > input")}
        pkeyinput{$("#schematable > thead > tr.tablesorter-filter-row > td:nth-child(6) > input")}
        tupleinput{$("#schematable > thead > tr.tablesorter-filter-row.hideme > td:nth-child(7) > input")}


        //for writing query
        clicksqlquery{$("#navSqlQuery > a")}

        //Procedure & SQL
        clickproceduresqlbtn    {$("#p-nav > a")}
        proceduresqlTitle       {$("#p > div > div.dataBlockHeading > h1")}
        expandall_check         {$("#p > div > div.dataBlockContent > div > div > label > input")}
        procedurenamesort       {$("#proctable > thead > tr.tablesorter-headerRow > th:nth-child(1) > div > div")}
        parameterssort          {$("#proctable > thead > tr.tablesorter-headerRow > th:nth-child(2) > div > div")}
        partitionsort           {$("#proctable > thead > tr.tablesorter-headerRow > th:nth-child(3) > div > div")}
        rwsort                  {$("#proctable > thead > tr.tablesorter-headerRow > th:nth-child(4) > div > div")}
        accesssort              {$("#proctable > thead > tr.tablesorter-headerRow > th:nth-child(5) > div > div")}
        attributessort          {$("#proctable > thead > tr.tablesorter-headerRow > th:nth-child(6) > div > div")}

        //Size worksheet

        clicksizeworksheetbtn   {$("#z-nav > a")}
        memoryusedbyTitle       {$("#z > div:nth-child(1) > div.dataBlockHeading > h1")}
        checkparagraph          {$("#zTbl > tbody > tr > td:nth-child(1)")}
        sizewithoutunittxt      {$("#zTbl > tbody > tr > td:nth-child(2) > small > ul > li:nth-child(1)")}
        usefulresultstxt        {$("#zTbl > tbody > tr > td:nth-child(2) > small > ul > li:nth-child(2)")}
        calculatedfieldstxt     {$("#zTbl > tbody > tr > td:nth-child(2) > small > ul > li:nth-child(3)")}
        warningpagetxt          {$("#zTbl > tbody > tr > td:nth-child(2) > small > ul > li:nth-child(4)")}
        varcharcolumntxt        {$("#zTbl > tbody > tr > td:nth-child(2) > small > ul > li:nth-child(5)")}

        //size analysis part of schema
        sizeanalysissummaryTitle{$("#z > div:nth-child(2) > div.dataBlockHeading > h3")}
        tablestxt               {$("#z > div:nth-child(2) > div.dataBlockContent > div > table > tbody > tr:nth-child(1) > td:nth-child(2)")}
        materializedtxt         {$("#z > div:nth-child(2) > div.dataBlockContent > div > table > tbody > tr:nth-child(2) > td:nth-child(2)")}
        indexestxt              {$("#z > div:nth-child(2) > div.dataBlockContent > div > table > tbody > tr:nth-child(3) > td:nth-child(2)")}
        totalusertxt            {$("#z > div:nth-child(2) > div.dataBlockContent > div > table > tbody > tr:nth-child(5) > td:nth-child(1) > b")}
        tablestxtvalue1         {$("#s-size-summary-table-min")}
        tabletxtvalue2          {$("#s-size-summary-table-max")}
        materializedtxtvalue    {$("#s-size-summary-view-min")}
        indexestxtvalue         {$("#s-size-summary-index-min")}
        totalusertxtvalue1      {$("#s-size-summary-total-min")}
        totalusertxtvalue2      {$("#s-size-summary-total-max")}

        //sorting in sizw worksheet
        namesortinsizeworksheet{$("#sizetable > thead > tr.tablesorter-headerRow > th:nth-child(1) > div > div")}
        typesortinsizeworksheet{$("#sizetable > thead > tr.tablesorter-headerRow > th:nth-child(2) > div > div")}
        countsortinsizeworksheet{$("#sizetable > thead > tr.tablesorter-headerRow > th:nth-child(3) > div > div")}
        rowminsortinsizeworksheet{$("#sizetable > thead > tr.tablesorter-headerRow > th:nth-child(4) > div > div")}
        rowmaxsortinsizeworksheet{$("#sizetable > thead > tr.tablesorter-headerRow > th:nth-child(5) > div > div")}
        indexminsortinsizeworksheet{$("#sizetable > thead > tr.tablesorter-headerRow > th:nth-child(6) > div > div")}
        indexmaxsortinsizeworksheet{$("#sizetable > thead > tr.tablesorter-headerRow > th:nth-child(7) > div > div")}
        tableminsortinsizeworksheet{$("#sizetable > thead > tr.tablesorter-headerRow > th:nth-child(8) > div > div")}
        tablemaxsortinsizeworksheet{$("#sizetable > thead > tr.tablesorter-headerRow > th:nth-child(9) > div > div")}


    }
}
