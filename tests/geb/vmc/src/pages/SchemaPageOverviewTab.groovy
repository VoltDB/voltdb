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

/**
 * This class represents the 'Overview' tab of the 'Schema' page (or tab) of
 * the VoltDB Management Center (page), which is the VoltDB web UI (replacing
 * the old Catalog Report).
 */
class SchemaPageOverviewTab extends SchemaPage {
    static content = {
        systemOverview              { $("h4", text:"System Overview") }
        mode                        { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(1) > td:nth-child(1)") }
        voltDBVersion               { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(2) > td:nth-child(1)") }
        buildstring                 { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(3) > td:nth-child(1)") }
        clusterComposition          { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(4) > td:nth-child(1)") }
        runningSince                { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(5) > td:nth-child(1)") }

        modeValue                   { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(1) > td:nth-child(2)") }
        voltDBVersionValue          { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(2) > td:nth-child(2)") }
        buildstringValue            { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(3) > td:nth-child(2)") }
        clusterCompositionValue     { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(4) > td:nth-child(2)") }
        runningSinceValue           { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(5) > td:nth-child(2)") }

        schemaOverview              { $("#o > div > div.dataBlockHeading > h3") }
        voltDbVersion               { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(1) > td:nth-child(1)") }
        lastSchemaUpdate            { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(2) > td:nth-child(1)") }
        tableCount                  { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(3) > td:nth-child(1)") }
        materializedViewCount       { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(4) > td:nth-child(1)") }
        indexCount                  { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(5) > td:nth-child(1)") }
        procedureCount              { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(6) > td:nth-child(1)") }
        sqlStatementCount           { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(7) > td:nth-child(1)") }

        voltDBVersionValue          { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(1) > td:nth-child(2)") }
        lastSchemaUpdateValue       { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(2) > td:nth-child(2)") }
        tableCountValue             { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(3) > td:nth-child(2)") }
        materializedViewCountValue  { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(4) > td:nth-child(2)") }
        indexCountValue             { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(5) > td:nth-child(2)") }
        procedureCountValue         { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(6) > td:nth-child(2)") }
        sqlStatementCountValue      { $("#o > div > div.dataBlockContent > table > tbody > tr:nth-child(7) > td:nth-child(2)") }
        documentationLink           { $("#iconDoc") }
        generatedbytxt              {$("#catalogContainer > div.documentation > span")}
    }
    static at = {
        overviewTab.displayed
        overviewTab.attr('class') == 'active'
    }

    def boolean checkSystemOverview() {
        systemOverview.isDisplayed()
    }

    def boolean checkSchemaOverview() {
        schemaOverview.isDisplayed()
    }

    def boolean checkMode() {
        mode.isDisplayed()
    }

    def boolean checkVoltDBVersion() {
        voltDBVersion.isDisplayed()
    }

    def boolean checkBuildstring() {
        buildstring.isDisplayed()
    }

    def boolean checkClusterComposition() {
        clusterComposition.isDisplayed()
    }

    def boolean checkRunningSince() {
        runningSince.isDisplayed()
    }

    def boolean checkVoltDbVersion() {
        voltDbVersion.isDisplayed()
    }

    def boolean checkLastSchemaUpdate() {
        lastSchemaUpdate.isDisplayed()
    }

    def boolean checkTableCount() {
        tableCount.isDisplayed()
    }

    def boolean checkMaterializedViewCount() {
        materializedViewCount.isDisplayed()
    }

    def boolean checkIndexCount() {
        indexCount.isDisplayed()
    }

    def boolean checkProcedureCount() {
        procedureCount.isDisplayed()
    }

    def boolean checkSqlStatementCount() {
        sqlStatementCount.isDisplayed()
    }
}
