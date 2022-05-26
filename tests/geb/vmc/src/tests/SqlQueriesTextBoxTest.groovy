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

package vmcTest.tests

import geb.*
import groovy.json.*
import java.util.List;
import java.util.Map;
import spock.lang.*
import vmcTest.pages.*

/**
 * This class contains tests of the 'SQL Query' tab of the VoltDB Management
 * Center (VMC) page, which is the VoltDB (new) web UI.
 * The tests are related to the text box of the Sql queries.
 */
class SqlQueriesTextBoxTest extends TestBase {
    def setup() {
        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        when:
        String createQuery = page.getCreateQueryForSqlQueriesTextBoxTest();
        and:
        page.setQueryText(createQuery);
        then:
        page.runQuery();
        then:
        !page.queryErrHtml.isDisplayed();
        then:
        page.refreshquery.click();
    }

    def checkValidQuery() {
        when:
        String insertQuery = page.getInsertQueryForSqlQueriesTextBoxTest();
        and:
        page.setQueryText(insertQuery);
        then:
        page.runQuery();
        then:
        !page.queryErrHtml.isDisplayed();
        then:
        page.refreshquery.click();

        when:
        String selectQuery = page.getSelectQueryForSqlQueriesTextBoxTest();
        and:
        page.setQueryText(selectQuery);
        then:
        page.runQuery();
        then:
        !page.queryErrHtml.isDisplayed();
        then:
        page.refreshquery.click();
        report "nello";
    }

    def checkInsertQueryWithSpecialCase() {
        when:
        String insertQuery = page.getInsertQueryWithSpecialCharactersForSqlQueriesTextBoxTest();
        and:
        page.setQueryText(insertQuery);
        then:
        page.runQuery();
        then:
        !page.queryErrHtml.isDisplayed();
        then:
        page.refreshquery.click();

        when:
        String selectQuery = page.getSelectQueryForSqlQueriesTextBoxTest();
        and:
        page.setQueryText(selectQuery);
        then:
        page.runQuery();
        then:
        !page.queryErrHtml.isDisplayed();
        then:
        page.refreshquery.click();
        report "nello";
    }

    def checkUpdateQueryWithSpecialCase() {
        when:
        String insertQuery = page.getInsertQueryForSqlQueriesTextBoxTest();
        and:
        page.setQueryText(insertQuery);
        then:
        page.runQuery();
        then:
        !page.queryErrHtml.isDisplayed();
        then:
        page.refreshquery.click();

        when:
        String updateQueryWithSpecialCharacters = page.getUpdateQueryWithSpecialCharactersForSqlQueriesTextBoxTest();
        and:
        page.setQueryText(updateQueryWithSpecialCharacters);
        then:
        page.runQuery();
        then:
        !page.queryErrHtml.isDisplayed();
        then:
        page.refreshquery.click();

        when:
        String selectQuery = page.getSelectQueryForSqlQueriesTextBoxTest();
        and:
        page.setQueryText(selectQuery);
        then:
        page.runQuery();
        then:
        !page.queryErrHtml.isDisplayed();
        then:
        page.refreshquery.click();
        report "nello";
    }

    def checkInsertQueryWithSpaces() {
        when:
        String insertQueryWithSpaces = page.getInsertQueryWithSpacesForSqlQueriesTextBoxTest();
        and:
//        page.queryInput.jquery.html("<div style='white-space:pre'>INSERT INTO my_test_table VALUES 'asdf     asf';</div>");
        page.queryInput.jquery.html(insertQueryWithSpaces);
        then:
        page.runQuery();
        then:
        !page.queryErrHtml.isDisplayed();
        then:
        page.refreshquery.click();

        when:
        String selectQuery = page.getSelectQueryForSqlQueriesTextBoxTest();
        and:
        page.setQueryText(selectQuery);
        then:
        page.runQuery();
        then:
        !page.queryErrHtml.isDisplayed();
        then:
        page.refreshquery.click();

        when:
        page.queryResultBoxTd.isDisplayed();
        then:
        assert insertQueryWithSpaces.contains(page.queryResultBoxTd.jquery.html());
        report "nello";
    }

    def cleanup() {
        when:
        String dropQuery = page.getDeleteQueryForSqlQueriesTextBoxTest();
        and:
        page.setQueryText(dropQuery);
        then:
        page.runQuery();
        then:
        !page.queryErrHtml.click();
        then:
        page.refreshquery.click();
    }
}
