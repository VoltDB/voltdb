/*
This file is part of VoltDB.

Copyright (C) 2008-2015 VoltDB Inc.

This file contains original code and/or modifications of original code.
Any modifications made by VoltDB Inc. are licensed under the following
terms and conditions:

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
*/


import geb.Module

class DatabaseReplicationModule extends Module {
    static content = {
        editButton          { $("#btnUpdateSource") }
        editPopupSave       { $("#btnSaveReplication") }
        delete              { $("#btnDeleteConnection") }
        addConnectionSource { $("#addConnectionSource") }

        enabledCheckbox     { $("#frmDatabaseReplication > div > table > tbody > tr:nth-child(1) > td:nth-child(2) > span:nth-child(1) > div > ins") }

        idField             { $("#txtDrId") }

        // DR Box
        status              { $("#chkDrOnOffVal") }

        databasePort        { $("#txtDrPort") }

        sourceCheckBox      { $("#frmDatabaseReplication > div > table > tbody > tr:nth-child(4) > td:nth-child(2) > span:nth-child(1) > div > ins") }
        sourceText          { $("#chkConnectionSourceOnOffValue") }
        sourceField         { $("#txtDatabase") }

        displayedId         { $("#lblDrId") }
        displayedPort       { $("#lblDrPort") }
        displayedSource     { $("#master-cluster-name") }
    }

}