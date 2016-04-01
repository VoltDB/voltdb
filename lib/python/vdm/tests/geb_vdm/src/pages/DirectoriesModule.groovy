/*
This file is part of VoltDB.

Copyright (C) 2008-2016 VoltDB Inc.

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

class DirectoriesModule extends Module {
    static content = {
        rootDestinationText         { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigRight > div.main > div.adminDirect > table > tbody > tr:nth-child(1) > td:nth-child(1)") }
        snapshotText                { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigRight > div.main > div.adminDirect > table > tbody > tr:nth-child(2) > td:nth-child(1)") }
        exportOverflowText          { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigRight > div.main > div.adminDirect > table > tbody > tr:nth-child(3) > td:nth-child(1)") }
        commandLogText              { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigRight > div.main > div.adminDirect > table > tbody > tr:nth-child(4) > td:nth-child(1)") }
        commandLogSnapshotsText     { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigRight > div.main > div.adminDirect > table > tbody > tr:nth-child(5) > td:nth-child(1)") }

        rootDestinationField        { $(id:"txtVoltdbRootDir") }
        snapshotField               { $(id:"txtSnapshotDir") }
        exportOverflowField         { $(id:"txtExportOverflowDir") }
        commandLogField             { $(id:"txtCommandLogDir") }
        commandLogSnapshotsField    { $(id:"txtCommandLogSnapDir") }
    }
}
