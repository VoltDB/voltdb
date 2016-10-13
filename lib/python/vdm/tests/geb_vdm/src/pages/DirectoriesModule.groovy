/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

import geb.Module

class DirectoriesModule extends Module {
    static content = {
        rootDestinationText         { $("#divDbManager > div:nth-child(8) > div > div > div > div.wrapOverview > div.mainTblLeft > table > tbody > tr:nth-child(1) > td:nth-child(1)") }
        snapshotText                { $("#divDbManager > div:nth-child(8) > div > div > div > div.wrapOverview > div.mainTblLeft > table > tbody > tr:nth-child(2) > td:nth-child(1)") }
        exportOverflowText          { $("#divDbManager > div:nth-child(8) > div > div > div > div.wrapOverview > div.mainTblLeft > table > tbody > tr:nth-child(3) > td:nth-child(1)") }
        commandLogText              { $("#divDbManager > div:nth-child(8) > div > div > div > div.wrapOverview > div.mainTblRight > table > tbody > tr:nth-child(1) > td:nth-child(1)") }
        commandLogSnapshotsText     { $("#divDbManager > div:nth-child(8) > div > div > div > div.wrapOverview > div.mainTblRight > table > tbody > tr:nth-child(2) > td:nth-child(1)") }

        rootDestinationField        { $(id:"txtVoltdbRootDir") }
        snapshotField               { $(id:"txtSnapshotDir") }
        exportOverflowField         { $(id:"txtExportOverflowDir") }
        commandLogField             { $(id:"txtCommandLogDir") }
        commandLogSnapshotsField    { $(id:"txtCommandLogSnapDir") }

    }
}
