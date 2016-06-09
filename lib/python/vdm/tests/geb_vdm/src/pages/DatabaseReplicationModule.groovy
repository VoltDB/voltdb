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
