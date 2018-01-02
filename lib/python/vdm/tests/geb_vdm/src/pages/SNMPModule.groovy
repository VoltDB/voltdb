/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

class SNMPModule extends Module {
    static content = {
        btnUpdateSNMP               { $(id: "btnUpdateSNMP") }
        btnSaveSNMP                 { $(id: "btnSaveSNMP") }
        btnDeleteSNMP               { $(id: "btnDeleteSNMP") }
        chkSNMP                     { $(id: "chkSNMP") }
        txtTarget                   { $(id: "txtTarget") }
        txtCommunity                { $(id: "txtCommunity") }
        txtUsername                 { $(id: "txtUsername") }
        selectAuthProtocol          { $(id: "selectAuthProtocol") }
        txtAuthKey                  { $(id: "txtAuthKey") }
        selectPrivacyProtocol       { $(id: "selectPrivacyProtocol") }
        txtPrivacyKey               { $(id: "txtPrivacyKey") }
        tdSNMP                      { $(id: "tdSNMP") }
        chkSNMPDivClass             { tdSNMP.find('div').first() }
        chkSNMPDiv                  { $("#tdSNMP > div > ins") }
        errorTarget                 { $(id: "errorTarget") }
        errorAuthKey                { $(id: "errorAuthKey") }
        errorPrivacyKey             { $(id: "errorPrivacyKey") }
        lblTarget                   { $("#adminTbl1 > tbody > tr:nth-child(15)") }
        snmpTitle                   { $("#row-8 > td.configLabel > a > span") }
        tdTargetVal                 { $(id: "tdTargetVal") }
        tdCommunityVal              { $(id: "tdCommunityVal") }
        tdUsernameVal               { $(id: "tdUsernameVal") }
        tdAuthProtocolVal           { $(id: "tdAuthProtocolVal") }
        tdAuthKeyVal                { $(id: "tdAuthKeyVal") }
        tdPrivacyProtocolVal        { $(id: "tdPrivacyProtocolVal") }
        tdPrivacyKeyVal             { $(id: "tdPrivacyKeyVal") }
    }
}
