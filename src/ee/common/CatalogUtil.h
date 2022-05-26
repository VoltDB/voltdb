/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#ifndef CATALOGUTIL_H
#define CATALOGUTIL_H

#include "catalog/database.h"
#include "catalog/table.h"
#include "common/types.h"

/*
  Global helper functions for extracting more complex
  information from the catalog.
*/

bool isTableMaterialized(const catalog::Table &table) {
    return table.materializer() != NULL;
}

#endif
