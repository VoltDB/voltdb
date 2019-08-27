/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

package org.voltdb.task;

import java.util.ArrayList;
import java.util.List;

import com.google_voltpatches.common.base.Joiner;

/**
 * Helper that can be used to collect errors that are encountered while validating the parameters of any
 * {@link Initializable} class.
 */
public final class TaskValidationErrors {
    private List<String> m_errors = null;

    /**
     * Add a new error message to the set of errors being returned from this validation
     *
     * @param error message to add. If {@code null} the message will be ignored
     */
    public void addErrorMessage(String error) {
        if (error == null) {
            return;
        }
        if (m_errors == null) {
            m_errors = new ArrayList<>();
        }
        m_errors.add(error);
    }

    /**
     * @return {@code true} if errors have been added to this instance
     */
    public boolean hasErrors() {
        return m_errors != null;
    }

    /**
     * Creates an error message comprised of all of the error messages added to this instance by calling
     * {@link #addErrorMessage(String)}. If no error messages were added then {@code null} is returned.
     *
     * @return An error message or {@code null} if {@link #hasErrors()} returns {@code false}
     */
    public String getErrorMessage() {
        if (m_errors == null) {
            return null;
        }
        return Joiner.on('\n').join(m_errors);
    }
}
