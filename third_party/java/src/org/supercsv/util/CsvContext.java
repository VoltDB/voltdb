/*
 * Copyright 2007 Kasper B. Graversen
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.supercsv.util;

import java.io.Serializable;
import java.util.List;

/**
 * This object represents the current context of a given CSV file being either read or written to. The lineNumber is the
 * actual line number (beginning at 1) of the file being read or written to. The rowNumber (beginning at 1) is the
 * number of the CSV row (which will be identical to lineNumber if no rows span multiple lines) - the last rowNumber
 * will correspond with the number of CSV records. The columnNumber (beginning at 1) is the number of the CSV column.
 * 
 * @author Kasper B. Graversen
 * @author James Bassett
 */
public class CsvContext implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	/** the line number of the file being read/written */
	private int lineNumber;
	
	/** the CSV row number (CSV rows can span multiple lines) */
	private int rowNumber;
	
	/** the CSV column number */
	private int columnNumber;
	
	/** the row just read in, or to be written */
	private List<Object> rowSource;
	
	/**
	 * Constructs a new <tt>CsvContext</tt>.
	 * 
	 * @param lineNumber
	 *            the current line number
	 * @param rowNumber
	 *            the current CSV row number
	 * @param columnNumber
	 *            the current CSV column number
	 */
	public CsvContext(final int lineNumber, final int rowNumber, final int columnNumber) {
		this.lineNumber = lineNumber;
		this.rowNumber = rowNumber;
		this.columnNumber = columnNumber;
	}
	
	/**
	 * @return the lineNumber
	 */
	public int getLineNumber() {
		return lineNumber;
	}
	
	/**
	 * @param lineNumber
	 *            the lineNumber to set
	 */
	public void setLineNumber(int lineNumber) {
		this.lineNumber = lineNumber;
	}
	
	/**
	 * @return the rowNumber
	 */
	public int getRowNumber() {
		return rowNumber;
	}
	
	/**
	 * @param rowNumber
	 *            the rowNumber to set
	 */
	public void setRowNumber(int rowNumber) {
		this.rowNumber = rowNumber;
	}
	
	/**
	 * @return the columnNumber
	 */
	public int getColumnNumber() {
		return columnNumber;
	}
	
	/**
	 * @param columnNumber
	 *            the columnNumber to set
	 */
	public void setColumnNumber(int columnNumber) {
		this.columnNumber = columnNumber;
	}
	
	/**
	 * @return the rowSource
	 */
	public List<Object> getRowSource() {
		return rowSource;
	}
	
	/**
	 * @param rowSource
	 *            the rowSource to set
	 */
	public void setRowSource(List<Object> rowSource) {
		this.rowSource = rowSource;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return String.format("{lineNo=%d, rowNo=%d, columnNo=%d, rowSource=%s}", lineNumber, rowNumber, columnNumber,
			rowSource);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + columnNumber;
		result = prime * result + rowNumber;
		result = prime * result + lineNumber;
		result = prime * result + ((rowSource == null) ? 0 : rowSource.hashCode());
		return result;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {
		if( this == obj ) {
			return true;
		}
		if( obj == null ) {
			return false;
		}
		if( getClass() != obj.getClass() ) {
			return false;
		}
		final CsvContext other = (CsvContext) obj;
		if( columnNumber != other.columnNumber ) {
			return false;
		}
		if( rowNumber != other.rowNumber ) {
			return false;
		}
		if( lineNumber != other.lineNumber ) {
			return false;
		}
		if( rowSource == null ) {
			if( other.rowSource != null ) {
				return false;
			}
		} else if( !rowSource.equals(other.rowSource) ) {
			return false;
		}
		return true;
	}
	
}
