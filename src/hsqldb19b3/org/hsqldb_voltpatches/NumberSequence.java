/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches;

import java.math.BigDecimal;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.lib.OrderedHashSet;

/**
 * Maintains a sequence of numbers.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version  1.9.0
 * @since 1.7.2
 */
public final class NumberSequence implements SchemaObject {

    public final static NumberSequence[] emptyArray = new NumberSequence[]{};

    //
    HsqlName name;

    // present value
    private long currValue;

    // last value
    private long lastValue;

    // limit state
    private boolean limitReached;

    // original start value - used in CREATE and ALTER commands
    private long    startValue;
    private long    minValue;
    private long    maxValue;
    private long    increment;
    private Type    dataType;
    private boolean isCycle;
    private boolean isAlways;
    private boolean restartValueDefault;

    public NumberSequence() {

        try {
            setDefaults(null, Type.SQL_BIGINT);
        } catch (HsqlException e) {}
    }

    public NumberSequence(HsqlName name, Type type) {
        setDefaults(name, type);
    }

    public void setDefaults(HsqlName name, Type type) {

        this.name = name;
        dataType  = type;
        this.name = name;
        dataType  = type;

        long min;
        long max;

        switch (dataType.typeCode) {

            case Types.SQL_SMALLINT :
                max = Short.MAX_VALUE;
                min = Short.MIN_VALUE;
                break;

            case Types.SQL_INTEGER :
                max = Integer.MAX_VALUE;
                min = Integer.MIN_VALUE;
                break;

            case Types.SQL_BIGINT :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                if (type.scale == 0) {
                    max = Long.MAX_VALUE;
                    min = Long.MIN_VALUE;

                    break;
                }

            // $FALL-THROUGH$
            default :
                throw Error.error(ErrorCode.X_42565);
        }

        minValue  = min;
        maxValue  = max;
        increment = 1;
    }

    /**
     * constructor with initial value and increment;
     */
    public NumberSequence(HsqlName name, long value, long increment,
                          Type type) {

        this(name, type);

        setStartValue(value);
        setIncrement(increment);
    }

    public int getType() {
        return SchemaObject.SEQUENCE;
    }

    public HsqlName getName() {
        return name;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session) {}

    public String getSQL() {

        StringBuffer sb = new StringBuffer(128);

        if (name == null) {
            sb.append(Tokens.T_GENERATED).append(' ');

            if (isAlways()) {
                sb.append(Tokens.T_ALWAYS);
            } else {
                sb.append(Tokens.T_BY).append(' ').append(Tokens.T_DEFAULT);
            }

            sb.append(' ').append(Tokens.T_AS).append(' ').append(
                Tokens.T_IDENTITY).append(Tokens.T_OPENBRACKET);
        } else {
            sb.append(Tokens.T_CREATE).append(' ');
            sb.append(Tokens.T_SEQUENCE).append(' ');
            sb.append(getName().getSchemaQualifiedStatementName()).append(' ');
            sb.append(Tokens.T_AS).append(' ');
            sb.append(getDataType().getNameString()).append(' ');
        }

        //
        sb.append(Tokens.T_START).append(' ');
        sb.append(Tokens.T_WITH).append(' ');
        sb.append(startValue);

        if (getIncrement() != 1) {
            sb.append(' ').append(Tokens.T_INCREMENT).append(' ');
            sb.append(Tokens.T_BY).append(' ');
            sb.append(getIncrement());
        }

        if (!hasDefaultMinMax()) {
            sb.append(' ').append(Tokens.T_MINVALUE).append(' ');
            sb.append(getMinValue());
            sb.append(' ').append(Tokens.T_MAXVALUE).append(' ');
            sb.append(getMaxValue());
        }

        if (isCycle()) {
            sb.append(' ').append(Tokens.T_CYCLE);
        }

        if (name == null) {
            sb.append(Tokens.T_CLOSEBRACKET);
        }

        return sb.toString();
    }

    public String getRestartSQL() {

        StringBuffer sb = new StringBuffer(128);

        sb.append(Tokens.T_ALTER).append(' ');
        sb.append(Tokens.T_SEQUENCE);
        sb.append(' ').append(name.getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_RESTART);
        sb.append(' ').append(Tokens.T_WITH).append(' ').append(peek());

        return sb.toString();
    }

    public static String getRestartSQL(Table t) {

        String colname = t.getColumn(t.identityColumn).getName().statementName;
        NumberSequence seq = t.identitySequence;
        StringBuffer   sb  = new StringBuffer(128);

        sb.append(Tokens.T_ALTER).append(' ').append(Tokens.T_TABLE);
        sb.append(' ').append(t.getName().getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_ALTER).append(' ');
        sb.append(Tokens.T_COLUMN);
        sb.append(' ').append(colname);
        sb.append(' ').append(Tokens.T_RESTART);
        sb.append(' ').append(Tokens.T_WITH).append(' ').append(seq.peek());

        return sb.toString();
    }

    public Type getDataType() {
        return dataType;
    }

    public long getIncrement() {
        return increment;
    }

    public synchronized long getStartValue() {
        return startValue;
    }

    public synchronized long getMinValue() {
        return minValue;
    }

    public synchronized long getMaxValue() {
        return maxValue;
    }

    public synchronized boolean isCycle() {
        return isCycle;
    }

    public synchronized boolean isAlways() {
        return isAlways;
    }

    public synchronized boolean hasDefaultMinMax() {

        long min;
        long max;

        switch (dataType.typeCode) {

            case Types.SQL_SMALLINT :
                max = Short.MAX_VALUE;
                min = Short.MIN_VALUE;
                break;

            case Types.SQL_INTEGER :
                max = Integer.MAX_VALUE;
                min = Integer.MIN_VALUE;
                break;

            case Types.SQL_BIGINT :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberSequence");
        }

        return minValue == min && maxValue == max;
    }

    synchronized void setStartValue(long value) {

        if (value < minValue || value > maxValue) {
            throw Error.error(ErrorCode.X_42597);
        }

        startValue = value;
        currValue  = lastValue = startValue;
    }

    synchronized void setMinValue(long value) {

        checkInTypeRange(value);

        if (value >= maxValue || currValue < value) {
            throw Error.error(ErrorCode.X_42597);
        }

        minValue = value;
    }

    synchronized void setDefaultMinValue() {
        minValue = getDefaultMinOrMax(false);
    }

    synchronized void setMaxValue(long value) {

        checkInTypeRange(value);

        if (value <= minValue || currValue > value) {
            throw Error.error(ErrorCode.X_42597);
        }

        maxValue = value;
    }

    synchronized void setDefaultMaxValue() {
        maxValue = getDefaultMinOrMax(true);
    }

    synchronized void setIncrement(long value) {

        if (value < Short.MIN_VALUE / 2 || value > Short.MAX_VALUE / 2) {
            throw Error.error(ErrorCode.X_42597);
        }

        increment = value;
    }

    synchronized void setCurrentValueNoCheck(long value) {

        checkInTypeRange(value);

        currValue = lastValue = value;
    }

    synchronized void setStartValueNoCheck(long value) {

        checkInTypeRange(value);

        startValue = value;
        currValue  = lastValue = startValue;
    }

    synchronized void setStartValueDefault() {
        restartValueDefault = true;
    }

    synchronized void setMinValueNoCheck(long value) {

        checkInTypeRange(value);

        minValue = value;
    }

    synchronized void setMaxValueNoCheck(long value) {

        checkInTypeRange(value);

        maxValue = value;
    }

    synchronized void setCycle(boolean value) {
        isCycle = value;
    }

    synchronized void setAlways(boolean value) {
        isAlways = value;
    }

    private long getDefaultMinOrMax(boolean isMax) {

        long min;
        long max;

        switch (dataType.typeCode) {

            case Types.SQL_SMALLINT :
                max = Short.MAX_VALUE;
                min = Short.MIN_VALUE;
                break;

            case Types.SQL_INTEGER :
                max = Integer.MAX_VALUE;
                min = Integer.MIN_VALUE;
                break;

            case Types.SQL_BIGINT :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberSequence");
        }

        return isMax ? max
                     : min;
    }

    private void checkInTypeRange(long value) {

        long min;
        long max;

        switch (dataType.typeCode) {

            case Types.SQL_SMALLINT :
                max = Short.MAX_VALUE;
                min = Short.MIN_VALUE;
                break;

            case Types.SQL_INTEGER :
                max = Integer.MAX_VALUE;
                min = Integer.MIN_VALUE;
                break;

            case Types.SQL_BIGINT :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberSequence");
        }

        if (value < min || value > max) {
            throw Error.error(ErrorCode.X_42597);
        }
    }

    synchronized void checkValues() {

        if (restartValueDefault) {
            currValue           = lastValue = startValue;
            restartValueDefault = false;
        }

        if (minValue >= maxValue || startValue < minValue
                || startValue > maxValue || currValue < minValue
                || currValue > maxValue) {
            throw Error.error(ErrorCode.X_42597);
        }
    }

    synchronized NumberSequence duplicate() {

        NumberSequence copy = new NumberSequence();

        copy.name       = name;
        copy.startValue = startValue;
        copy.currValue  = currValue;
        copy.lastValue  = lastValue;
        copy.increment  = increment;
        copy.dataType   = dataType;
        copy.minValue   = minValue;
        copy.maxValue   = maxValue;
        copy.isCycle    = isCycle;
        copy.isAlways   = isAlways;

        return copy;
    }

    synchronized void reset(NumberSequence other) {

        name       = other.name;
        startValue = other.startValue;
        currValue  = other.currValue;
        lastValue  = other.lastValue;
        increment  = other.increment;
        dataType   = other.dataType;
        minValue   = other.minValue;
        maxValue   = other.maxValue;
        isCycle    = other.isCycle;
        isAlways   = other.isAlways;
    }

    /**
     * getter for a given value
     */
    synchronized long userUpdate(long value) {

        if (value == currValue) {
            currValue += increment;

            return value;
        }

        if (increment > 0) {
            if (value > currValue) {
                currValue += ((value - currValue + increment) / increment)
                             * increment;
            }
        } else {
            if (value < currValue) {
                currValue += ((value - currValue + increment) / increment)
                             * increment;
            }
        }

        return value;
    }

    /**
     * Updates are necessary for text tables
     * For memory tables, the logged and scripted RESTART WITH will override
     * this.
     * No checks as values may have overridden the sequnece defaults
     */
    synchronized long systemUpdate(long value) {

        if (value == currValue) {
            currValue += increment;

            return value;
        }

        if (increment > 0) {
            if (value > currValue) {
                currValue = value + increment;
            }
        } else {
            if (value < currValue) {
                currValue = value + increment;
            }
        }

        return value;
    }

    synchronized Object getValueObject() {

        long   value = getValue();
        Object result;

        switch (dataType.typeCode) {

            default :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
                result = ValuePool.getInt((int) value);
                break;

            case Types.SQL_BIGINT :
                result = ValuePool.getLong(value);
                break;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                result = ValuePool.getBigDecimal(new BigDecimal(value));
                break;
        }

        return result;
    }

    /**
     * principal getter for the next sequence value
     */
    synchronized public long getValue() {

        if (limitReached) {
            throw Error.error(ErrorCode.X_2200H);
        }

        long nextValue;

        if (increment > 0) {
            if (currValue > maxValue - increment) {
                if (isCycle) {
                    nextValue = minValue;
                } else {
                    limitReached = true;
                    nextValue    = minValue;
                }
            } else {
                nextValue = currValue + increment;
            }
        } else {
            if (currValue < minValue - increment) {
                if (isCycle) {
                    nextValue = maxValue;
                } else {
                    limitReached = true;
                    nextValue    = minValue;
                }
            } else {
                nextValue = currValue + increment;
            }
        }

        long result = currValue;

        currValue = nextValue;

        return result;
    }

    /**
     * reset to start value
     */
    synchronized void reset() {

        // no change if called before getValue() or called twice
        lastValue = currValue = startValue;
    }

    /**
     * get next value without incrementing
     */
    synchronized public long peek() {
        return currValue;
    }

    /**
     * reset the wasUsed flag
     */
    synchronized boolean resetWasUsed() {

        boolean result = lastValue != currValue;

        lastValue = currValue;

        return result;
    }

    /**
     * reset to new initial value
     */
    synchronized public void reset(long value) {

        if (value < minValue || value > maxValue) {
            throw Error.error(ErrorCode.X_42597);
        }

        startValue = currValue = lastValue = value;
    }
}
