/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package com;

/** Stores the scaling parameters for loading and running. */
public class ScaleParameters {
    public ScaleParameters(int items, int warehouses, int districtsPerWarehouse,
            int customersPerDistrict, int newOrdersPerDistrict) {
        assert 1 <= items && items <= Constants.NUM_ITEMS;
        this.items = items;
        assert warehouses > 0;
        this.warehouses = warehouses;
        assert 1 <= districtsPerWarehouse &&
                districtsPerWarehouse <= Constants.DISTRICTS_PER_WAREHOUSE;
        this.districtsPerWarehouse = districtsPerWarehouse;
        assert 1 <= customersPerDistrict &&
                customersPerDistrict <= Constants.CUSTOMERS_PER_DISTRICT;
        this.customersPerDistrict = customersPerDistrict;
        assert 0 <= newOrdersPerDistrict &&
                newOrdersPerDistrict <= Constants.CUSTOMERS_PER_DISTRICT;
        assert newOrdersPerDistrict <= Constants.INITIAL_NEW_ORDERS_PER_DISTRICT;
        this.newOrdersPerDistrict = newOrdersPerDistrict;
    }

    public static ScaleParameters makeDefault(int warehouses) {
        return new ScaleParameters(Constants.NUM_ITEMS, warehouses,
                Constants.DISTRICTS_PER_WAREHOUSE, Constants.CUSTOMERS_PER_DISTRICT,
                Constants.INITIAL_NEW_ORDERS_PER_DISTRICT);
    }

    public static ScaleParameters makeWithScaleFactor(int warehouses, double scaleFactor) {
        assert scaleFactor >= 1.0;

        int items = (int) (Constants.NUM_ITEMS/scaleFactor);
        if (items <= 0) items = 1;
        int districts = (int) (Constants.DISTRICTS_PER_WAREHOUSE/scaleFactor);
        if (districts <= 0) districts = 1;
        int customers = (int) (Constants.CUSTOMERS_PER_DISTRICT/scaleFactor);
        if (customers <= 0) customers = 1;
        int newOrders = (int) (Constants.INITIAL_NEW_ORDERS_PER_DISTRICT/scaleFactor);
        if (newOrders < 0) newOrders = 0;

        return new ScaleParameters(items, warehouses, districts, customers, newOrders);
    }

    public String toString() {
        String out = items + " items\n";
        out += warehouses + " warehouses\n";
        out += districtsPerWarehouse + " districts/warehouse\n";
        out += customersPerDistrict + " customers/district\n";
        out += newOrdersPerDistrict + " initial new orders/district";
        return out;
    }

    public final int items;
    public final int warehouses;
    public final int districtsPerWarehouse;
    public final int customersPerDistrict;
    public final int newOrdersPerDistrict;
}
