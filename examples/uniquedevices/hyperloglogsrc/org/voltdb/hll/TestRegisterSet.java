package org.voltdb.hll;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestRegisterSet {

    @Test
    public void testBits() {
        final int RS_COUNT = 33;
        RegisterSet rs = new RegisterSet(RS_COUNT);
        for (int i = 0; i < RS_COUNT * RegisterSet.REGISTER_SIZE; i++) {
            System.out.printf("%d\n", i);
            System.out.flush();

            rs.setBit(i, i % 2);
            for (int j = 0; j <= i; j++) {
                assertEquals(j % 2, rs.getBit(j));
            }
        }

        for (int i = 0; i < RS_COUNT * RegisterSet.REGISTER_SIZE; i++) {
            System.out.printf("%d\n", i);
            System.out.flush();

            rs.setBit(i, (i % 2) == 0 ? 1 : 0);
            for (int j = 0; j <= i; j++) {
                assertEquals((j % 2) == 0 ? 1 : 0, rs.getBit(j));
            }
        }
    }

    @Test
    public void testBasic() {
        final int RS_COUNT = 128;
        RegisterSet rs = new RegisterSet(RS_COUNT);
        for (int i = 0; i < RS_COUNT; i++) {
            System.out.printf("%d\n", i);
            System.out.flush();

            rs.set(i, i % 32);
            for (int j = 0; j <= i; j++) {
                assertEquals(j % 32, rs.get(j));
            }
        }
    }

    @Test
    public void testUpdateIfGreater() {
        final int RS_COUNT = 128;
        RegisterSet rs = new RegisterSet(RS_COUNT);

        // set all ones
        for (int i = 0; i < RS_COUNT; i++) {
            rs.set(i, 1);
        }

        // update every other to 5s
        for (int i = 0; i < RS_COUNT; i++) {
            if (i % 2 == 0) {
                rs.set(i, 5);
            }
        }

        // update all to 3s if greater
        for (int i = 0; i < RS_COUNT; i++) {
            rs.updateIfGreater(i, 3);
        }

        // verify all 3s and 5s
        for (int i = 0; i < RS_COUNT; i++) {
            if (i % 2 == 0) {
                assertEquals(5, rs.get(i));
            }
            else {
                assertEquals(3, rs.get(i));
            }
        }
    }

}
