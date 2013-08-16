package org.voltdb.licensetool;

/*
 * All the various cryptographic and IO exceptions, and there are
 * many, that bubble up are wrapped in this license exception
 * and allowed to escape the license checker.
 */
public class LicenseException extends Exception {
    private static final long serialVersionUID = 8699544709857898752L;
    LicenseException(Exception ex) {
        super(ex);
    }
    LicenseException(String message) {
        super(message);
    }
}
