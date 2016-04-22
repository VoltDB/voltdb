package org.voltdb.importclient.kinesis;

import org.voltdb.importclient.ImportBaseException;

public class KinesisStreamImporterException extends ImportBaseException
{
    private static final long serialVersionUID = 7668280657393399984L;

    public KinesisStreamImporterException() {
    }

    public KinesisStreamImporterException(String format, Object... args) {
        super(format, args);
    }

    public KinesisStreamImporterException(Throwable cause) {
        super(cause);
    }

    public KinesisStreamImporterException(String format, Throwable cause,
            Object... args) {
        super(format, cause, args);
    }
}
