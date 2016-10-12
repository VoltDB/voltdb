package org.voltcore.utils;

import java.nio.ByteBuffer;

public interface Serializer {
    ByteBuffer serialize();
}
