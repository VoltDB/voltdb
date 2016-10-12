package org.voltcore.utils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class DeferredSerializationIterator implements Iterator<DeferredSerialization> {

    public static DeferredSerializationIterator EMPTY_DEFERRED_SERIALIZATION_ITERATOR = new DeferredSerializationIterator(Collections.emptyList());
    private Iterator<DeferredSerialization> dsIter;
    private final Serializer serializer;

    public DeferredSerializationIterator(Serializer serializer) {
        this.serializer = serializer;
        this.dsIter = null;
    }

    private DeferredSerializationIterator(List<DeferredSerialization> dsList) {
        this.serializer = null;
        this.dsIter = dsList.iterator();
    }

    @Override
    public boolean hasNext() {
        if (dsIter == null) {
            ByteBuffer buf = serializer.serialize();
            DeferredSerialization ds = new SimpleDeferredSerialization(buf);
            dsIter = Collections.singletonList(ds).iterator();
        }
        return dsIter.hasNext();
    }

    @Override
    public DeferredSerialization next() {
        return dsIter.next();
    }
}
