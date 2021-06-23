package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;

import static org.janusgraph.diskstorage.StaticBuffer.ARRAY_FACTORY;

public class FDBRangeQuery {

    private final KVQuery originalQuery;
    private final KeySelector startKeySelector;
    private final KeySelector endKeySelector;
    private final int limit;

    final StaticBuffer.Factory<byte[]> ARRAY_ALT_FACTORY = (array, offset, limit) -> {
        final byte[] copyArray = new byte[limit - offset];
        System.arraycopy(array, offset, copyArray, 0, limit - offset);
        return copyArray;
    };

    public FDBRangeQuery(Subspace database, KVQuery kvQuery) {
        originalQuery = kvQuery;
        limit = kvQuery.getLimit();

        final StaticBuffer keyStart = kvQuery.getStart();
        final StaticBuffer keyEnd = kvQuery.getEnd();
        byte[] startKey = (keyStart == null) ? database.range().begin : database.pack(keyStart.as(ARRAY_FACTORY));
        byte[] endKey = (keyEnd == null) ? database.range().end : database.pack(keyEnd.as(ARRAY_FACTORY));
        startKeySelector = KeySelector.firstGreaterOrEqual(startKey);
        endKeySelector = KeySelector.firstGreaterOrEqual(endKey);
    }

    public KVQuery getOriginalQuery() {
        return originalQuery;
    }

    public KeySelector getStartKeySelector() {
        return startKeySelector;
    }

    public KeySelector getEndKeySelector() {
        return endKeySelector;
    }

    public int getLimit() {
        return limit;
    }
}
