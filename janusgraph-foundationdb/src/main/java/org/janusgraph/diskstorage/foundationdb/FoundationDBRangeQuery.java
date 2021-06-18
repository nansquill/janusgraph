package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;

public class FoundationDBRangeQuery {

    private final KVQuery originalQuery;
    private final KeySelector startKeySelector;
    private final KeySelector endKeySelector;
    private final int limit;

    public FoundationDBRangeQuery(Subspace database, KVQuery kvQuery) {
        originalQuery = kvQuery;
        limit = kvQuery.getLimit();

        final StaticBuffer keyStart = kvQuery.getStart();
        final StaticBuffer keyEnd = kvQuery.getEnd();
        byte[] startKey = (keyStart == null) ? database.range().begin : database.pack(keyStart.as(FoundationDBKeyValueStore.ENTRY_FACTORY));
        byte[] endKey = (keyEnd == null) ? database.range().end : database.pack(keyEnd.as(FoundationDBKeyValueStore.ENTRY_FACTORY));
        startKeySelector = KeySelector.firstGreaterOrEqual(startKey);
        endKeySelector = KeySelector.firstGreaterOrEqual(endKey);
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

    public KVQuery asKVQuery() {
        return originalQuery;
    }
}
