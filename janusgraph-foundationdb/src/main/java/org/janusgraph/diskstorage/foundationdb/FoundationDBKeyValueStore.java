package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FoundationDBKeyValueStore implements OrderedKeyValueStore, AutoCloseable {

    static final StaticBuffer.Factory<byte[]> ENTRY_FACTORY = (array, offset, limit) -> {
        final byte[] bArray = new byte[limit - offset];
        System.arraycopy(array, offset, bArray, 0, limit - offset);
        return bArray;
    };

    private String name;
    private DirectorySubspace storeDatabase;
    private final FoundationDBStoreManager storeManager;
    private boolean isOpen;

    public FoundationDBKeyValueStore(String name, DirectorySubspace storeDatabase, FoundationDBStoreManager foundationDBStoreManager) {
        name = name;
        storeDatabase = storeDatabase;
        storeManager = foundationDBStoreManager;
        isOpen = true;
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        FoundationDBTransaction transaction = getTransaction(txh);
        try {
            transaction.clear(storeDatabase.pack(key.as(ENTRY_FACTORY)));
        }
        catch (Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        FoundationDBTransaction transaction = getTransaction(txh);
        try {
            byte[] databaseKey = storeDatabase.pack(key.as(ENTRY_FACTORY));
            final byte[] entry = transaction.get(databaseKey);
            if(entry != null) {
                return getBuffer(entry);
            }
            else {
                return null;
            }
        }
        catch (BackendException backendException) {
            throw backendException;
        }
        catch (Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key, txh) != null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        if(getTransaction(txh) == null) {

        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws BackendException {
        if(isOpen) {
            storeManager.removeDatabase(this);
        }
        isOpen = false;
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, Integer ttl) throws BackendException {
        insert(key, value, txh);
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        final FoundationDBTransaction transaction = getTransaction(txh);
        if(storeManager.getMode() == FoundationDBStoreManager.RangeQueryIteratorMode.SYNC) {

            try {
                final List<KeyValue> result = transaction.getRange(new FoundationDBRangeQuery(storeDatabase, query));
                return new FoundationDBRecordIterator(storeDatabase, result.iterator(), query.getKeySelector());
            }
            catch (BackendException backendException) {
                throw backendException;
            }
            catch (Exception exception) {
                throw new PermanentBackendException(exception);
            }
        }
        else {
            try {
                final FoundationDBRangeQuery rangeQuery = new FoundationDBRangeQuery(storeDatabase, query);
                final AsyncIterator<KeyValue> result = transaction.getRangeIterator(rangeQuery);
                return new FoundationDBRecordAsyncIterator(storeDatabase, transaction, rangeQuery, result, query.getKeySelector());
            }
            catch (Exception exception) {
                throw new PermanentBackendException(exception);
            }
        }
    }

    @Override
    public Map<KVQuery, RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        FoundationDBTransaction transaction = getTransaction(txh);
        if(storeManager.getMode() == FoundationDBStoreManager.RangeQueryIteratorMode.SYNC) {
            final Map<KVQuery, FoundationDBRangeQuery> fdbQueries = new HashMap<>();
            try {
                for(final KVQuery kvQuery : queries) {
                    fdbQueries.put(kvQuery, new FoundationDBRangeQuery(storeDatabase, kvQuery));
                }
                final Map<KVQuery, List<KeyValue>> unfilteredResultMap = transaction.getMultiRange(fdbQueries.values());
                final Map<KVQuery, RecordIterator<KeyValueEntry>> iteratorMap = new HashMap<>();
                for(Map.Entry<KVQuery, List<KeyValue>> kvQueryListEntry : unfilteredResultMap.entrySet()) {
                    iteratorMap.put(kvQueryListEntry.getKey(), new FoundationDBRecordIterator(storeDatabase, kvQueryListEntry.getValue().iterator(), kvQueryListEntry.getKey().getKeySelector()));
                }
                return iteratorMap;
            }
            catch (BackendException backendException) {
                throw backendException;
            }
            catch (Exception exception) {
                throw new PermanentBackendException(exception);
            }
        }
        else {
            final Map<KVQuery, RecordIterator<KeyValueEntry>> resultMap = new HashMap<>();
            try {
                for(final KVQuery kvQuery : queries) {
                    FoundationDBRangeQuery rangeQuery = new FoundationDBRangeQuery(storeDatabase, kvQuery);
                    AsyncIterator<KeyValue> result = transaction.getRangeIterator(rangeQuery);
                    resultMap.put(kvQuery, new FoundationDBRecordAsyncIterator(storeDatabase, transaction, rangeQuery, result, kvQuery.getKeySelector()));
                }
            }
            catch (Exception exception) {
                throw new PermanentBackendException(exception);
            }
            return resultMap;
        }
    }

    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws PermanentBackendException {
        FoundationDBTransaction transaction = getTransaction(txh);
        try {
            transaction.set(storeDatabase.pack(key.as(ENTRY_FACTORY)), value.as(ENTRY_FACTORY));
        }
        catch (Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }

    private static FoundationDBTransaction getTransaction(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null);
        return ((FoundationDBTransaction) txh);
    }

    public static StaticBuffer getBuffer(byte[] entry) {
        return new StaticArrayBuffer(entry);
    }
}
