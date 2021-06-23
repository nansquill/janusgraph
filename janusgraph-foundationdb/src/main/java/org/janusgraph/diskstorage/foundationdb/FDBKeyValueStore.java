package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.Database;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.janusgraph.diskstorage.StaticBuffer.ARRAY_FACTORY;

public class FDBKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(FDBKeyValueStore.class);

    private final Database database;
    private final DirectorySubspace subspace;
    private final String name;
    private final FDBStoreManager fdbStoreManager;
    private boolean isOpen;

    public FDBKeyValueStore(String name, Database database, DirectorySubspace subspace, FDBStoreManager fdbStoreManager) {
        this.name = name;
        this.database = database;
        this.subspace = subspace;
        this.fdbStoreManager = fdbStoreManager;
        this.isOpen = true;
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        log.trace("Deletion");
        FDBTx tx = getTransaction(txh);
        try {
            log.trace("db={}, op=delete, tx={}", name, txh);
            tx.clear(subspace.pack(key.as(ARRAY_FACTORY)));
        } catch (PermanentBackendException exception) {
            throw exception;
        }
        catch (Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        FDBTx tx = getTransaction(txh);
        try {
            byte[] databaseEntry = key.as(ARRAY_FACTORY);

            log.trace("db={}, op=get, tx={}", name, txh);

            final byte[] entry = tx.get(subspace.pack(key.as(ARRAY_FACTORY)));

            if(entry != null) {
                return getBuffer(entry);
            }
            return null;
        } catch (PermanentBackendException exception) {
            throw exception;
        }
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key, txh) != null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        if (getTransaction(txh) == null) {
            log.warn("Attempt to acquire lock with transactions disabled");
        } //else we need no locking
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws BackendException {
        try {
            if(isOpen) {
                CompletableFuture<Boolean> isClosed = subspace.removeIfExists(database, subspace.getPath());
                if(!isClosed.get()) {
                    throw new PermanentBackendException("Directory has not been closed");
                }
            }
        } catch (CancellationException exception) {
            throw new PermanentBackendException("Transaction's future has been cancelled");
        } catch (ExecutionException exception) {
            throw new PermanentBackendException("Transaction's future has completed exceptionally");
        } catch (InterruptedException exception) {
            throw new PermanentBackendException("Current thread was interrupted while waiting");
        } catch (RuntimeException exception) {
            throw new PermanentBackendException("Transaction's rollback in progress");
        }
        if(isOpen) {
            fdbStoreManager.removeDatabase(this);
        }
        isOpen = false;
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, Integer ttl) throws BackendException {

        FDBTx tx = getTransaction(txh);

        log.trace("db={}, op=insert, tx={}", name, txh);

        try {
            tx.set(subspace.pack(key.as(ARRAY_FACTORY)), subspace.pack(value.as(ARRAY_FACTORY)));
        } catch (PermanentBackendException exception) {
            throw exception;
        }
        catch (Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        return null;
    }

    @Override
    public Map<KVQuery, RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        return null;
    }

    private FDBTx getTransaction(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null);
        return (FDBTx) txh;
    }

    public static StaticBuffer getBuffer(byte[] entry) {
        return new StaticArrayBuffer(entry);
    }
}
