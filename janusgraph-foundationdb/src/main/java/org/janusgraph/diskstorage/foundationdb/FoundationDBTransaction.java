package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class FoundationDBTransaction extends AbstractStoreTransaction {

    private volatile Transaction transaction;

    private final AtomicInteger transactionControl = new AtomicInteger(0);

    private final List<Insert> inserts = Collections.synchronizedList(new ArrayList<>());
    private final List<byte[]> deletions = Collections.synchronizedList(new ArrayList<>());

    public enum IsolationLevel {SERIALIZABLE, READ_COMMITTED_NO_WRITE, READ_COMMITTED_WITH_WRITE}

    public FoundationDBTransaction(Database database, Transaction transaction, BaseTransactionConfig baseTransactionConfig, IsolationLevel isolationLevel) {
        super(baseTransactionConfig);
    }

    public void clear(byte[] key) {
        deletions.add(key);
        transaction.clear(key);
    }

    public byte[] get(byte[] databaseKey) throws PermanentBackendException {
        try {
            return this.transaction.get(databaseKey).get();
        }
        catch (ExecutionException executionException) {
            throw new PermanentBackendException("Max transaction reset count exceeded with final exception", executionException);
        }
        catch (Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }

    public List<KeyValue> getRange(final FoundationDBRangeQuery query) throws PermanentBackendException {
        final int startTransactionId = transactionControl.get();
        try {
            List<KeyValue> result = transaction.getRange(query.getStartKeySelector(), query.getEndKeySelector(), query.getLimit()).asList().get();
            return result != null ? result : Collections.emptyList();
        }
        catch (ExecutionException executionException) {
            throw new PermanentBackendException("Max transaction reset count exceeded with final exception", executionException);
        }
        catch (Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }

    public AsyncIterator<KeyValue> getRangeIterator(FoundationDBRangeQuery rangeQuery) {
        final int limit = rangeQuery.asKVQuery().getLimit();
        return transaction.getRange(rangeQuery.getStartKeySelector(), rangeQuery.getEndKeySelector(), limit, false, StreamingMode.WANT_ALL).iterator();
    }

    public void set(final byte[] key, final byte[] value) {
        inserts.add(new Insert(key, value));
    }

    public synchronized Map<KVQuery, List<KeyValue>> getMultiRange(final Collection<FoundationDBRangeQuery> queries) throws PermanentBackendException {
        //TODO
        Map<KVQuery, List<KeyValue>> resultMap = new ConcurrentHashMap<>();
        final List<FoundationDBRangeQuery> retries = new CopyOnWriteArrayList<>(queries);
        return resultMap;
    }

    private class Insert {
        private final byte[] key;
        private final byte[] value;

        public Insert(final byte[] key, final byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() {
            return this.key;
        }

        public byte[] getValue() {
            return this.value;
        }
    }
}
