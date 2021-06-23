package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.google.common.base.Preconditions;
import com.apple.foundationdb.Transaction;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class FDBTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(FDBTx.class);

    private volatile Transaction tx;
    private final Database database;

    public FDBTx(Transaction t, BaseTransactionConfig config) {
        super(config);

        tx = t;
        database = t.getDatabase();
        Preconditions.checkNotNull(this.tx);
    }

    //atomic
    @Nullable
    public byte[] get(byte[] key) throws PermanentBackendException {
        CompletableFuture<byte[]> future = tx.get(key);
        if(future == null) {
            //throw new PermanentBackendException("Transaction key not found on database");
            return null;
        }
        try {
            return future.get();
        } catch (CancellationException exception) {
            throw new PermanentBackendException("Transaction's future has been cancelled");
        } catch (ExecutionException exception) {
            throw new PermanentBackendException("Transaction's future has completed exceptionally");
        } catch (InterruptedException exception) {
            throw new PermanentBackendException("Current thread was interrupted while waiting");
        } catch (RuntimeException exception) {
            throw new PermanentBackendException("Transaction's rollback in progress");
        }
    }

    //atomic
    @Nonnull
    public List<KeyValue> getRange(FDBRangeQuery fdbRangeQuery) throws PermanentBackendException {
        CompletableFuture<List<KeyValue>> result = tx.getRange(fdbRangeQuery.getStartKeySelector(), fdbRangeQuery.getEndKeySelector(), fdbRangeQuery.getLimit()).asList();
        if(result == null) {
            return Collections.emptyList();
        }
        try {
            return result.get();
        } catch (CancellationException exception) {
            throw new PermanentBackendException("Transaction's future has been cancelled");
        } catch (ExecutionException exception) {
            throw new PermanentBackendException("Transaction's future has completed exceptionally");
        } catch (InterruptedException exception) {
            throw new PermanentBackendException("Current thread was interrupted while waiting");
        } catch (RuntimeException exception) {
            throw new PermanentBackendException("Transaction's rollback in progress");
        }
    }

    //atomic
    public void set(byte[] key, byte[] value) throws PermanentBackendException {
        try {
            tx.set(key, value);
        } catch (IllegalArgumentException exception) {
            throw new PermanentBackendException("Key and/or value are not set");
        } catch (FDBException exception) {
            throw new PermanentBackendException("Set operation failed");
        } catch (RuntimeException exception) {
            throw new PermanentBackendException("Transaction's rollback in progress");
        }
    }

    //atomic
    @Nonnull
    public void clear(byte[] key) throws PermanentBackendException{
        try {
            tx.clear(key);
        } catch (IllegalArgumentException exception) {
            throw new PermanentBackendException("Key and/or value are not set");
        } catch (FDBException exception) {
            throw new PermanentBackendException("Set operation failed");
        } catch (RuntimeException exception) {
            throw new PermanentBackendException("Transaction's rollback in progress");
        }
    }

    @Override
    public synchronized void commit() throws BackendException {
        super.commit();
        if(tx == null) { return; }
        if(log.isTraceEnabled()) {
            log.trace("{} committed", this);
        }
        try {
            CompletableFuture<Void> future = tx.commit();
            future.wait();
            return; //success
        } catch (FDBException exception) {
            //commit_unknown_result or used_during_commit
            throw new PermanentBackendException("Transaction's commit unknown result or used during commit");
        } catch (IllegalMonitorStateException exception) {
            throw new PermanentBackendException("Current thread is not the owner of the object's monitor");
        } catch (InterruptedException exception) {
            throw new PermanentBackendException("Current thread interrupted during commit");
        } catch (RuntimeException exception) {
            throw new PermanentBackendException("Transaction's rollback in progress");
        }
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        if(tx == null) { return; }
        if(log.isTraceEnabled()) {
            log.trace("{} rollback", this);
        }
        try {
            tx.cancel();
        }
        catch (Exception exception) {
            throw new PermanentBackendException("Transaction's rollback in progress");
        }
    }

    public Transaction getTransaction() {
        return tx;
    }
}
