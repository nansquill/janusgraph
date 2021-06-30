// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

    public FDBTx(Transaction tx, BaseTransactionConfig config) {
        super(config);

        Preconditions.checkNotNull(this.tx);
        this.tx = tx;
        this.database = tx.getDatabase();
    }

    /* AbstractStoreTransaction implementation */

    @Override
    public synchronized void commit() throws BackendException {
        super.commit();
        if(tx == null) { return; }
        if(log.isTraceEnabled()) {
            log.trace("{} committed", this, new TransactionClose(this.toString()));
        }
        try {
            tx.commit().get();
            tx.close();
            tx = null;
            //success
        } catch (FDBException exception) {
            //commit_unknown_result or used_during_commit
            throw new PermanentBackendException("Transaction's commit unknown result or used during commit");
        } catch (IllegalMonitorStateException exception) {
            throw new PermanentBackendException("Current thread is not the owner of the object's monitor");
        } catch (InterruptedException exception) {
            throw new PermanentBackendException("Current thread interrupted during commit");
        } catch (RuntimeException exception) {
            throw new PermanentBackendException("Transaction's rollback in progress");
        } catch (ExecutionException executionException) {
            throw new PermanentBackendException("Transaction's execution failed");
        }
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        if(tx == null) { return; }
        if(log.isTraceEnabled()) {
            log.trace("{} rollback", this, new TransactionClose(this.toString()));
        }
        try {
            tx.cancel();
            tx = null;
        }
        catch (Exception exception) {
            throw new PermanentBackendException("Transaction's rollback in progress");
        }
    }

    /* AbstractStoreTransaction implementation end */

    /* BaseTransactionConfigurable implementation */

    public Transaction getTransaction() {
        return tx;
    }

    /* BaseTransactionConfigurable implementation end */

    @Override
    public String toString() {
        return getClass().getSimpleName() + (null == tx ? "nulltx" : tx.toString());
    }

    private static class TransactionClose extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionClose(String msg) {
            super(msg);
        }
    }

    /**
     * atomics :
     * byte[] get(byte[] key)
     * List getRange(FDBRangeQuery fdbRangeQuery)
     * void set(byte[] key, byte[] value)
     * void clear(byte[] key)
     */

    @Nullable
    public byte[] get(byte[] key) throws BackendException {
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

    @Nonnull
    public List<KeyValue> getRange(FDBRangeQuery fdbRangeQuery) throws BackendException {
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

    public void set(byte[] key, byte[] value) throws BackendException {
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

    @Nonnull
    public void clear(byte[] key) throws BackendException{
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

}
