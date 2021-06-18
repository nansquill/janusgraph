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
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.DIRECTORY;
import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.ISOLATION_LEVEL;
import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.VERSION;
import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.GET_RANGE_MODE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

@PreInitializeConfigOptions
public class FoundationDBStoreManager extends AbstractStoreManager implements OrderedKeyValueStoreManager, AutoCloseable {

    public enum RangeQueryIteratorMode { ASYNC, SYNC }

    private RangeQueryIteratorMode mode;

    private final Map<String, FoundationDBKeyValueStore> stores;

    protected FDB foundationDatabase;
    protected Database database;
    protected DirectorySubspace rootDirectory;
    protected String rootDirectoryName;
    protected final StoreFeatures storeFeatures;
    protected final FoundationDBTransaction.IsolationLevel isolationLevel;


    public FoundationDBStoreManager(Configuration storageConfig) throws BackendException {
        super(storageConfig);

        stores = new ConcurrentHashMap<>();
        foundationDatabase = FDB.selectAPIVersion(storageConfig.get(VERSION));

        final String isolationLevelStr = storageConfig.get(ISOLATION_LEVEL);
        switch (isolationLevelStr.toLowerCase().trim()) {
            case "serializable":
                isolationLevel = FoundationDBTransaction.IsolationLevel.SERIALIZABLE;
                break;
            case "read_committed_no_write":
                isolationLevel = FoundationDBTransaction.IsolationLevel.READ_COMMITTED_NO_WRITE;
                break;
            case "read_committed_with_write":
                isolationLevel = FoundationDBTransaction.IsolationLevel.READ_COMMITTED_WITH_WRITE;
                break;
            default:
                throw new PermanentBackendException("Unrecognized isolation level " + isolationLevelStr);
        }

        if (!storageConfig.has(DIRECTORY) && (storageConfig.has(GRAPH_NAME))){
            rootDirectoryName = storageConfig.get(GRAPH_NAME);
        }
        else {
            rootDirectoryName = storageConfig.get(DIRECTORY);
        }

        try {
            rootDirectory = DirectoryLayer.getDefault().createOrOpen(database, PathUtil.from(rootDirectoryName)).get();
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }

        final String getRangeMode = storageConfig.get(GET_RANGE_MODE);
        switch (getRangeMode.toLowerCase().trim()) {
            case "iterator":
                mode = RangeQueryIteratorMode.ASYNC;
                break;
            case "list":
                mode = RangeQueryIteratorMode.SYNC;
                break;
        }

        storeFeatures = new StandardStoreFeatures.Builder()
            .orderedScan(true)
            .transactional(transactional)
            .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
            .locking(true)
            .keyOrdered(true)
            .supportsInterruption(false)
            .optimisticLocking(true)
            .multiQuery(true)
            .build();
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig baseTransactionConfig) throws BackendException {
        try {
            final Transaction transaction = database.createTransaction();
            final FoundationDBTransaction foundationDBTransaction = new FoundationDBTransaction(database, transaction, baseTransactionConfig, isolationLevel);
            return foundationDBTransaction;
        }
        catch (Exception exception) {
            throw new PermanentBackendException("Could not start FoundationDB transaction", exception);
        }
    }

    @Override
    public void close() throws BackendException {
        if(foundationDatabase != null) {
            if(!stores.isEmpty()) {
                throw new IllegalStateException("Cannot shutdown manager since some database are still open");
            }
            try {
                Thread.sleep(30);
            }
            catch (InterruptedException interruptedException) {

            }
            try {
                database.close();
            }
            catch (Exception exception) {
                throw new PermanentBackendException("Could not close FoundationDB database", exception);
            }
        }
    }

    @Override
    public void clearStorage() throws BackendException {

        try {
            rootDirectory.removeIfExists(database).get();
        }
        catch (Exception exception) {
            throw new PermanentBackendException("Could not clear FoundationDB storage", exception);
        }
    }

    @Override
    public boolean exists() throws BackendException {

        try {
            return DirectoryLayer.getDefault().exists(database, PathUtil.from(rootDirectoryName)).get();
        }
        catch (InterruptedException interruptedException) {
            throw new PermanentBackendException(interruptedException);
        }
        catch (ExecutionException executionException) {
            throw new PermanentBackendException(executionException);
        }
    }

    @Override
    public StoreFeatures getFeatures() {
        return storeFeatures;
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FoundationDBKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        if(stores.containsKey(name)) {
            return stores.get(name);
        }
        try {
            final DirectorySubspace storeDatabase = rootDirectory.createOrOpen(database, PathUtil.from(name)).get();
            FoundationDBKeyValueStore store = new FoundationDBKeyValueStore(name, storeDatabase, this);
            stores.put(name, store);
            return store;
        }
        catch (Exception exception) {
            throw new PermanentBackendException("Could not open FoundationDB data store", exception);
        }
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        try {
            for(Map.Entry<String, KVMutation> mutationEntry : mutations.entrySet()) {
                FoundationDBKeyValueStore store = openDatabase(mutationEntry.getKey());
                KVMutation mutationValue = mutationEntry.getValue();

                if(mutationValue.hasAdditions()) {
                    for(KeyValueEntry keyValueEntry : mutationValue.getAdditions()) {
                        store.insert(keyValueEntry.getKey(), keyValueEntry.getValue(), txh);
                    }
                }
                if(mutationValue.hasDeletions()) {
                    for(StaticBuffer staticBuffer : mutationValue.getDeletions()) {
                        store.delete(staticBuffer, txh);
                    }
                }
            }
        }
        catch (BackendException backendException) {
            throw backendException;
        }
        catch (Exception exception) {
            throw new PermanentBackendException(exception);
        }
    }

    public void removeDatabase(FoundationDBKeyValueStore foundationDBKeyValueStore) {
        if(!stores.containsKey(foundationDBKeyValueStore.getName())) {
            throw new IllegalArgumentException("Tried to remove an unknown database from the storage manager");
        }
        String name = foundationDBKeyValueStore.getName();
        stores.remove(name);
    }

    public RangeQueryIteratorMode getMode() {
        return mode;
    }
}
