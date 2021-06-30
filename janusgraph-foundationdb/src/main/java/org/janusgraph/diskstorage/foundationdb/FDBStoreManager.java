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
import org.janusgraph.diskstorage.common.LocalStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;
import org.janusgraph.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.janusgraph.diskstorage.foundationdb.FDBConfiguration.CLUSTER_FILE_PATH;
import static org.janusgraph.diskstorage.foundationdb.FDBConfiguration.STORAGE_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

public class FDBStoreManager extends LocalStoreManager implements OrderedKeyValueStoreManager, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FDBStoreManager.class);

    private final Map<String, FDBKeyValueStore> stores;

    protected final FDB fdb;
    protected Database environment;
    protected DirectorySubspace rootDirectory;
    protected final String rootDirectoryName;
    protected final StoreFeatures features;

    public FDBStoreManager(Configuration configuration) throws BackendException{
        super(configuration);
        //STORAGE_DIRECTORY, STORAGE_ROOT, GRAPH_NAME


        stores = new HashMap<>();
        fdb = FDB.selectAPIVersion(620);

        rootDirectoryName = (!configuration.has(STORAGE_DIRECTORY) && (configuration.has(GRAPH_NAME))) ?
            configuration.get(GRAPH_NAME) :
            configuration.get(STORAGE_DIRECTORY);

        environment = !"default".equals(configuration.get(CLUSTER_FILE_PATH)) ?
            fdb.open(configuration.get(CLUSTER_FILE_PATH)) : fdb.open();

        init();

        features = new StandardStoreFeatures.Builder()
            .orderedScan(true)
            .transactional(transactional)
            .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
            .locking(true)
            .keyOrdered(true)
            .supportsInterruption(false)
            .optimisticLocking(true)
            .multiQuery(true)
            .build();

        log.info("FDBStoreManager initialized");
    }

    /* OrderedKeyValueStoreManager implementation */

    @Override
    public FDBKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        if(stores.containsKey(name)) {
            return stores.get(name);
        }
        try {
            final DirectorySubspace storeDatabase = rootDirectory.createOrOpen(environment, PathUtil.from(name)).get();
            log.debug("Opened database {}", name);

            FDBKeyValueStore store = new FDBKeyValueStore(name, environment, storeDatabase, this);
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
                FDBKeyValueStore store = openDatabase(mutationEntry.getKey());
                KVMutation mutationValue = mutationEntry.getValue();

                if(mutationValue.hasAdditions()) {
                    for(KeyValueEntry keyValueEntry : mutationValue.getAdditions()) {
                        store.insert(keyValueEntry.getKey(), keyValueEntry.getValue(), txh, keyValueEntry.getTtl());
                        log.trace("Insertion on {}: {}", mutationEntry.getKey(), keyValueEntry);
                    }
                }
                if(mutationValue.hasDeletions()) {
                    for(StaticBuffer staticBuffer : mutationValue.getDeletions()) {
                        store.delete(staticBuffer, txh);
                        log.trace("Deletion on {}: {}", mutationEntry.getKey(), staticBuffer);
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

    /* OrderedKeyValueStoreManager implementation end */

    /* StoreManager implementation */

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) throws BackendException {
        try {
            Transaction tx = null;
            if(transactional) {
                tx = environment.createTransaction();
            }
            else {
                if(config instanceof TransactionConfiguration) {
                    if(!((TransactionConfiguration) config).isSingleThreaded()) {
                        // Non-transactional cursors can't shared between threads, more info ThreadLocker.checkState
                        throw new PermanentBackendException("FoundationDB does not support non-transactional for multi threaded tx");
                    }
                }
            }
            final FDBTx fdbTx = new FDBTx(tx, config);

            if (log.isTraceEnabled()) {
                log.trace("FoundationDB tx created", new TransactionBegin(fdbTx.toString()));
            }

            return fdbTx;
        }
        catch (Exception exception) {
            throw new PermanentBackendException("Could not start FoundationDB transaction", exception);
        }
    }

    @Override
    public void close() throws BackendException {
        if(environment != null) {
            if(!stores.isEmpty()) {
                throw new IllegalStateException("Cannot shutdown manager since some database are still open");
            }
            try {
                Thread.sleep(30);
            }
            catch (InterruptedException interruptedException) {

            }
            try {
                environment.close();
            }
            catch (Exception exception) {
                throw new PermanentBackendException("Could not close FoundationDB database", exception);
            }
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        if (!stores.isEmpty()) {
            throw new IllegalStateException("Cannot delete store, since database is open: " + stores.keySet());
        }
        try {
            rootDirectory.removeIfExists(environment).get();
            log.debug("Removed database {} (clearStorage)", environment);
        }
        catch (Exception exception) {
            throw new PermanentBackendException("Could not clear FoundationDB storage", exception);
        }
        close();
        IOUtils.deleteFromDirectory(directory);
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            return DirectoryLayer.getDefault().exists(environment, PathUtil.from(rootDirectoryName)).get();
        }
        catch (InterruptedException exception) {
            throw new PermanentBackendException(exception);
        }
        catch (ExecutionException exception) {
            throw new PermanentBackendException(exception);
        }
    }

    @Override
    public StoreFeatures getFeatures() { return features; }

    @Override
    public String getName() { return this.toString(); }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    /* StoreManager implementation end */

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + directory.toString();
    }

    private static class TransactionBegin extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionBegin(String msg) {
            super(msg);
        }
    }

    private void init() throws BackendException {
        try {
            // create the root directory to hold the JanusGraph data
            rootDirectory = DirectoryLayer.getDefault().createOrOpen(environment, PathUtil.from(rootDirectoryName)).get();
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }

    public void removeDatabase(FDBKeyValueStore fdbKeyValueStore) {
        if(!stores.containsKey(fdbKeyValueStore.getName())) {
            throw new IllegalArgumentException("Tried to remove an unknown database from the storage manager");
        }
        String name = fdbKeyValueStore.getName();
        stores.remove(name);
        log.debug("Removed database {}", name);
    }


}
