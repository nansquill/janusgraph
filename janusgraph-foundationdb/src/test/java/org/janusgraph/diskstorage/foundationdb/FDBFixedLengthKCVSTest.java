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

import com.google.common.collect.ImmutableMap;
import org.janusgraph.FDBContainer;
import org.janusgraph.FDBStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.ExecutionException;

@Testcontainers
public class FDBFixedLengthKCVSTest extends KeyColumnValueStoreTest {

    @Container
    public static FDBContainer container = new FDBContainer();

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        FDBStoreManager sm = new FDBStoreManager(container.getFDBConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(sm, ImmutableMap.of(storeName, 8));
    }

    @Override
    public void testConcurrentGetSlice() throws ExecutionException, InterruptedException, BackendException {
        super.testConcurrentGetSlice();
    }

    @Override
    public void testConcurrentGetSliceAndMutate() throws BackendException, ExecutionException, InterruptedException {
        super.testConcurrentGetSliceAndMutate();
    }
}
