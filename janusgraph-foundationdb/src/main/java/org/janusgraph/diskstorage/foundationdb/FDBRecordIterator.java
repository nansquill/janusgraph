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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.util.RecordIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FDBRecordIterator implements RecordIterator<KeyValueEntry> {

    protected Subspace storeDatabase;
    protected Iterator<KeyValue> entries;
    protected KeySelector selector;

    protected KeyValueEntry nextKeyValueEntry;

    public FDBRecordIterator(DirectorySubspace storeDatabase, Iterator<KeyValue> entries, KeySelector selector) {
        this.storeDatabase = storeDatabase;
        this.entries = entries;
        this.selector = selector;

        nextKeyValueEntry = null;
    }

    /* RecordIterator implementation */

    @Override
    public boolean hasNext() {
        fetchNext();
        return (nextKeyValueEntry != null);
    }

    @Override
    public KeyValueEntry next() {
        if(hasNext()) {
            KeyValueEntry result = nextKeyValueEntry;
            nextKeyValueEntry = null;
            return result;
        }
        else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {

    }

    /* RecordIterator implementation end */

    protected void fetchNext() {
        while(nextKeyValueEntry == null && entries.hasNext()) {
            KeyValue keyValue = entries.next();
            StaticBuffer key = FDBKeyValueStore.getBuffer(storeDatabase.unpack(keyValue.getKey()).getBytes(0));
            if(selector.include(key)) {
                nextKeyValueEntry = new KeyValueEntry(key, FDBKeyValueStore.getBuffer(keyValue.getValue()));
            }
        }
    }
}
