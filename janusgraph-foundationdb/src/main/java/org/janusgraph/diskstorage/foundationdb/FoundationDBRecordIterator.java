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

public class FoundationDBRecordIterator implements RecordIterator<KeyValueEntry> {

    protected Subspace storeDatabase;
    protected Iterator<KeyValue> entries;
    protected KeySelector selector;

    protected KeyValueEntry nextKeyValueEntry;

    public FoundationDBRecordIterator(DirectorySubspace storeDatabase, Iterator<KeyValue> entries, KeySelector selector) {
        this.storeDatabase = storeDatabase;
        this.entries = entries;
        this.selector = selector;

        nextKeyValueEntry = null;
    }

    @Override
    public void close() throws IOException {

    }

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

    protected void fetchNext() {
        while(nextKeyValueEntry == null && entries.hasNext()) {
            KeyValue keyValue = entries.next();
            StaticBuffer key = FoundationDBKeyValueStore.getBuffer(storeDatabase.unpack(keyValue.getKey()).getBytes(0));
            if(selector.include(key)) {
                nextKeyValueEntry = new KeyValueEntry(key, FoundationDBKeyValueStore.getBuffer(keyValue.getValue()));
            }
        }
    }
}
