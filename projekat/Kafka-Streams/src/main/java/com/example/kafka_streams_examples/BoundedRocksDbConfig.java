package com.example.kafka_streams_examples;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.WriteBufferManager;

import java.util.Map;

/**
 * Ograničava ukupnu off-heap memoriju RocksDB-a (svi store-ovi dele isti
 * cache),
 * da kontejner ostane unutar docker memory limita.
 */
public class BoundedRocksDbConfig implements RocksDBConfigSetter {

    private static final long TOTAL_OFF_HEAP_BYTES = 48L * 1024 * 1024;
    private static final long TOTAL_MEMTABLE_BYTES = 16L * 1024 * 1024;

    private static final Cache CACHE = new LRUCache(TOTAL_OFF_HEAP_BYTES, -1, false, 0.1);
    private static final WriteBufferManager WRITE_BUFFER_MANAGER = new WriteBufferManager(TOTAL_MEMTABLE_BYTES, CACHE);

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        tableConfig.setBlockCache(CACHE);
        tableConfig.setCacheIndexAndFilterBlocks(true);
        tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
        tableConfig.setPinTopLevelIndexAndFilter(true);
        options.setTableFormatConfig(tableConfig);

        options.setWriteBufferManager(WRITE_BUFFER_MANAGER);
        options.setWriteBufferSize(4L * 1024 * 1024);
        options.setMaxWriteBufferNumber(2);
    }

    @Override
    public void close(String storeName, Options options) {
        // Cache i WriteBufferManager su zajednički za sve store-ove: ovde se namerno ne
        // zatvaraju.
    }
}