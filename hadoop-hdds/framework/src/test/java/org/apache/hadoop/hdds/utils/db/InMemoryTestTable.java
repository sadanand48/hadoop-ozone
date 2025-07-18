/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;

/**
 * InMemory Table implementation for tests.
 */
public final class InMemoryTestTable<KEY, VALUE> implements Table<KEY, VALUE> {
  private final Map<KEY, VALUE> map;

  public InMemoryTestTable() {
    this(Collections.emptyMap());
  }

  public InMemoryTestTable(Map<KEY, VALUE> map) {
    this.map = new ConcurrentHashMap<>();
    this.map.putAll(map);
  }

  @Override
  public void put(KEY key, VALUE value) {
    map.put(key, value);
  }

  @Override
  public void putWithBatch(BatchOperation batch, KEY key, VALUE value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean isExist(KEY key) {
    return map.containsKey(key);
  }

  @Override
  public VALUE get(KEY key) {
    return map.get(key);
  }

  @Override
  public VALUE getIfExist(KEY key) {
    return map.get(key);
  }

  @Override
  public void delete(KEY key) {
    map.remove(key);
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, KEY key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteRange(KEY beginKey, KEY endKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyValueIterator<KEY, VALUE> iterator(KEY prefix, KeyValueIterator.Type type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName() {
    return "";
  }

  @Override
  public long getEstimatedKeyCount() {
    return map.size();
  }

  @Override
  public List<KeyValue<KEY, VALUE>> getRangeKVs(KEY startKey, int count, KEY prefix,
      MetadataKeyFilters.MetadataKeyFilter... filters) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<KeyValue<KEY, VALUE>> getSequentialRangeKVs(KEY startKey, int count, KEY prefix,
                                                                    MetadataKeyFilters.MetadataKeyFilter... filters) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteBatchWithPrefix(BatchOperation batch, KEY prefix) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dumpToFileWithPrefix(File externalFile, KEY prefix) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void loadFromFile(File externalFile) {
    throw new UnsupportedOperationException();
  }
}
