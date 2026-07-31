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

package org.apache.hadoop.ozone.om.snapshot.trapped;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.SnapshotTrappedLedgerEntry;
import org.apache.hadoop.ozone.om.helpers.SnapshotTrappedLedgerEntry.State;

/**
 * Trapped accounting objectID ledger with a bounded read-through cache.
 */
public class SnapshotTrappedLedger {

  private final Table<String, SnapshotTrappedLedgerEntry> ledgerTable;
  private final Cache<String, SnapshotTrappedLedgerEntry> cache;
  private final Map<BatchOperation, Set<String>> keysInsertedInBatch;

  public SnapshotTrappedLedger(
      Table<String, SnapshotTrappedLedgerEntry> ledgerTable,
      int cacheSize) {
    this.ledgerTable = ledgerTable;
    Preconditions.checkArgument(cacheSize > 0, "cacheSize must be > 0");
    this.cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
    this.keysInsertedInBatch = new WeakHashMap<>();
  }

  /**
   * Inserts a ledger row only if absent in both DB/cache and current batch.
   *
   * @return true if inserted; false if already present
   */
  public boolean putIfAbsent(
      BatchOperation batchOperation,
      long volumeId,
      long bucketId,
      long objectId,
      UUID snapshotId,
      State state) throws IOException {
    Preconditions.checkNotNull(batchOperation, "batchOperation == null");
    Preconditions.checkNotNull(snapshotId, "snapshotId == null");
    Preconditions.checkNotNull(state, "state == null");

    String ledgerKey =
        SnapshotTrappedLedgerEntry.encodeKey(volumeId, bucketId, objectId);

    Set<String> insertedKeys = getOrCreateBatchKeys(batchOperation);
    if (insertedKeys.contains(ledgerKey)) {
      return false;
    }

    SnapshotTrappedLedgerEntry existing = get(ledgerKey);
    if (existing != null) {
      insertedKeys.add(ledgerKey);
      return false;
    }

    SnapshotTrappedLedgerEntry newEntry =
        new SnapshotTrappedLedgerEntry(objectId, snapshotId, state);
    ledgerTable.putWithBatch(batchOperation, ledgerKey, newEntry);
    cache.put(ledgerKey, newEntry);
    insertedKeys.add(ledgerKey);
    return true;
  }

  /**
   * Gets the ledger row for a key, reading through to DB on cache miss.
   */
  public SnapshotTrappedLedgerEntry get(String ledgerKey) throws IOException {
    SnapshotTrappedLedgerEntry entry = cache.getIfPresent(ledgerKey);
    if (entry != null) {
      return entry;
    }
    entry = ledgerTable.getIfExist(ledgerKey);
    if (entry != null) {
      cache.put(ledgerKey, entry);
    }
    return entry;
  }

  /**
   * Compares state and updates it atomically within the batch context.
   *
   * @return true when state transition was applied
   */
  public boolean compareAndSetState(
      BatchOperation batchOperation,
      long volumeId,
      long bucketId,
      long objectId,
      EnumSet<State> expectedStates,
      State targetState) throws IOException {
    Preconditions.checkNotNull(batchOperation, "batchOperation == null");
    Preconditions.checkNotNull(expectedStates, "expectedStates == null");
    Preconditions.checkArgument(!expectedStates.isEmpty(),
        "expectedStates must not be empty");
    Preconditions.checkNotNull(targetState, "targetState == null");

    String ledgerKey =
        SnapshotTrappedLedgerEntry.encodeKey(volumeId, bucketId, objectId);
    SnapshotTrappedLedgerEntry existing = get(ledgerKey);
    if (existing == null || !expectedStates.contains(existing.getState())) {
      return false;
    }

    SnapshotTrappedLedgerEntry updated = new SnapshotTrappedLedgerEntry(
        existing.getObjectId(),
        existing.getSnapshotId(),
        targetState);
    ledgerTable.putWithBatch(batchOperation, ledgerKey, updated);
    cache.put(ledgerKey, updated);
    return true;
  }

  private Set<String> getOrCreateBatchKeys(BatchOperation batchOperation) {
    synchronized (keysInsertedInBatch) {
      return keysInsertedInBatch.computeIfAbsent(
          batchOperation, ignored -> new HashSet<>());
    }
  }

  @VisibleForTesting
  void invalidateCacheEntry(String ledgerKey) {
    cache.invalidate(ledgerKey);
  }
}
