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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.EnumSet;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.SnapshotTrappedLedgerEntry;
import org.apache.hadoop.ozone.om.helpers.SnapshotTrappedLedgerEntry.State;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link SnapshotTrappedLedger}.
 */
public class TestSnapshotTrappedLedger {

  @TempDir
  private File folder;

  private OmMetadataManagerImpl omMetadataManager;
  private SnapshotTrappedLedger ledger;
  private Table<String, SnapshotTrappedLedgerEntry> ledgerTable;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS, folder.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(conf, null);
    ledgerTable = omMetadataManager.getSnapshotTrappedLedgerTable();
    ledger = new SnapshotTrappedLedger(ledgerTable, 100);
  }

  @AfterEach
  public void teardown() throws Exception {
    if (omMetadataManager != null) {
      omMetadataManager.stop();
    }
  }

  @Test
  public void testPutIfAbsentWithBatchAndDbDedup() throws Exception {
    UUID snapshotId = UUID.randomUUID();
    long volumeId = 1L;
    long bucketId = 2L;
    long objectId = 3L;
    String ledgerKey = SnapshotTrappedLedgerEntry.encodeKey(volumeId, bucketId, objectId);

    try (BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation()) {
      assertTrue(ledger.putIfAbsent(
          batchOperation, volumeId, bucketId, objectId, snapshotId, State.ACCOUNTED_KEY));
      assertFalse(ledger.putIfAbsent(
          batchOperation, volumeId, bucketId, objectId, snapshotId, State.ACCOUNTED_KEY));
      omMetadataManager.getStore().commitBatchOperation(batchOperation);
    }

    SnapshotTrappedLedgerEntry persisted = ledgerTable.get(ledgerKey);
    assertNotNull(persisted);
    assertEquals(State.ACCOUNTED_KEY, persisted.getState());
    assertEquals(snapshotId, persisted.getSnapshotId());

    try (BatchOperation secondBatch = omMetadataManager.getStore().initBatchOperation()) {
      assertFalse(ledger.putIfAbsent(
          secondBatch, volumeId, bucketId, objectId, snapshotId, State.ACCOUNTED_KEY));
    }
  }

  @Test
  public void testPutIfAbsentReadsDbOnCacheMiss() throws Exception {
    UUID snapshotId = UUID.randomUUID();
    long volumeId = 11L;
    long bucketId = 22L;
    long objectId = 33L;
    String ledgerKey = SnapshotTrappedLedgerEntry.encodeKey(volumeId, bucketId, objectId);
    SnapshotTrappedLedgerEntry existing = new SnapshotTrappedLedgerEntry(
        objectId, snapshotId, State.ACCOUNTED_DIR_ROOT);
    ledgerTable.put(ledgerKey, existing);

    ledger.invalidateCacheEntry(ledgerKey);

    try (BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation()) {
      assertFalse(ledger.putIfAbsent(
          batchOperation, volumeId, bucketId, objectId, snapshotId, State.ACCOUNTED_DIR_ROOT));
    }
  }

  @Test
  public void testCompareAndSetState() throws Exception {
    UUID snapshotId = UUID.randomUUID();
    long volumeId = 5L;
    long bucketId = 6L;
    long objectId = 7L;
    String ledgerKey = SnapshotTrappedLedgerEntry.encodeKey(volumeId, bucketId, objectId);

    try (BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation()) {
      assertTrue(ledger.putIfAbsent(
          batchOperation, volumeId, bucketId, objectId, snapshotId, State.ACCOUNTED_KEY));
      omMetadataManager.getStore().commitBatchOperation(batchOperation);
    }

    try (BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation()) {
      assertTrue(ledger.compareAndSetState(
          batchOperation, volumeId, bucketId, objectId,
          EnumSet.of(State.ACCOUNTED_KEY), State.PURGED));
      omMetadataManager.getStore().commitBatchOperation(batchOperation);
    }
    assertEquals(State.PURGED, ledgerTable.get(ledgerKey).getState());

    try (BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation()) {
      assertFalse(ledger.compareAndSetState(
          batchOperation, volumeId, bucketId, objectId,
          EnumSet.of(State.ACCOUNTED_KEY), State.PURGED));
    }
  }
}
