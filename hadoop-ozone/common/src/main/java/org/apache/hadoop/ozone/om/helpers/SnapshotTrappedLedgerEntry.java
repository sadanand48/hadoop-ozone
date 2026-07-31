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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import java.util.Objects;
import java.util.UUID;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotTrappedLedgerEntryProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotTrappedLedgerStateProto;

/**
 * Ledger row recording trapped accounting ownership for an objectID.
 */
public final class SnapshotTrappedLedgerEntry {
  private static final Codec<SnapshotTrappedLedgerEntry> CODEC =
      new DelegatedCodec<>(
          Proto2Codec.get(SnapshotTrappedLedgerEntryProto.getDefaultInstance()),
          SnapshotTrappedLedgerEntry::getFromProtobuf,
          SnapshotTrappedLedgerEntry::toProtobuf,
          SnapshotTrappedLedgerEntry.class);

  private final long objectId;
  private final UUID snapshotId;
  private final State state;

  public SnapshotTrappedLedgerEntry(long objectId, UUID snapshotId, State state) {
    this.objectId = objectId;
    this.snapshotId = Objects.requireNonNull(snapshotId, "snapshotId == null");
    this.state = Objects.requireNonNull(state, "state == null");
  }

  public static Codec<SnapshotTrappedLedgerEntry> getCodec() {
    return CODEC;
  }

  public long getObjectId() {
    return objectId;
  }

  public UUID getSnapshotId() {
    return snapshotId;
  }

  public State getState() {
    return state;
  }

  public SnapshotTrappedLedgerEntryProto toProtobuf() {
    return SnapshotTrappedLedgerEntryProto.newBuilder()
        .setObjectId(objectId)
        .setSnapshotId(HddsUtils.toProtobuf(snapshotId))
        .setState(state.toProto())
        .build();
  }

  public static SnapshotTrappedLedgerEntry getFromProtobuf(
      SnapshotTrappedLedgerEntryProto proto) {
    return new SnapshotTrappedLedgerEntry(
        proto.getObjectId(),
        fromProtobuf(proto.getSnapshotId()),
        State.fromProto(proto.getState()));
  }

  public static String encodeKey(long volumeId, long bucketId, long objectId) {
    return new StringBuilder()
        .append(OM_KEY_PREFIX).append(volumeId)
        .append(OM_KEY_PREFIX).append(bucketId)
        .append(OM_KEY_PREFIX).append(objectId)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SnapshotTrappedLedgerEntry that = (SnapshotTrappedLedgerEntry) o;
    return objectId == that.objectId
        && snapshotId.equals(that.snapshotId)
        && state == that.state;
  }

  @Override
  public int hashCode() {
    return Objects.hash(objectId, snapshotId, state);
  }

  /**
   * State machine values for trapped object ledger.
   */
  public enum State {
    ACCOUNTED_KEY,
    ACCOUNTED_DIR_ROOT,
    DIR_EXPAND_ACCOUNTED,
    PURGED;

    public SnapshotTrappedLedgerStateProto toProto() {
      switch (this) {
      case ACCOUNTED_KEY:
        return SnapshotTrappedLedgerStateProto.ACCOUNTED_KEY;
      case ACCOUNTED_DIR_ROOT:
        return SnapshotTrappedLedgerStateProto.ACCOUNTED_DIR_ROOT;
      case DIR_EXPAND_ACCOUNTED:
        return SnapshotTrappedLedgerStateProto.DIR_EXPAND_ACCOUNTED;
      case PURGED:
        return SnapshotTrappedLedgerStateProto.PURGED;
      default:
        throw new IllegalStateException("Unhandled ledger state: " + this);
      }
    }

    public static State fromProto(SnapshotTrappedLedgerStateProto proto) {
      switch (proto) {
      case ACCOUNTED_KEY:
        return ACCOUNTED_KEY;
      case ACCOUNTED_DIR_ROOT:
        return ACCOUNTED_DIR_ROOT;
      case DIR_EXPAND_ACCOUNTED:
        return DIR_EXPAND_ACCOUNTED;
      case PURGED:
        return PURGED;
      default:
        throw new IllegalArgumentException("Unsupported ledger state proto: " + proto);
      }
    }
  }
}
