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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.om.helpers.SnapshotTrappedLedgerEntry.State;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SnapshotTrappedLedgerEntry}.
 */
public class TestSnapshotTrappedLedgerEntry {

  @Test
  public void testCodecRoundTrip() throws Exception {
    Codec<SnapshotTrappedLedgerEntry> codec = SnapshotTrappedLedgerEntry.getCodec();
    SnapshotTrappedLedgerEntry entry = new SnapshotTrappedLedgerEntry(
        123L, UUID.randomUUID(), State.ACCOUNTED_KEY);

    byte[] persisted = codec.toPersistedFormat(entry);
    SnapshotTrappedLedgerEntry decoded = codec.fromPersistedFormat(persisted);

    assertEquals(entry, decoded);
  }

  @Test
  public void testEncodeKey() {
    String key = SnapshotTrappedLedgerEntry.encodeKey(10L, 20L, 30L);
    assertEquals("/10/20/30", key);
  }

  @Test
  public void testStateProtoConversion() {
    for (State state : State.values()) {
      assertEquals(state, State.fromProto(state.toProto()));
    }
  }
}
