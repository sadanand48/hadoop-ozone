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

package org.apache.hadoop.ozone.shell.volume;

import java.io.IOException;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.ClearSpaceQuotaOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * clear quota of the volume.
 */
@Command(name = "clrquota",
    description = "clear quota of the volume. At least one of the " +
        "quota clear flag is mandatory.")
public class ClearQuotaHandler extends VolumeHandler {

  @CommandLine.Mixin
  private ClearSpaceQuotaOptions clrSpaceQuota;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    String volumeName = address.getVolumeName();
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);
    boolean isOptionPresent = false;
    if (clrSpaceQuota.getClrSpaceQuota()) {
      volume.clearSpaceQuota();
      isOptionPresent = true;
    }
    if (clrSpaceQuota.getClrNamespaceQuota()) {
      volume.clearNamespaceQuota();
      isOptionPresent = true;
    }

    if (!isOptionPresent) {
      throw new IOException(
          "At least one of the quota clear flag is required.");
    }
  }
}
