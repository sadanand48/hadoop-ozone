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

package org.apache.hadoop.ozone.conf;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/**
 * Handler for ozone getconf storagecontainermanagers.
 */
@Command(name = "storagecontainermanagers",
    aliases = {"-storagecontainermanagers"},
    description = "gets list of ozone storage container "
        + "manager nodes in the cluster",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class StorageContainerManagersCommandHandler implements Callable<Void> {

  @ParentCommand
  private OzoneGetConf tool;

  @Override
  public Void call() throws Exception {
    Collection<InetSocketAddress> addresses = HddsUtils
        .getScmAddressForClients(OzoneConfiguration.of(tool.getConf()));

    for (InetSocketAddress addr : addresses) {
      tool.printOut(addr.getHostName());
    }
    return null;
  }
}
