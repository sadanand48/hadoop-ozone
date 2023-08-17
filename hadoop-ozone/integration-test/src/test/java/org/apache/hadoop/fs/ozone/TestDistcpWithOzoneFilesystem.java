/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.ozone;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.mapred.CopyMapper;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
@RunWith(Parameterized.class)
public class TestDistcpWithOzoneFilesystem {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDistcpWithOzoneFilesystem.class);

  private static OzoneConfiguration conf;
  private static MiniOzoneCluster cluster = null;
  private static FileSystem fs;
  private static BucketLayout bucketLayout;
  private static String rootPath;
  private static boolean enableRatis;

  private static File metaDir;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[] {true, BucketLayout.FILE_SYSTEM_OPTIMIZED},
        new Object[] {false, BucketLayout.LEGACY});
  }


  public TestDistcpWithOzoneFilesystem(boolean enableRatis,
      BucketLayout bucketLayout) {
    // do nothing
  }

  @Parameterized.BeforeParam
  public static void initParam(boolean ratisEnable, BucketLayout layout)
      throws IOException, InterruptedException, TimeoutException {
    // Initialize the cluster before EACH set of parameters
    enableRatis = ratisEnable;
    bucketLayout = layout;
    initClusterAndEnv();
  }

  @Parameterized.AfterParam
  public static void teardownParam() {
    // Tear down the cluster after EACH set of parameters
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  public static void initClusterAndEnv()
      throws IOException, InterruptedException, TimeoutException {
    conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, enableRatis);
    bucketLayout = BucketLayout.FILE_SYSTEM_OPTIMIZED;
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT, bucketLayout.name());
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();

    rootPath = String.format("%s://%s/", OzoneConsts.OZONE_OFS_URI_SCHEME,
        conf.get(OZONE_OM_ADDRESS_KEY));

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    fs = FileSystem.get(conf);
    metaDir = OMStorage.getOmDbDir(conf);
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDistcpWithPreserveEC() throws Exception {
    Path srcBucketPath = createAndGetBucketPath();
    Path insideSrcBucket = new Path(srcBucketPath, "*");
    Path dstBucketPath;
    Path dstVolume = new Path("/", "volume" + RandomStringUtils.randomNumeric(4));
    dstBucketPath = new Path(dstVolume,"bucket" +  RandomStringUtils.randomNumeric(4));
    // create 2 files on source

    createFiles(srcBucketPath, 2);
    // Create target directory/bucket
    fs.mkdirs(dstVolume);

    // perform normal distcp
    final DistCpOptions options =
        new DistCpOptions.Builder(Collections.singletonList(insideSrcBucket),
            dstBucketPath).preserve(
            DistCpOptions.FileAttribute.ERASURECODINGPOLICY).build();
    options.appendToConf(conf);
    Job distcpJob = new DistCp(conf, options).execute();
    verifyCopy(dstBucketPath, distcpJob, 2, 2);
    OFSPath destPath = new OFSPath(dstBucketPath, conf);
    OFSPath srcPath = new OFSPath(srcBucketPath, conf);
    OzoneBucket targetBucket =
        cluster.newClient().getObjectStore().getVolume(destPath.getVolumeName())
            .getBucket(destPath.getBucketName());
    OzoneBucket sourceBucket =
        cluster.newClient().getObjectStore().getVolume(srcPath.getVolumeName())
            .getBucket(srcPath.getBucketName());
    Assert.assertEquals(sourceBucket.getReplicationConfig(),
        targetBucket.getReplicationConfig());

  }

  private static void verifyCopy(Path dstBucketPath, Job distcpJob,
      long expectedFilesToBeCopied, long expectedTotalFilesInDest)
      throws IOException {
    long filesCopied =
        distcpJob.getCounters().findCounter(CopyMapper.Counter.COPY).getValue();
    FileStatus[] destinationFileStatus = fs.listStatus(dstBucketPath);
    Assert.assertEquals(expectedTotalFilesInDest, destinationFileStatus.length);
    Assert.assertEquals(expectedFilesToBeCopied, filesCopied);
  }

  private static void createFiles(Path srcBucketPath, int fileCount)
      throws IOException {
    for (int i = 1; i <= fileCount; i++) {
      String keyName = "key" + RandomStringUtils.randomNumeric(5);
      Path file =
          new Path(srcBucketPath, keyName);
      ContractTestUtils.touch(fs, file);
    }
  }
  private static Path createAndGetBucketPath() throws IOException {
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.LEGACY);
    builder.setDefaultReplicationConfig(
        new DefaultReplicationConfig(
            new ECReplicationConfig("RS-3-2-1024k")));
    BucketArgs omBucketArgs = builder.build();
    String vol = UUID.randomUUID().toString();
    String buck = UUID.randomUUID().toString();
    final OzoneBucket bucket = TestDataUtil
        .createVolumeAndBucket(cluster.newClient(), vol, buck,
            omBucketArgs);
    Path volumePath =
        new Path(OZONE_URI_DELIMITER, bucket.getVolumeName());
    Path bucketPath = new Path(volumePath, bucket.getName());
    return bucketPath;
  }
}
