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
import org.apache.hadoop.hdds.client.*;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
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
import java.util.*;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
@RunWith(Parameterized.class)
public class TestDistcpWithOzoneFilesystem {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDistcpWithOzoneFilesystem.class);

  private static OzoneConfiguration conf;
  private static MiniOzoneCluster cluster = null;

  private static OzoneClient ozoneClient;
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
    ozoneClient = cluster.newClient();
    rootPath = String.format("%s://%s/", OzoneConsts.OZONE_OFS_URI_SCHEME,
        conf.get(OZONE_OM_ADDRESS_KEY));

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    fs = FileSystem.get(conf);
    metaDir = OMStorage.getOmDbDir(conf);
  }

  @AfterClass
  public static void shutdown() {
    IOUtils.closeQuietly(ozoneClient);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDistcpWithPreserveEC() throws Exception {

    // Case 1 :
    // Source: EC Key in an EC bucket
    // Target: bucket not created yet
    // preserve = true

    Path srcBucketPath = createAndGetECBucketPath();
    Path insideSrcBucket = new Path(srcBucketPath, "*");
    String dstVolName = "volume" + RandomStringUtils.randomNumeric(4);
    Path dstVolume = new Path("/",
        dstVolName);
    Path dstBucketPath = new Path(dstVolume,
        "bucket" +  RandomStringUtils.randomNumeric(4));
    List<String> keys;
    // create 2 files on source
    keys = createFiles(srcBucketPath, 2);
    // Create target volume
    fs.mkdirs(dstVolume);
    // perform normal distcp
    runDistcpAndVerifyCopy(insideSrcBucket, dstBucketPath, true);
    checkSourceAndTargetBucketReplicationConfig(srcBucketPath, dstBucketPath,
        true);
    checkSourceAndTargetKeyReplication(srcBucketPath, dstBucketPath,keys, true);

    // Case 2:
    // Source: EC Key in an EC bucket
    // Target: bucket not created yet
    // preserve = false

    srcBucketPath = createAndGetECBucketPath();
    insideSrcBucket = new Path(srcBucketPath, "*");
    dstVolName = "volume" + RandomStringUtils.randomNumeric(4);
    dstVolume = new Path("/", dstVolName);
    dstBucketPath = new Path(dstVolume,
        "bucket" +  RandomStringUtils.randomNumeric(4));
    // create 2 files on source
    keys = createFiles(srcBucketPath, 2);
    // Create target volume
    fs.mkdirs(dstVolume);
    // perform normal distcp
    runDistcpAndVerifyCopy(insideSrcBucket, dstBucketPath,false);
    checkSourceAndTargetBucketReplicationConfig(srcBucketPath, dstBucketPath,
        false);
    checkSourceAndTargetKeyReplication(srcBucketPath, dstBucketPath, keys,false);

    // Case 3:
    // Source: EC Key in an EC bucket
    // Target: EC bucket created
    // preserve = true

    srcBucketPath = createAndGetECBucketPath();
    insideSrcBucket = new Path(srcBucketPath, "*");
    dstVolName = "volume" + RandomStringUtils.randomNumeric(4);
    // create 2 files on source
    keys = createFiles(srcBucketPath, 2);
    // Create target bucket
    dstBucketPath = createAndGetBucketPath(dstVolName,
        "buck" + RandomStringUtils.randomNumeric(4),true);
    // perform normal distcp
    runDistcpAndVerifyCopy(insideSrcBucket, dstBucketPath,true);
    checkSourceAndTargetBucketReplicationConfig(srcBucketPath, dstBucketPath,
        true);
    checkSourceAndTargetKeyReplication(srcBucketPath, dstBucketPath, keys,true);


    // Case 4:
    // Source: EC Key in an EC bucket
    // Target: EC bucket created
    // preserve = false
    srcBucketPath = createAndGetECBucketPath();
    insideSrcBucket = new Path(srcBucketPath, "*");
    dstVolName = "volume" + RandomStringUtils.randomNumeric(4);
    // create 2 files on source
    keys = createFiles(srcBucketPath, 2);
    // Create target bucket
    dstBucketPath = createAndGetBucketPath(dstVolName,
        "buck" + RandomStringUtils.randomNumeric(4),true);
    // perform normal distcp
    runDistcpAndVerifyCopy(insideSrcBucket, dstBucketPath,false);
    checkSourceAndTargetBucketReplicationConfig(srcBucketPath, dstBucketPath,
        true);
    checkSourceAndTargetKeyReplication(srcBucketPath, dstBucketPath, keys,true);


    // Case 5:
    // Source: EC Key in an EC bucket
    // Target: Ratis bucket created
    // preserve = true
    srcBucketPath = createAndGetECBucketPath();
    insideSrcBucket = new Path(srcBucketPath, "*");
    dstVolName = "volume" + RandomStringUtils.randomNumeric(4);
    // create 2 files on source
    keys = createFiles(srcBucketPath, 2);
    // Create target bucket
    dstBucketPath = createAndGetBucketPath(dstVolName,
        "buck" + RandomStringUtils.randomNumeric(4),false);
    // perform normal distcp
    runDistcpAndVerifyCopy(insideSrcBucket, dstBucketPath, true);
    checkSourceAndTargetBucketReplicationConfig(srcBucketPath, dstBucketPath,
        false);
    checkSourceAndTargetKeyReplication(srcBucketPath, dstBucketPath, keys,
        true);


    // Case 6:
    // Source: EC Key in an EC bucket
    // Target: Ratis bucket created
    // preserve = false
    srcBucketPath = createAndGetECBucketPath();
    insideSrcBucket = new Path(srcBucketPath, "*");
    dstVolName = "volume" + RandomStringUtils.randomNumeric(4);
    // create 2 files on source
    keys = createFiles(srcBucketPath, 2);
    // Create target bucket
    dstBucketPath = createAndGetBucketPath(dstVolName,
        "buck" + RandomStringUtils.randomNumeric(4),false);
    // perform normal distcp
    runDistcpAndVerifyCopy(insideSrcBucket, dstBucketPath, false);
    checkSourceAndTargetBucketReplicationConfig(srcBucketPath, dstBucketPath,
        false);
    checkSourceAndTargetKeyReplication(srcBucketPath, dstBucketPath, keys,
        false);


    // Case 7:
    // Source: Ratis Key
    // Target: EC bucket created
    // preserve = false
    srcBucketPath = createAndGetBucketPath("srcvol" + RandomStringUtils.randomNumeric(4),
        "buck" + RandomStringUtils.randomNumeric(4),false);
    insideSrcBucket = new Path(srcBucketPath, "*");
    dstVolName = "volume" + RandomStringUtils.randomNumeric(4);
    // create 2 files on source
    keys = createFiles(srcBucketPath, 2);
    // Create target bucket
    dstBucketPath = createAndGetBucketPath(dstVolName,
        "buck" + RandomStringUtils.randomNumeric(4),true);
    // perform normal distcp
    runDistcpAndVerifyCopy(insideSrcBucket, dstBucketPath, false);
    checkSourceAndTargetBucketReplicationConfig(srcBucketPath, dstBucketPath,
        false);
    checkSourceAndTargetKeyReplication(srcBucketPath, dstBucketPath, keys,
        false);

    // Case 8
    // Source: Ratis Key
    // Target: EC bucket created
    // preserve = true
    srcBucketPath = createAndGetBucketPath("srcvol" + RandomStringUtils.randomNumeric(4),
        "buck" + RandomStringUtils.randomNumeric(4),false);
    insideSrcBucket = new Path(srcBucketPath, "*");
    dstVolName = "volume" + RandomStringUtils.randomNumeric(4);
    // create 2 files on source
    keys = createFiles(srcBucketPath, 2);
    // Create target bucket
    dstBucketPath = createAndGetBucketPath(dstVolName,
        "buck" + RandomStringUtils.randomNumeric(4),true);
    // perform normal distcp
    runDistcpAndVerifyCopy(insideSrcBucket, dstBucketPath, true);
    checkSourceAndTargetBucketReplicationConfig(srcBucketPath, dstBucketPath,
        false);
    checkSourceAndTargetKeyReplication(srcBucketPath, dstBucketPath, keys,
        false);

  }

  private static void checkSourceAndTargetBucketReplicationConfig(Path
      srcBucketPath, Path dstBucketPath, boolean shouldMatch)
      throws IOException {
    OFSPath destPath = new OFSPath(dstBucketPath, conf);
    OFSPath srcPath = new OFSPath(srcBucketPath, conf);
    OzoneBucket targetBucket =
        ozoneClient.getObjectStore().getVolume(destPath.getVolumeName())
            .getBucket(destPath.getBucketName());
    OzoneBucket sourceBucket =
        ozoneClient.getObjectStore().getVolume(srcPath.getVolumeName())
            .getBucket(srcPath.getBucketName());
    boolean match = sourceBucket.getReplicationConfig().equals(
        targetBucket.getReplicationConfig());
    Assert.assertEquals(shouldMatch, match);
  }

  private static void checkSourceAndTargetKeyReplication(Path srcBucketPath,
      Path dstBucketPath,List<String> keys, boolean shouldMatch) throws IOException {
    OFSPath destPath = new OFSPath(dstBucketPath, conf);
    OFSPath srcPath = new OFSPath(srcBucketPath, conf);
    OzoneBucket targetBucket =
        ozoneClient.getObjectStore().getVolume(destPath.getVolumeName())
            .getBucket(destPath.getBucketName());
    OzoneBucket sourceBucket =
        ozoneClient.getObjectStore().getVolume(srcPath.getVolumeName())
            .getBucket(srcPath.getBucketName());
    for (String key : keys){
      OzoneKey srcKey = sourceBucket.getKey(key);
      OzoneKey tgtKey = targetBucket.getKey(key);
      boolean match = srcKey.getReplicationConfig().equals(tgtKey.getReplicationConfig());
      Assert.assertEquals(shouldMatch,match);
    }
  }

  private static void runDistcpAndVerifyCopy(Path insideSrcBucket,
      Path dstBucketPath,boolean preserve)
      throws Exception {
    final DistCpOptions.Builder builder =
        new DistCpOptions.Builder(Collections.singletonList(insideSrcBucket),
            dstBucketPath);
    if (preserve) {
      builder.preserve(DistCpOptions.FileAttribute.ERASURECODINGPOLICY);
    }
    DistCpOptions options = builder.build();
    options.appendToConf(conf);
    Job distcpJob = new DistCp(conf, options).execute();
    verifyCopy(dstBucketPath, distcpJob, 2, 3,1);
  }

  private static void verifyCopy(Path dstBucketPath, Job distcpJob,
      long expectedFilesToBeCopied, long expectedTotalFilesInDest,
      long expectedDirsToBeCopied)
      throws IOException {
    long filesCopied =
        distcpJob.getCounters().findCounter(CopyMapper.Counter.COPY).getValue();
    long dirsCopied =
        distcpJob.getCounters().findCounter(CopyMapper.Counter.DIR_COPY).getValue();
    FileStatus[] destinationFileStatus = fs.listStatus(dstBucketPath);
    Assert.assertEquals(expectedTotalFilesInDest, destinationFileStatus.length);
    Assert.assertEquals(expectedFilesToBeCopied, filesCopied);
    Assert.assertEquals(expectedDirsToBeCopied,dirsCopied);
  }

  private static List<String> createFiles(Path srcBucketPath, int fileCount)
      throws IOException {
    List<String>  createdKeys = new ArrayList<>();
    for (int i = 1; i <= fileCount; i++) {
      String keyName = "key" + RandomStringUtils.randomNumeric(5);
      createdKeys.add(keyName);
      Path file =
          new Path(srcBucketPath, keyName);
      ContractTestUtils.touch(fs, file);
    }
    Path dir = new Path(srcBucketPath,"dir");
    fs.mkdirs(dir);
    createdKeys.add("dir/");
    return createdKeys;
  }
  private static Path createAndGetECBucketPath() throws IOException {
    String vol = UUID.randomUUID().toString();
    String buck = UUID.randomUUID().toString();
    return createAndGetBucketPath(vol,buck,true);
  }

  private static Path createAndGetRatisBucketPath() throws IOException {
    String vol = UUID.randomUUID().toString();
    String buck = UUID.randomUUID().toString();
    return createAndGetBucketPath(vol, buck, false);
  }

  private static Path createAndGetBucketPath(String vol, String buck,
      boolean ecBucket) throws IOException {
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.LEGACY);
    if (ecBucket) {
      builder.setDefaultReplicationConfig(new DefaultReplicationConfig(
          new ECReplicationConfig("RS-3-2-1024k")));
    } else {
      builder.setDefaultReplicationConfig(new DefaultReplicationConfig(
          ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
              ReplicationFactor.THREE)));
    }
    BucketArgs omBucketArgs = builder.build();
    final OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(cluster.newClient(), vol, buck,
            omBucketArgs);
    Path volumePath = new Path(OZONE_URI_DELIMITER, bucket.getVolumeName());
    return new Path(volumePath, bucket.getName());
  }
}
