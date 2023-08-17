package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.protocol.ECFilesystemCommon;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.client.OzoneClient;

import java.io.IOException;

public class ECFilesystemCommonOzone extends ECFilesystemCommon {
  ECFilesystemCommonOzone(OzoneClient client){
    this.ozoneClient = client;
  }

  OzoneClient ozoneClient;

  @Override
  public ErasureCodingPolicy getErasureCodingPolicy(FileStatus status) {
    OFSPath ofsPath =
        new OFSPath(status.getPath().toString(), new OzoneConfiguration());
    try {
      ECReplicationConfig replicationConfig =
          (ECReplicationConfig) ozoneClient.getObjectStore()
              .getVolume(ofsPath.getVolumeName())
              .getBucket(ofsPath.getBucketName()).getReplicationConfig();
      ECReplicationConfig.EcCodec ecCodec = replicationConfig.getCodec();
      int ecChunkSize1 = replicationConfig.getEcChunkSize();
      int ecChunkSize = ecChunkSize1;
      int data = replicationConfig.getData();
      int parity = replicationConfig.getParity();
      ECSchema ecSchema = new ECSchema(ecCodec.name(),data,parity);
      return new ErasureCodingPolicy(ecSchema,ecChunkSize);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setErasureCodingPolicy(FileSystem fs, Path path,
      String ecPolicyName) throws IOException {
    ErasureCodingPolicy ecPolicy =
        SystemErasureCodingPolicies.getByName(ecPolicyName);
    String ecRep = ecPolicy.getCodecName()
        .toLowerCase() + "-" + ecPolicy.getNumDataUnits() + "-"
        + ecPolicy.getNumParityUnits() + "-" + ecPolicy.getCellSize() + "k";
    OFSPath ofsPath = new OFSPath(path.toString(), new OzoneConfiguration());
    ozoneClient.getObjectStore().getVolume(ofsPath.getVolumeName())
        .getBucket(ofsPath.getBucketName())
        .setReplicationConfig(new ECReplicationConfig(ecRep));
  }

  @Override
  public FSDataOutputStream createECOutputStream(FileSystem fs, Path f,
      FsPermission permission, int bufferSize, short replication,
      long blockSize, Options.ChecksumOpt checksumOpt, String ecPolicyName)
      throws IOException {
    if (fs instanceof BasicRootedOzoneFileSystem){
      BasicRootedOzoneFileSystem ofs = (BasicRootedOzoneFileSystem) fs;
      ErasureCodingPolicy ecPolicy =
          SystemErasureCodingPolicies.getByName(ecPolicyName);
      ReplicationConfig replicationConfig = new ECReplicationConfig(ecPolicyName.toLowerCase());
      return ofs.create(f, permission, true, bufferSize, replication, blockSize,
          replicationConfig);
    }
    return super.createECOutputStream(fs, f, permission, bufferSize,
        replication, blockSize, checksumOpt, ecPolicyName);
  }
}
