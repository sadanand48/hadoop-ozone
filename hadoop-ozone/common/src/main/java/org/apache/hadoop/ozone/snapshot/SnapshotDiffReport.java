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

package org.apache.hadoop.ozone.snapshot;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotDiffReportProto;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * Snapshot diff report.
 */
public class SnapshotDiffReport
    extends org.apache.hadoop.hdfs.protocol.SnapshotDiffReport {

  private static final String LINE_SEPARATOR = System.getProperty(
      "line.separator", "\n");

  /**
   * Volume name to which the snapshot bucket belongs.
   */
  private final String volumeName;

  /**
   * Bucket name to which the snapshot belongs.
   */
  private final String bucketName;

  /**
   * list of diff.
   */
  private final List<DiffReportEntry> diffList;

  /**
   * subsequent token for the diff report.
   */
  private final String token;


  public SnapshotDiffReport(final String snapshotRoot,
      final String volumeName,
      final String bucketName,
      final String fromSnapshot,
      final String toSnapshot,
      final List<DiffReportEntry> entryList, final String token) {
    super(snapshotRoot, fromSnapshot, toSnapshot, entryList);
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.diffList = entryList != null ? entryList : Collections.emptyList();
    this.token = token;
  }

  public List<DiffReportEntry> getDiffList() {
    return diffList;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append("Difference between snapshot: ")
        .append(getFromSnapshot())
        .append(" and snapshot: ")
        .append(getLaterSnapshotName())
        .append(LINE_SEPARATOR);
    for (DiffReportEntry entry : diffList) {
      str.append(entry.toString()).append(LINE_SEPARATOR);
    }
    if (StringUtils.isNotEmpty(token)) {
      str.append("Next token: ")
          .append(token)
          .append(LINE_SEPARATOR);
    }
    return str.toString();
  }

  public SnapshotDiffReportProto toProtobuf() {
    final SnapshotDiffReportProto.Builder builder = SnapshotDiffReportProto
        .newBuilder();
    builder.setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setFromSnapshot(getFromSnapshot())
        .setToSnapshot(getLaterSnapshotName());
    builder.addAllDiffList(diffList.stream().map(
            SnapshotDiffReport::toProtobufDiffReportEntry)
        .collect(Collectors.toList()));
    if (StringUtils.isNotEmpty(token)) {
      builder.setToken(token);
    }
    return builder.build();
  }

  public static SnapshotDiffReport fromProtobuf(
      final SnapshotDiffReportProto report) {
    Path bucketPath = new Path(
        OZONE_URI_DELIMITER + report.getVolumeName()
            + OZONE_URI_DELIMITER + report.getBucketName());
    OFSPath path = new OFSPath(bucketPath, new OzoneConfiguration());
    return new SnapshotDiffReport(path.toString(),
        report.getVolumeName(),
        report.getBucketName(),
        report.getFromSnapshot(),
        report.getToSnapshot(),
        report.getDiffListList().stream()
            .map(SnapshotDiffReport::fromProtobufDiffReportEntry)
            .collect(Collectors.toList()),
        report.getToken());
  }

  public static DiffType fromProtobufDiffType(
      final OzoneManagerProtocolProtos.DiffReportEntryProto
          .DiffTypeProto type) {
    return DiffType.valueOf(type.name());
  }

  public static OzoneManagerProtocolProtos.DiffReportEntryProto
      .DiffTypeProto toProtobufDiffType(DiffType type) {
    return OzoneManagerProtocolProtos.DiffReportEntryProto
        .DiffTypeProto.valueOf(type.name());
  }

  public static DiffReportEntry fromProtobufDiffReportEntry(
      final OzoneManagerProtocolProtos.DiffReportEntryProto entry) {
    if (entry == null) {
      return null;
    }
    DiffType type = fromProtobufDiffType(entry.getDiffType());
    return type == null ? null :
        new DiffReportEntry(type, entry.getSourcePath().getBytes(),
            entry.hasTargetPath() ? entry.getTargetPath().getBytes() : null);
  }

  public static OzoneManagerProtocolProtos
      .DiffReportEntryProto toProtobufDiffReportEntry(DiffReportEntry entry) {
    final OzoneManagerProtocolProtos.DiffReportEntryProto.Builder builder =
        OzoneManagerProtocolProtos.DiffReportEntryProto.newBuilder();
    builder.setDiffType(toProtobufDiffType(entry.getType()))
        .setSourcePath(new String(entry.getSourcePath()));
    if (entry.getTargetPath() != null) {
      String targetPath = new String(entry.getTargetPath());
      builder.setTargetPath(targetPath);
    }
    return builder.build();
  }


  public static DiffReportEntry getDiffReportEntry(final DiffType type,
      final String sourcePath) {
    return getDiffReportEntry(type, sourcePath, null);
  }

  public static DiffReportEntry getDiffReportEntry(final DiffType type,
      final String sourcePath, final String targetPath) {
    return new DiffReportEntry(type,
        sourcePath.getBytes(StandardCharsets.UTF_8),
        targetPath != null ? targetPath.getBytes(StandardCharsets.UTF_8) :
            null);
  }


}
