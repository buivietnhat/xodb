package com.xodb.catalog.dto;

import lombok.Data;

@Data
public class FileInfoDto {
  String fileName;
  long fileSize;

  // information to find in S3
  String s3BucketName;
}
