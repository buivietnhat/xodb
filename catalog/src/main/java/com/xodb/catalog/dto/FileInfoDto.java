package com.xodb.catalog.dto;

public class FileInfoDto {
  String fileName;
  long fileSize;

  // information to find in S3
  String s3BucketName;
}
