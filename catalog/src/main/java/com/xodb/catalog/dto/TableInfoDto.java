package com.xodb.catalog.dto;

import java.util.List;

public class TableInfoDto {
  String name;
  private SchemaDto schema;
  long tableObjId;
  List<FileInfoDto> fileList;
}
