package com.xodb.catalog.dto;

import lombok.Data;

import java.util.List;

@Data
public class TableInfoDto {
  private String name;
  private SchemaDto schema;
  private long tableObjId;
  private List<FileInfoDto> fileList;
}
