package com.xodb.catalog.repository;

import com.xodb.catalog.dto.TableInfoDto;

public interface TableInfoRepository {
  void saveOrUpdate(TableInfoDto tableInfoDto);

  TableInfoDto findByTableName(String tableName);

  void deleteByTableName(String tableName);
}
