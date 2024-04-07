package com.xodb.catalog.repository.impl;

import com.xodb.catalog.dto.TableInfoDto;
import com.xodb.catalog.repository.TableInfoRepository;

public class TableInfoRepositoryImpl implements TableInfoRepository {
  @Override
  public void saveOrUpdate(TableInfoDto tableInfoDto) {

  }

  @Override
  public TableInfoDto findByTableName(String tableName) {
    return null;
  }

  @Override
  public void deleteByTableName(String tableName) {

  }
}
