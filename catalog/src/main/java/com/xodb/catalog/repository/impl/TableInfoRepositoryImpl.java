package com.xodb.catalog.repository.impl;

import com.xodb.catalog.dto.TableInfoDto;
import com.xodb.catalog.exception.DBException;
import com.xodb.catalog.exception.ResourceNotFoundException;
import com.xodb.catalog.repository.TableInfoRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Repository;
import org.springframework.beans.factory.annotation.Value;

@Repository
public class TableInfoRepositoryImpl implements TableInfoRepository {
  @Value("${db.rockdb.path}")
  private String rockDBPath;

  private RocksDB db;
  private Options options;

  @PostConstruct
  private void initialize() {
    RocksDB.loadLibrary();
    options = new Options().setCreateIfMissing(true);
    try {
      String dataPath = this.getClass().getClassLoader().getResource("").getPath() + rockDBPath;
      db = RocksDB.open(options, dataPath);
    } catch (RocksDBException e) {
      throw new DBException("cannot open RockDB with path " + rockDBPath + " + with error " + e.getMessage());
    }
  }

  @PreDestroy
  private void cleanUp() {
    if (db != null) {
      db.close();
    }

    if (options != null) {
      options.close();
    }
  }

  @Override
  public void saveOrUpdate(TableInfoDto tableInfoDto) {
//    TableInfo.newBuilder().setSchema(tableInfoDto.getSchema())
//        .setName(tableInfoDto.getName())
//        .setFileInfos(tableInfoDto.getFileList());
//    db.put(tableInfoDto.getName().getBytes(), ta)
  }

  @Override
  public TableInfoDto findByTableName(String tableName) {
    return null;
  }

  @Override
  public void deleteByTableName(String tableName) {

  }
}
