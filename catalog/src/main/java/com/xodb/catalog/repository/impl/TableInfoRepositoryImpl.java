package com.xodb.catalog.repository.impl;

import com.xodb.catalog.dto.TableInfoDto;
import com.xodb.catalog.repository.TableInfoRepository;
import jakarta.annotation.PostConstruct;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Repository;
import org.springframework.beans.factory.annotation.Value;

@Repository
public class TableInfoRepositoryImpl implements TableInfoRepository {
  @Value("${db.rockdb.path}")
  private String rockDBPath;

  @PostConstruct
  public void loadRockDb() {
    RocksDB.loadLibrary();

    try (final Options options = new Options().setCreateIfMissing(true)) {

      // a factory method that returns a RocksDB instance
      try (final RocksDB db = RocksDB.open(options, rockDBPath)) {

        // do something
      }
    } catch (RocksDBException e) {
      // do some error handling
    }
  }

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
