package com.xodb.catalog.repository.impl;

import com.xodb.catalog.TableInfo;
import com.xodb.catalog.XodbCatalogProtos;
import com.xodb.catalog.dto.TableInfoDto;
import com.xodb.catalog.exception.DBException;
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

  private RocksDB db;

  @PostConstruct
  public void loadRockDb() {
    RocksDB.loadLibrary();

    try (final Options options = new Options().setCreateIfMissing(true)) {
      // a factory method that returns a RocksDB instance
      db = RocksDB.open(options, rockDBPath);
    } catch (RocksDBException e) {
      // do some error handling
      db.close();
      throw new DBException("cannot open RockDB with path " + rockDBPath + " + with error " + e.getMessage());
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
