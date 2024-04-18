package com.xodb.catalog.repository.impl;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class TableInfoRepositoryImplTest {
  @Autowired
  private TableInfoRepositoryImpl tableInfoRepository;

  @Test
  void getTableInfoTest() {
    System.out.println("hehe");
  }
}