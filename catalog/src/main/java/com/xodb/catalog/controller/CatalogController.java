package com.xodb.catalog.controller;

import com.xodb.catalog.dto.TableInfoDto;
import com.xodb.catalog.service.CatalogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CatalogController {
  private final CatalogService catalogService;

  @Autowired
  public CatalogController(CatalogService catalogService) {
    this.catalogService = catalogService;
  }

  @GetMapping("/table-info")
  ResponseEntity<TableInfoDto> getTableInfo(@RequestParam String name) {
    return ResponseEntity.ok(catalogService.getTableInfo(name));
  }
}
