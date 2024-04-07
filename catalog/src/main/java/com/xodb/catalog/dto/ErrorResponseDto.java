package com.xodb.catalog.dto;

import lombok.Data;
import org.springframework.http.HttpStatus;

import java.time.LocalDateTime;

@Data
public class ErrorResponseDto {
  private String apiPath;
  private HttpStatus errorCode;
  private String errorMessage;
  private LocalDateTime timestamp;

  public ErrorResponseDto(String apiPath, HttpStatus errorCode, String errorMessage, LocalDateTime timestamp) {
    this.apiPath = apiPath;
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
    this.timestamp = timestamp;
  }
}
