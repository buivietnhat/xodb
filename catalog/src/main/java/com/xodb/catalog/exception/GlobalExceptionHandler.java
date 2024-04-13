package com.xodb.catalog.exception;

import com.xodb.catalog.dto.ErrorResponseDto;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.time.LocalDateTime;

@ControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {
  @ExceptionHandler(ResourceNotFoundException.class)
  public ResponseEntity<Object> handleResourceNotFound(ResourceNotFoundException ex, WebRequest request) {
    ErrorResponseDto errorResponseDto = new ErrorResponseDto(
        request.getDescription(false),
        HttpStatus.NOT_FOUND,
        ex.getMessage(),
        LocalDateTime.now()
    );

    return new ResponseEntity<>(errorResponseDto, HttpStatus.NOT_FOUND);
  }

  @ExceptionHandler(DBException.class)
  public ResponseEntity<Object> handleDBException(DBException ex, WebRequest request) {
    ErrorResponseDto errorResponseDto = new ErrorResponseDto(
        request.getDescription(false),
        HttpStatus.INTERNAL_SERVER_ERROR,
        ex.getMessage(),
        LocalDateTime.now()
    );

    return new ResponseEntity<>(errorResponseDto, HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ErrorResponseDto> handleGlobalException(Exception exception,
                                                                WebRequest webRequest) {
    ErrorResponseDto errorResponseDTO = new ErrorResponseDto(
        webRequest.getDescription(false),
        HttpStatus.INTERNAL_SERVER_ERROR,
        exception.getMessage(),
        LocalDateTime.now()
    );
    return new ResponseEntity<>(errorResponseDTO, HttpStatus.INTERNAL_SERVER_ERROR);
  }
}
