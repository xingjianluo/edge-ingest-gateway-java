package com.github.xingjianluo.edgeingestgatewayjava.web;

import com.github.xingjianluo.edgeingestgatewayjava.buffer.CheckpointRollbackException;
import com.github.xingjianluo.edgeingestgatewayjava.model.IngestException;
import com.github.xingjianluo.edgeingestgatewayjava.model.IngestResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 统一异常 -> 可区分的拒绝结论。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(IngestException.class)
    public ResponseEntity<IngestResult> handleIngest(IngestException ex) {
        return ResponseEntity.badRequest()
                .body(IngestResult.rejected(ex.reason(), ex.getMessage()));
    }

    @ExceptionHandler(CheckpointRollbackException.class)
    public ResponseEntity<IngestResult> handleRollback(CheckpointRollbackException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(IngestResult.rejected(null, ex.getMessage()));
    }
}
