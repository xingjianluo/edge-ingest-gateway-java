package com.github.xingjianluo.edgeingestgatewayjava.resume;

/**
 * 位点非法（回退/损坏）时抛出。
 */
public class OffsetStoreException extends RuntimeException {
    public OffsetStoreException(String message) {
        super(message);
    }
}
