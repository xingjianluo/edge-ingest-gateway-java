package com.github.xingjianluo.edgeingestgatewayjava.buffer;

/**
 * 缓冲写入被明确拒绝（写满/背压）时抛出，携带稳定原因码。
 */
public class BufferRejectException extends RuntimeException {

    private final String code;

    public BufferRejectException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
