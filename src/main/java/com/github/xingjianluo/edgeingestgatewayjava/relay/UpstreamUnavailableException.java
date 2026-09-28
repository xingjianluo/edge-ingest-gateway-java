package com.github.xingjianluo.edgeingestgatewayjava.relay;

/**
 * 上游不可用（网络中断）。
 */
public class UpstreamUnavailableException extends RuntimeException {

    public UpstreamUnavailableException(String message) {
        super(message);
    }

    public UpstreamUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
