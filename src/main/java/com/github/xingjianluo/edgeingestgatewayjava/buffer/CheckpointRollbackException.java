package com.github.xingjianluo.edgeingestgatewayjava.buffer;

/**
 * 位点回退被明确拒绝时抛出。
 */
public class CheckpointRollbackException extends RuntimeException {

    private final long currentFrontier;
    private final long requestedSeq;

    public CheckpointRollbackException(long currentFrontier, long requestedSeq) {
        super("checkpoint rollback rejected: current frontier=" + currentFrontier
                + ", requested=" + requestedSeq);
        this.currentFrontier = currentFrontier;
        this.requestedSeq = requestedSeq;
    }

    public long currentFrontier() {
        return currentFrontier;
    }

    public long requestedSeq() {
        return requestedSeq;
    }
}
