package com.ledger.clone.core.disruptor;

import com.lmax.disruptor.WaitStrategy;
import lombok.Getter;

@Getter
public class LedgerDisruptorConfig {

    public static final int DEFAULT_RING_BUFFER_SIZE = 1024;

    public enum Strategy {
        BUSY_SPIN,
        YIELDING,
        SLEEPING,
        BLOCKING
    }

    private final int ringBufferSize;
    private final Strategy waitStrategy;

    public LedgerDisruptorConfig() {
        this(DEFAULT_RING_BUFFER_SIZE, Strategy.YIELDING);
    }

    public LedgerDisruptorConfig(int ringBufferSize, Strategy waitStrategy) {
        if (Integer.bitCount(ringBufferSize) != 1) {
            throw new IllegalArgumentException("Ring buffer size must be a power of 2, got " + ringBufferSize);
        }
        this.ringBufferSize = ringBufferSize;
        this.waitStrategy = waitStrategy;
    }

    public WaitStrategy toLmaxWaitStrategy() {
        return switch (waitStrategy) {
            case BUSY_SPIN -> new com.lmax.disruptor.BusySpinWaitStrategy();
            case YIELDING -> new com.lmax.disruptor.YieldingWaitStrategy();
            case SLEEPING -> new com.lmax.disruptor.SleepingWaitStrategy();
            case BLOCKING -> new com.lmax.disruptor.BlockingWaitStrategy();
        };
    }
}
