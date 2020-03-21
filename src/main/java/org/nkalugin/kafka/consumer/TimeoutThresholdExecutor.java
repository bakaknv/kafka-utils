package org.nkalugin.kafka.consumer;


import java.util.function.Supplier;


/**
 * @author nkalugin on 22.05.18.
 */
public class TimeoutThresholdExecutor
{
    private long lastExecutionTimestamp = 0;

    private final Supplier<? extends Number> timeoutSupplier;


    public TimeoutThresholdExecutor(Supplier<? extends Number> timeoutSupplier)
    {
        this.timeoutSupplier = timeoutSupplier;
    }


    private boolean canExecute()
    {
        return System.currentTimeMillis() - lastExecutionTimestamp >= timeoutSupplier.get().longValue();
    }


    private void updateLastCommitTimestamp()
    {
        this.lastExecutionTimestamp = System.currentTimeMillis();
    }


    public void doWithTimedThreshold(Runnable runnable)
    {
        if (canExecute())
        {
            runnable.run();
            updateLastCommitTimestamp();
        }
    }
}
