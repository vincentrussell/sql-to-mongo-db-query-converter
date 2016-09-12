package com.github.vincentrussell.query.mongodb.sql.converter;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.*;

public class TimeoutInputStream extends InputStream {

    private static ExecutorService EXECUTOR = Executors.newFixedThreadPool(1);

    private final InputStream inputStream;
    private final long timeout;
    private final TimeUnit timeUnit;
    private boolean receivedData = false;


    public TimeoutInputStream(InputStream inputStream, long timeout, TimeUnit timeUnit) {
        this.inputStream = inputStream;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    @Override
    public int read() throws IOException {

        int result = -1;
        Future<Integer> future = EXECUTOR.submit(new ReadNext(inputStream));
        try {
            result = getInteger(future);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            result = -1;
        }

        receivedData = true;
        return result;
    }

    private Integer getInteger(Future<Integer> future) throws InterruptedException, ExecutionException, TimeoutException {
        if (!receivedData) {
            return future.get(1, TimeUnit.MINUTES);
        } else {
            return future.get(timeout, timeUnit);
        }
    }

    @Override
    public void close() throws IOException {
        //noop
    }

    private static class ReadNext implements Callable<Integer> {

        private final InputStream inputStream;

        private ReadNext(InputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public Integer call() throws Exception {
            return inputStream.read();
        }
    }
}