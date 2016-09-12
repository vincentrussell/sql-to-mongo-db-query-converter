package com.github.vincentrussell.query.mongodb.sql.converter;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class NonCloseableBufferedOutputStream extends BufferedOutputStream {

    public NonCloseableBufferedOutputStream(OutputStream out) {
        super(out);
    }

    public NonCloseableBufferedOutputStream(OutputStream out, int size) {
        super(out, size);
    }

    @Override
    public void close() throws IOException {
        try {
            flush();
        } catch (IOException ignored) {
            //noop
        }
    }
}