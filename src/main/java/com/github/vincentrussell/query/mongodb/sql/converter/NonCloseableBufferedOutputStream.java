package com.github.vincentrussell.query.mongodb.sql.converter;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Wrapper around an {@link OutputStream} to make sure that when you call close it only flushes the stream, but
 * does not close.
 */
public class NonCloseableBufferedOutputStream extends BufferedOutputStream {

    /**
     * Default constructor.
     * @param out the wrapped {@link OutputStream}
     */
    public NonCloseableBufferedOutputStream(final OutputStream out) {
        super(out);
    }


    /**
     * Does not close this output stream and releases any system resources associated with the stream.
     * Only calls flush on the output stream.
     * @exception  IOException  if an I/O error occurs.
     * @see        java.io.FilterOutputStream#flush()
     * @see        java.io.FilterOutputStream#out
     */
    @Override
    public void close() throws IOException {
        try {
            flush();
        } catch (IOException ignored) {
            //noop
        }
    }
}
