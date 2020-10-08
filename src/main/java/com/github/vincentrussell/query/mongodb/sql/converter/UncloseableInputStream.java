package com.github.vincentrussell.query.mongodb.sql.converter;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 *  An {@link InputStream} which cannot be closed.
 */
public class UncloseableInputStream extends FilterInputStream {

    /**
     * Default constructor.
     * @param in the wrapped {@link InputStream}
     */
    public UncloseableInputStream(final InputStream in) {
        super(in);
    }

    /**
     * This method does not has any effect the {@link InputStream}
     * cannot be closed.
     */
    @Override
    public void close() throws IOException {
        //noop
    }
}
