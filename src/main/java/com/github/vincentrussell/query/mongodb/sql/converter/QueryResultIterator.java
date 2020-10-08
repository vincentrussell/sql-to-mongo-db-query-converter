package com.github.vincentrussell.query.mongodb.sql.converter;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.calrissian.mango.collect.AbstractCloseableIterator;

import java.io.IOException;

/**
 * Wrapper {@link java.util.Iterator} around the {@link MongoCursor}.
 * @param <T> the type of elements returned by this iterator
 */
public class QueryResultIterator<T> extends AbstractCloseableIterator<T> {

    private final MongoCursor<T> mongoCursor;

    /**
     * Default constructor.
     * @param mongoIterable the wrapped {@link MongoIterable}
     */
    public QueryResultIterator(final MongoIterable<T> mongoIterable) {
        this.mongoCursor = mongoIterable.iterator();
    }

    /**
     * {@inheritDoc}
     * @return the next element if there was one. If {@code endOfData} was called during execution,
     * the return value will be ignored.
     */
    @Override
    protected T computeNext() {
        if (mongoCursor.hasNext()) {
            return mongoCursor.next();
        } else {
            mongoCursor.close();
        }
        return endOfData();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        mongoCursor.close();
    }
}
