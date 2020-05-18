package com.github.vincentrussell.query.mongodb.sql.converter;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.calrissian.mango.collect.AbstractCloseableIterator;

import java.io.IOException;

public class QueryResultIterator<T> extends AbstractCloseableIterator<T> {

    private final MongoCursor<T> mongoCursor;

    public QueryResultIterator(MongoIterable<T> mongoIterable) {
        this.mongoCursor = mongoIterable.iterator();
    }

    @Override
    protected T computeNext() {
        if (mongoCursor.hasNext()) {
            return mongoCursor.next();
        } else {
            mongoCursor.close();
        }
        return endOfData();
    }

    @Override
    public void close() throws IOException {
        mongoCursor.close();
    }
}
