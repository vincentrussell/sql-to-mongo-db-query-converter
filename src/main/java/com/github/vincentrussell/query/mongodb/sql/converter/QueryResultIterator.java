package com.github.vincentrussell.query.mongodb.sql.converter;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.calrissian.mango.collect.AbstractCloseableIterator;

import java.io.IOException;

public class QueryResultIterator<T> extends AbstractCloseableIterator<T> {

    private final MongoIterable<T> mongoIterable;
    private final MongoCursor<T> mongoCursor;

    public QueryResultIterator(MongoIterable<T> mongoIterable) {
        this.mongoIterable = mongoIterable;
        this.mongoCursor = mongoIterable.iterator();
    }

    public QueryResultIterator(long count) {
        this.mongoIterable = null;
        this.mongoCursor = new LongMongoCursor<>(count);
    }

    @Override
    protected T computeNext() {
        if (mongoCursor.hasNext()) {
            return mongoCursor.next();
        }
        if (mongoCursor!=null) {
            mongoCursor.close();
        }
        return endOfData();
    }

    @Override
    public void close() throws IOException {
        if (mongoCursor!=null) {
            mongoCursor.close();
        }
    }

    private static class LongMongoCursor<T> extends AbstractCloseableIterator<T> implements MongoCursor<T> {
        private final Long count;
        private boolean returned = false;

        private LongMongoCursor(Long count) {
            this.count = count;
        }

        @Override
        protected T computeNext() {
            if (!returned) {
                returned = true;
                return (T)count;
            }
            return endOfData();
        }

        @Override
        public T tryNext() {
            return null;
        }

        @Override
        public ServerCursor getServerCursor() {
            return null;
        }

        @Override
        public ServerAddress getServerAddress() {
            return null;
        }

        @Override
        public void close() {

        }
    }
}
