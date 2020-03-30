package com.github.vincentrussell.query.mongodb.sql.converter.util;

import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.*;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;

public class DateObject {
    private final Function value;
    private final String column;
    private Expression comparisonExpression;

    public DateObject(String column, Function value, Expression expression)
        throws ParseException {
        this.column = column;
        this.value = value;
        comparisonExpression = expression;
    }

    public String getColumn() {
        return column;
    }

    public Object getValue(){
        String oprator = "$eq";
        if (EqualsTo.class.isInstance(comparisonExpression)) {
            oprator = "$eq";
        } else if (GreaterThan.class.isInstance(comparisonExpression)) {
            oprator = "$gt";
        }else if (GreaterThanEquals.class.isInstance(comparisonExpression)) {
            oprator = "$gte";
        } else if (MinorThan.class.isInstance(comparisonExpression)) {
            oprator = "$lt";
        } else {
            oprator = "$lte";
        }



            //        ByteBuffer.wrap(guid_bytesMost).getLong();

//        return new UUID(ByteBuffer.wrap(guid_bytesLeast).getLong(),ByteBuffer.wrap(guid_bytesMost).getLong());
       // return originalUUID;
//        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
//            try {
//                return   new Document(oprator, format.parse("2020-01-01T00:00:00Z"));
//            } catch (java.text.ParseException e) {
//                e.printStackTrace();
//                return new Document(oprator,new Date());
//            }

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        try {
            String dateInput = SqlUtils.trimQuatation(value.getParameters().getExpressions().get(0).toString());
            return new Document(oprator, format.parse(dateInput));
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            return new Document(oprator,new Date());
        }

            //      return UUID.nameUUIDFromBytes(uuid_bytes);


    }


}
