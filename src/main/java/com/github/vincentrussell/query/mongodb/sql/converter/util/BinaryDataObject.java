package com.github.vincentrussell.query.mongodb.sql.converter.util;

import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.google.common.collect.Lists;
import edu.emory.mathcs.backport.java.util.Arrays;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.BinaryCodec;
import org.apache.commons.lang.StringUtils;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BinaryDataObject {
    private final Function value;
    private final String column;
    private Expression comparisonExpression;

    public BinaryDataObject(String column, Function value, Expression expression)
        throws ParseException {
        this.column = column;
        this.value = value;
        this.comparisonExpression = expression;
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

        int paramcount = value.getParameters().getExpressions().size();
        byte[] guid_bytesLeast = getBytesFromGUID(value.getParameters().getExpressions().get(0).toString());
        BsonBinary binary = new BsonBinary(new Byte("3"), guid_bytesLeast);
        return new Document(oprator, binary);
    }

    private byte[] getBytesFromGUID(String str) {

        UUID originalUUID = UUID.fromString(SqlUtils.trimQuatation(str));
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(originalUUID.getMostSignificantBits());
        bb.putLong(originalUUID.getLeastSignificantBits());
        byte[] uuid_bytes = bb.array();
        byte[] guid_bytes = Arrays.copyOf(uuid_bytes,16);
        guid_bytes[0] = uuid_bytes[3];
        guid_bytes[1] = uuid_bytes[2];
        guid_bytes[2] = uuid_bytes[1];
        guid_bytes[3] = uuid_bytes[0];
        guid_bytes[4] = uuid_bytes[5];
        guid_bytes[5] = uuid_bytes[4];
        guid_bytes[6] = uuid_bytes[7];
        guid_bytes[7] = uuid_bytes[6];
        return guid_bytes;
    }

}
