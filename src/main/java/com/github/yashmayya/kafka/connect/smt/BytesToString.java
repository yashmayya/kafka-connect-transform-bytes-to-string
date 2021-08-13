package com.github.yashmayya.kafka.connect.smt;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class BytesToString<R extends ConnectRecord<R>> implements Transformation<R> {
    static String FIELD_NAME = "field.name";
    private static final String PURPOSE = "Decode a base64 encoded field";
    static final ConfigDef CONFIG_DEF = new ConfigDef().define(FIELD_NAME, Type.STRING, Importance.HIGH, PURPOSE);
    private String fieldName;
    private final Map<Schema, Schema> schemaLookup = new HashMap<>();

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(FIELD_NAME);
    }

    @Override
    public R apply(R record) {
        if (actualSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(actualValue(record), PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value);
        updatedValue.put(fieldName, new String((byte[]) value.get(fieldName), StandardCharsets.UTF_8));
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(actualValue(record), PURPOSE);
        final Schema updatedSchema = schemaLookup.computeIfAbsent(record.valueSchema(), inputSchema -> {
            SchemaBuilder builder = SchemaBuilder.struct();
            if (null != inputSchema.name()) {
                builder.name(inputSchema.name());
            }
            if (null != inputSchema.doc()) {
                builder.doc(inputSchema.doc());
            }
            if (inputSchema.isOptional()) {
                builder.optional();
            }
            if (null != inputSchema.version()) {
                builder.version(inputSchema.version() + 1);
            }
            for (Field field : inputSchema.fields()) {
                if (field.name().equals(fieldName)) {
                    builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
                } else {
                    builder.field(field.name(), field.schema());
                }
            }
            return builder.build();
        });
        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            if (field.name().equals(fieldName)) {
                updatedValue.put(fieldName, new String((byte[]) value.get(fieldName), StandardCharsets.UTF_8));
            } else {
                updatedValue.put(field.name(), value.get(field));
            }
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    protected abstract Schema actualSchema(R record);
    protected abstract Object actualValue(R record);
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends BytesToString<R> {

        @Override
        protected Schema actualSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object actualValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedKey) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedKey, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends BytesToString<R> {

        @Override
        protected Schema actualSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object actualValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
