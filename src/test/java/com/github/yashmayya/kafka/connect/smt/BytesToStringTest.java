package com.github.yashmayya.kafka.connect.smt;

import static org.junit.Assert.assertEquals;

import com.github.yashmayya.kafka.connect.smt.BytesToString.Value;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BytesToStringTest {
    private final BytesToString<SourceRecord> transform = new Value<>();

    @Before
    public void setUp() {
        final Map<String, Object> props = new HashMap<>();
        props.put(BytesToString.FIELD_NAME, "name");
        transform.configure(props);
    }

    @After
    public void tearDown() {
        transform.close();
    }

    @Test
    public void testWithoutSchema() {
        String test = "Hello World!";
        final SourceRecord record = new SourceRecord(null, null, "topic", null,
                Collections.singletonMap("name", test.getBytes()));
        final SourceRecord transformedRecord = transform.apply(record);
        assertEquals(test, ((Map) transformedRecord.value()).get("name"));
    }

    @Test
    public void testWithSchema() {
        String test = "Hello World!";
        final Schema structSchema = SchemaBuilder.struct().name("testSchema").field("name", Schema.BYTES_SCHEMA).build();
        final Struct struct = new Struct(structSchema).put("name", test.getBytes());
        final SourceRecord record = new SourceRecord(null, null, "topic", structSchema, struct);
        final SourceRecord transformedRecord = transform.apply(record);
        assertEquals(test, ((Struct) transformedRecord.value()).getString("name"));
    }
}
