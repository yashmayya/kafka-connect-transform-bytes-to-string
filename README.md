# kafka-connect-transform-bytes-to-string
A Kafka Connect Single Message Transform (SMT) to convert a byte array field (encoded in UTF_8) to a String. For records with
schemas, this will also update the schema for the specified field from BYTES to STRING

This SMT supports converting fields in the record Key or Value

Properties:

|Name|Description|Type|Importance|
|---|---|---|---|
|`field.name`| Field name to decode | String| High |

Example configs:

```
transforms=decode
transforms.decode.type=com.github.yashmayya.kafka.connect.smt.BytesToString$Value
transforms.decode.field.name="name"
```

----------
### How to use this

- Run `mvn clean package` in the repo's root directory
- Copy the created jar from the /target directory to some directory, say `kafka-connect-transform-bytes-to-string`, in your Connect worker's plugin path
- Create a connector using this transform in its properties!
