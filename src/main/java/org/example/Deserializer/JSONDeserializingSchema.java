package org.example.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.example.dto.Transaction;

import java.io.IOException;

public class JSONDeserializingSchema implements DeserializationSchema<Transaction> {

    private final ObjectMapper mapper = new ObjectMapper();
    @Override
    public Transaction deserialize(byte[] bytes) throws IOException {
        return mapper.readValue(bytes,Transaction.class);
    }

    @Override
    public boolean isEndOfStream(Transaction transaction) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}
