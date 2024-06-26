package com.quangtn.streaming.deserializers;

import com.quangtn.streaming.domain.Conversion;
import com.quangtn.streaming.utils.Gzip;
import com.quangtn.streaming.utils.Json;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.Objects;

@Slf4j
public class ConversionGzipJsonDeserializer implements DeserializationSchema<Conversion> {

    @Override
    public Conversion deserialize(byte[] bytes) throws IOException {
        Conversion conversion = null;
        if(Objects.nonNull(bytes)) {
            try {
                val json = Gzip.decompress(bytes);
                conversion = Json.toObject(json, Conversion.class);
            }
            catch (final Exception e) {
                log.error("", e.getMessage());
            }
        }
        return conversion;
    }

    @Override
    public boolean isEndOfStream(final Conversion conversion) { return false; }

    @Override
    public TypeInformation<Conversion> getProducedType() { return TypeInformation.of(Conversion.class); }
}
