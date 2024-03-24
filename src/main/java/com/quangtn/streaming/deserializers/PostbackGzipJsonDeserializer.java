package com.quangtn.streaming.deserializers;

import com.quangtn.streaming.domain.Postback;
import com.quangtn.streaming.utils.Gzip;
import com.quangtn.streaming.utils.Json;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.Objects;

@Slf4j
public class PostbackGzipJsonDeserializer implements DeserializationSchema<Postback> {

    @Override
    public Postback deserialize(byte[] bytes) throws IOException {
        Postback postback = null;
        if(Objects.nonNull(bytes)) {
            try {
                val json = Gzip.decompress(bytes);
                postback = Json.toObject(json, Postback.class);
            }
            catch (final Exception e) {
                log.error("", e.getMessage());
            }
        }
        return postback;
    }

    @Override
    public boolean isEndOfStream(final Postback postback) { return false; }

    @Override
    public TypeInformation<Postback> getProducedType() { return TypeInformation.of(Postback.class);
    }
}
