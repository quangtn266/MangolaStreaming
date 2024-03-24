package com.quangtn.streaming.deserializers;

import com.quangtn.streaming.domain.BidResp;
import com.quangtn.streaming.utils.Gzip;
import com.quangtn.streaming.utils.Json;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.Objects;

@Slf4j
public class BidRespGzipJsonDeserializer implements DeserializationSchema<BidResp> {

    @Override
    public BidResp deserialize(final byte[] bytes) throws IOException {
        BidResp bidResp = null;
        if(Objects.nonNull(bytes)) {
            try {
                val json = Gzip.decompress(bytes);
                bidResp = Json.toObject(json, BidResp.class);
            }
            catch (final Exception e) {
                log.error("", e.getMessage());
            }
        }
        return bidResp;
    }

    @Override
    public boolean isEndOfStream(final BidResp bidResp) { return false; }

    @Override
    public TypeInformation<BidResp> getProducedType() { return  TypeInformation.of(BidResp.class); }
}
