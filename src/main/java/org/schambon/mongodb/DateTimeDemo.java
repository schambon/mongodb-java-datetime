package org.schambon.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.*;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class DateTimeDemo {

    public static void main(String[] args) {

        // First we have to register a DocumentCodecProvider that specifically tells the driver to decode to ZonedDateTime
        // when it encounters a BSON date_time (and we're decoding to Document)
        Map<BsonType, Class<?>> replacements = new HashMap<>();
        replacements.put(BsonType.DATE_TIME, ZonedDateTime.class);
        BsonTypeClassMap bsonTypeClassMap = new BsonTypeClassMap(replacements);

        DocumentCodecProvider provider = new DocumentCodecProvider(bsonTypeClassMap);

        // Then we tell the driver *how* to decode, by providing a Codec
        // We want all dates to be decoded as Europe/Paris based, so that's how we configure our codec
        ZonedDateTimeCodec zonedDateTimeCodec = new ZonedDateTimeCodec(ZoneId.of("Europe/Paris"));

        // finally we register all that in a registry that we provide in the
        CodecRegistry registry = fromRegistries(
                MongoClient.getDefaultCodecRegistry(),
                fromCodecs(zonedDateTimeCodec),
                fromProviders(provider)
        );

        MongoClient client = new MongoClient(new MongoClientURI("mongodb://localhost:27017", new MongoClientOptions.Builder().codecRegistry(registry)));

        MongoCollection<Document> datetimeColl = client.getDatabase("test").getCollection("datetime");

        // create a document with a London-zoned date time
        ZonedDateTime zdt = ZonedDateTime.now().withZoneSameInstant(ZoneId.of("Europe/London"));
        System.out.println(zdt);

        // write the doc to the db (here replace with upsert to make it repeatable)
        datetimeColl.replaceOne(new Document("_id", "testing"), new Document("date", zdt), new ReplaceOptions().upsert(true));

        // and read it back
        Document found = datetimeColl.find(new Document("_id", "testing")).first();
        ZonedDateTime date = found.get("date", ZonedDateTime.class);
        System.out.println(date);

        // we've read it back as a Paris date...
        assert date.getZone().equals(ZoneId.of("Europe/Paris"));
        // should be one hour ahead of the London-based date we had before
        assert date.getHour() == zdt.getHour() + 1;
        // but should be the same datetime overall (i.e. same Instant)
        assert date.isEqual(zdt);

    }

    static class DocumentCodecProvider implements CodecProvider {
        private final BsonTypeClassMap bsonTypeClassMap;

        public DocumentCodecProvider(final BsonTypeClassMap bsonTypeClassMap) {
            this.bsonTypeClassMap = bsonTypeClassMap;
        }

        @Override
        public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
            if (clazz == Document.class) {
                // construct DocumentCodec with a CodecRegistry and a BsonTypeClassMap
                return (Codec<T>) new DocumentCodec(registry, bsonTypeClassMap);
            }

            return null;
        }
    }


    static class ZonedDateTimeCodec implements Codec<ZonedDateTime> {

        // the zone to apply to decoded dates
        private final ZoneId zoneId;

        public ZonedDateTimeCodec(ZoneId zoneId) {
            this.zoneId = zoneId;
        }

        @Override
        public ZonedDateTime decode(BsonReader reader, DecoderContext decoderContext) {
            BsonType currentBsonType = reader.getCurrentBsonType();
            if (!currentBsonType.equals(BsonType.DATE_TIME)) {
                throw new CodecConfigurationException(String.format("Unable to decode non-date value into ZonedDateTime"));
            }
            long epoch = reader.readDateTime();

            return ZonedDateTime.ofInstant(Instant.ofEpochMilli(epoch), zoneId);
        }

        @Override
        public void encode(BsonWriter writer, ZonedDateTime value, EncoderContext encoderContext) {
            writer.writeDateTime(value.toInstant().toEpochMilli());
        }

        @Override
        public Class<ZonedDateTime> getEncoderClass() {
            return ZonedDateTime.class;
        }
    }
}
