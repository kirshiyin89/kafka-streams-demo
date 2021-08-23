package org.demo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.kstream.Grouped.with;

public class KafkaStreamsDemo {

    public static final String INPUT_TOPIC = "weather-tmp-input";

    public static final String OUTPUT_TOPIC = "weather-tmp-output";

    static void createAvgTempCalcStream(final StreamsBuilder builder) {
        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);

        KStream<String, Double> dayTempStream = inputStream
                .map((s, line) -> {
                    try {
                        Weather w = mapLineToWeather(line);
                        return new KeyValue<>(w.day, w.temperature);
                    } catch (Exception e) {
                        System.err.println("Mapping error:" + e.getMessage());
                        throw e;
                    }
                });

        dayTempStream
                .groupByKey(with(Serdes.String(), Serdes.Double()).withName("datewithtemperature"))
                .aggregate(TempCalculator::new, (day, temperature, tempCalculator) -> {
                    try {
                        tempCalculator.incrementCounter();
                        tempCalculator.addSum(temperature);
                        return tempCalculator;
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                        throw e;
                    }
                }, Materialized.with(Serdes.String(), CustomSerdes.instance()))
                .toStream()
                .mapValues((day, tempCalculator) -> tempCalculator.getAvg())
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Double()));
    }



    public static Weather mapLineToWeather(String csvLine) {
        String[] csvFields = csvLine.split(",");
        String date = csvFields[0];
        Double temp = Double.parseDouble(csvFields[3]);
        return new Weather(date, temp);
    }

    public static void main(final String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        createAvgTempCalcStream(builder);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        final CountDownLatch latch = new CountDownLatch(1);

        // Catch ctrl + c event
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Shutdown streams...");
                streams.close();
                latch.countDown();
            }
        });

        try {
            System.out.println("Streams started...");
            streams.start();
            System.out.println("Waiting for events...");
            latch.await();
        } catch (final Throwable e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }

    static class TempCalculator {
        Integer count;
        Double sum;

        public TempCalculator() {
            count = 0;
            sum = 0.0;
        }

        public Double getSum() {
            return sum;
        }
        public Integer getCount() {
            return count;
        }
        @JsonIgnore
        public double getAvg() {
            if (count != 0) {
                return sum / count;
            } else {
                System.err.println("No day records found");
                return sum;
            }
        }

        @JsonIgnore
        public void incrementCounter() {
            ++ this.count;
        }

        @JsonIgnore
        public void addSum(Double sum) {
            this.sum += sum;
        }

    }
}

class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        // nothing to do
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }

}


class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private Class<T> tClass;

    public JsonDeserializer() {
    }

    public JsonDeserializer(Class<T> tClass) {
        this.tClass = tClass;
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        // nothing to do
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        T data;
        try {
            data = objectMapper.readValue(bytes, tClass);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {
        // nothing to do
    }
}


final class CustomSerdes {

    static public final class MySerde
            extends Serdes.WrapperSerde<KafkaStreamsDemo.TempCalculator> {
        public MySerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(KafkaStreamsDemo.TempCalculator.class));
        }
    }


    public static Serde<KafkaStreamsDemo.TempCalculator> instance() {
        return new CustomSerdes.MySerde();
    }

}