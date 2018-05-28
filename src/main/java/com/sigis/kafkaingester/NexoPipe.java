package com.sigis.kafkaingester;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class NexoPipe {
    private Properties config = new Properties();
    private static final String NEXO_IN_TOPIC = "parser.nexo.receiver";
    private static final String NEXO_OUT_TOPIC = "parser.decoder.console";

    public NexoPipe(){
        this.setKafkaConfig();

    }

    public static void main(String[] args) throws Exception {

        final StreamsBuilder builder = new StreamsBuilder();
        final NexoPipe np = new NexoPipe();

        /********
         * Pipe
         * ******
        builder.stream(np.NEXO_IN_TOPIC).to(np.NEXO_OUT_TOPIC);

        */


        /**
         * Split and Pipe
         *
        KStream<String, String> source = builder.stream(np.NEXO_IN_TOPIC);
        source.flatMapValues(value -> Arrays.asList(value.toUpperCase().split("\\W+"))
        ).to(np.NEXO_OUT_TOPIC);
        */

        /**
         * WordCount
         */
        KStream<String, String> source = builder.stream(np.NEXO_IN_TOPIC);
        source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
                .toStream()
                .to(NEXO_OUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));


        source.to("parsed");

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, np.config);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
                System.out.println(latch.toString());
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);


    }

    private void createTopology(){


       /* final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("streams-plaintext-input");
*/
        /*KStreamBuilder StreamTopology = new KStreamBuilder();
        KStream<String, String> topicRecords = StreamTopology.Stream(stringSerde, stringSerde, "input");*/
    }



    private void setKafkaConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "nexo-pipe");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.22.20.109:9091,172.22.20.109:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        this.config = config;
    }

}
