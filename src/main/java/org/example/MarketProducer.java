package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.Properties;

public class MarketProducer {
    private static final String TOPIC = "raw-trades";
    private static final String BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        WebSocketClient client = new WebSocketClient(new URI(BINANCE_WS_URL)) {
            @Override
            public void onOpen(ServerHandshake handshakedata) {
                System.out.println("Connected to Binance Trade Stream.");
            }

            @Override
            public void onMessage(String message) {
                producer.send(new ProducerRecord<>(TOPIC, "BTCUSDT", message), (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Forwarded to Kafka Partition: " + metadata.partition() + " Offset: " + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                System.out.println("Closed connection: " + reason);
            }

            @Override
            public void onError(Exception ex) {
                ex.printStackTrace();
            }
        };

        client.connect();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down producer...");
            client.close();
            producer.close();
        }));
    }
}
