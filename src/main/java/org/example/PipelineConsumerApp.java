package org.example;

import com.fasterxml.jackson.core.json.async.NonBlockingJsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class PipelineConsumerApp {
    private static final String TOPIC = "raw-trades";
    private static final String INSERT_SQL = """
            INSERT INTO trades (trade_id, exchange_id, instrument_id, price, quantity, trade_time, is_buyer_maker)
            VALUES (?, 1, 1, ?, ?, ?, ?)
            ON CONFLICT (exchange_id, trade_id) DO NOTHING;
            """;
    public static void main(String[] args) throws Exception {
        //Kafka Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "market-data-persistance-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        ObjectMapper mapper = new ObjectMapper();
        DataSource dataSource = DatabasePool.getDataSource();

        System.out.println("Consumer started. Waiting for trades...");

        try (consumer) {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    continue;
                }

                try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
                    conn.setAutoCommit(false);

                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            TradeEvent trade = mapper.readValue(record.value(), TradeEvent.class);

                            stmt.setLong(1, trade.tradeId());
                            stmt.setBigDecimal(2, trade.price());
                            stmt.setBigDecimal(3, trade.quantity());
                            stmt.setTimestamp(4, Timestamp.from(trade.getTradeInstant()));
                            stmt.setBoolean(5, trade.isBuyerMaker());
                            stmt.addBatch();
                        } catch (Exception e) {
                            System.err.println("Deserialization error on offset " + record.offset() + ": " + e.getMessage());
                        }
                    }

                    int[] inserted = stmt.executeBatch();
                    conn.commit();

                    consumer.commitSync();
                    System.out.println("Batch persisted to DB: " + inserted.length + " trades processed.");
                } catch (Exception dbEx) {
                    System.err.println("Database batch write failed: " + dbEx.getMessage());
                    dbEx.printStackTrace();
                }
            }
        }



    }
}
