/*
 * Flink Job: Real-time Fraud Detection
 * 
 * Consumes transactions from the Kafka topic "oltp.transactions",
 * applies scoring logic (simulated ML model or external call),
 * and emits alerts to the "events.fraud" topic.
 *
 * Maven Dependencies (extract):
 * <dependency>
 *   <groupId>org.apache.flink</groupId>
 *   <artifactId>flink-streaming-java</artifactId>
 *   <version>1.17.1</version>
 * </dependency>
 * <dependency>
 *   <groupId>org.apache.flink</groupId>
 *   <artifactId>flink-connector-kafka</artifactId>
 *   <version>1.17.1</version>
 * </dependency>
 */

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class FraudDetectionJob {

    // ------------------------------------------------------------------------
    //  Job entry point
    // ------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        // Create the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Enable checkpointing for fault tolerance (every 5 seconds)
        env.enableCheckpointing(5000);

        // --------------------------------------------------------------------
        // 1. Configure the Kafka source (transactions)
        // --------------------------------------------------------------------
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "fraud-detection-job");
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        FlinkKafkaConsumer<Transaction> transactionSource = new FlinkKafkaConsumer<>(
                "oltp.transactions",                                  // source topic
                new TransactionDeserializationSchema(),               // custom deserialisation
                consumerProps
        );
        // Watermark strategy: monotonous based on the transaction timestamp field
        transactionSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Transaction>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );

        DataStream<Transaction> transactions = env.addSource(transactionSource)
                .name("Kafka Source - Transactions");

        // --------------------------------------------------------------------
        // 2. Processing logic: stateful fraud detection
        // --------------------------------------------------------------------
        DataStream<FraudAlert> fraudAlerts = transactions
                .keyBy(Transaction::getCustomerId)   // partition by customer
                .flatMap(new FraudDetectionProcessFunction())
                .name("Fraud Detection Process");

        // --------------------------------------------------------------------
        // 3. Configure the Kafka sink (fraud alerts)
        // --------------------------------------------------------------------
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "fraud-alert-producer");

        FlinkKafkaProducer<FraudAlert> alertSink = new FlinkKafkaProducer<>(
                "events.fraud",                                        // destination topic
                new FraudAlertSerializationSchema(),                   // serialisation
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        fraudAlerts.addSink(alertSink)
                .name("Kafka Sink - Fraud Alerts");

        // --------------------------------------------------------------------
        // Execute the job
        // --------------------------------------------------------------------
        env.execute("Fraud Detection Job");
    }

    // ========================================================================
    // Inner classes for business logic
    // ========================================================================

    /**
     * Stateful process function for fraud detection.
     * Maintains the transaction count and total amount per customer
     * over the last 5 minutes.
     */
    public static class FraudDetectionProcessFunction
            extends RichFlatMapFunction<Transaction, FraudAlert> {

        // State: number of transactions in the 5-minute window
        private transient ValueState<Integer> transactionCountState;
        // State: total amount in the window
        private transient ValueState<Double> totalAmountState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Integer> countDescriptor =
                    new ValueStateDescriptor<>("txnCount", Types.INT);
            transactionCountState = getRuntimeContext().getState(countDescriptor);

            ValueStateDescriptor<Double> amountDescriptor =
                    new ValueStateDescriptor<>("totalAmount", Types.DOUBLE);
            totalAmountState = getRuntimeContext().getState(amountDescriptor);
        }

        @Override
        public void flatMap(Transaction transaction, Collector<FraudAlert> out) throws Exception {
            // Retrieve current state (initialised to 0 if it does not exist)
            Integer currentCount = transactionCountState.value();
            Double currentTotal = totalAmountState.value();
            if (currentCount == null) {
                currentCount = 0;
                currentTotal = 0.0;
            }

            // Update counters
            currentCount++;
            currentTotal += transaction.getAmount();
            transactionCountState.update(currentCount);
            totalAmountState.update(currentTotal);

            // Scoring logic: simple rule combining velocity and amount
            // (In production, an external ML model would be called here via REST API)
            boolean isFraudulent = false;
            double fraudScore = 0.0;

            // Rule 1: more than 3 transactions in 5 minutes -> suspicious
            if (currentCount > 3) {
                fraudScore += 0.5;
            }
            // Rule 2: total amount > 10,000 USD -> suspicious
            if (currentTotal > 10_000) {
                fraudScore += 0.5;
            }

            isFraudulent = fraudScore >= 0.5;

            // Create the alert if fraud is detected
            if (isFraudulent) {
                FraudAlert alert = new FraudAlert();
                alert.setTransactionId(transaction.getTransactionId());
                alert.setCustomerId(transaction.getCustomerId());
                alert.setMerchantId(transaction.getMerchantId());
                alert.setAmount(transaction.getAmount());
                alert.setFraudScore(fraudScore);
                alert.setTimestamp(System.currentTimeMillis());
                alert.setReason("Velocity/Amount threshold exceeded");

                out.collect(alert);

                // Reset state after emission to avoid duplicates
                // (depending on the desired strategy, it can be kept or cleared)
                // transactionCountState.clear();
                // totalAmountState.clear();
            }
        }
    }

    // ========================================================================
    // Simple data models
    // ========================================================================

    /**
     * Simplified representation of a transaction from Kafka.
     */
    public static class Transaction {
        private String transactionId;
        private String customerId;
        private String merchantId;
        private double amount;
        private long timestamp; // epoch milliseconds

        // Getters/Setters
        public String getTransactionId() { return transactionId; }
        public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public String getMerchantId() { return merchantId; }
        public void setMerchantId(String merchantId) { this.merchantId = merchantId; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    }

    /**
     * Fraud alert emitted to Kafka.
     */
    public static class FraudAlert {
        private String transactionId;
        private String customerId;
        private String merchantId;
        private double amount;
        private double fraudScore;
        private long timestamp;
        private String reason;

        // Getters/Setters
        public String getTransactionId() { return transactionId; }
        public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public String getMerchantId() { return merchantId; }
        public void setMerchantId(String merchantId) { this.merchantId = merchantId; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
        public double getFraudScore() { return fraudScore; }
        public void setFraudScore(double fraudScore) { this.fraudScore = fraudScore; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }
    }

    // ========================================================================
    // Kafka serialisation/deserialisation schemas
    // ========================================================================

    /**
     * Deserialises a Kafka JSON message into a Transaction object.
     * In practice, Jackson or Gson would be used.
     */
    public static class TransactionDeserializationSchema extends JSONKeyValueDeserializationSchema {
        // Simplified implementation: assumes the JSON contains the corresponding fields.
        // In a real-world scenario, one would extend AbstractDeserializationSchema<Transaction>.
    }

    /**
     * Serialises a FraudAlert object into JSON for Kafka.
     */
    public static class FraudAlertSerializationSchema extends JSONKeyValueSerializationSchema {
        // Simplified implementation.
    }
}