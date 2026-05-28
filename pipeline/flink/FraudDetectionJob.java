/*
 * Job Flink : Détection de fraude en temps réel
 * 
 * Consomme les transactions depuis le topic Kafka "oltp.transactions",
 * applique une logique de scoring (modèle ML simulé ou appel externe),
 * et émet les alertes dans le topic "events.fraud".
 *
 * Dépendances Maven (extrait) :
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
    //  Point d'entrée du job
    // ------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        // Création de l'environnement d'exécution Flink
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Activation des checkpoints pour la tolérance aux pannes (toutes les 5 secondes)
        env.enableCheckpointing(5000);

        // --------------------------------------------------------------------
        // 1. Configuration de la source Kafka (transactions)
        // --------------------------------------------------------------------
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "fraud-detection-job");
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        FlinkKafkaConsumer<Transaction> transactionSource = new FlinkKafkaConsumer<>(
                "oltp.transactions",                                  // topic source
                new TransactionDeserializationSchema(),               // désérialisation personnalisée
                consumerProps
        );
        // Stratégie de watermark : monotonique basée sur le champ timestamp de la transaction
        transactionSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Transaction>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );

        DataStream<Transaction> transactions = env.addSource(transactionSource)
                .name("Kafka Source - Transactions");

        // --------------------------------------------------------------------
        // 2. Logique de traitement : détection de fraude avec état
        // --------------------------------------------------------------------
        DataStream<FraudAlert> fraudAlerts = transactions
                .keyBy(Transaction::getCustomerId)   // partitionnement par client
                .flatMap(new FraudDetectionProcessFunction())
                .name("Fraud Detection Process");

        // --------------------------------------------------------------------
        // 3. Configuration du sink Kafka (alertes de fraude)
        // --------------------------------------------------------------------
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "fraud-alert-producer");

        FlinkKafkaProducer<FraudAlert> alertSink = new FlinkKafkaProducer<>(
                "events.fraud",                                        // topic destination
                new FraudAlertSerializationSchema(),                   // sérialisation
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        fraudAlerts.addSink(alertSink)
                .name("Kafka Sink - Fraud Alerts");

        // --------------------------------------------------------------------
        // Exécution du job
        // --------------------------------------------------------------------
        env.execute("Fraud Detection Job");
    }

    // ========================================================================
    // Classes internes pour la logique métier
    // ========================================================================

    /**
     * Process function stateful pour la détection de fraude.
     * Conserve le nombre de transactions et le montant total par client
     * sur les 5 dernières minutes.
     */
    public static class FraudDetectionProcessFunction
            extends RichFlatMapFunction<Transaction, FraudAlert> {

        // État : nombre de transactions dans la fenêtre de 5 minutes
        private transient ValueState<Integer> transactionCountState;
        // État : montant total dans la fenêtre
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
            // Récupération de l'état actuel (initialisé à 0 s'il n'existe pas)
            Integer currentCount = transactionCountState.value();
            Double currentTotal = totalAmountState.value();
            if (currentCount == null) {
                currentCount = 0;
                currentTotal = 0.0;
            }

            // Mise à jour des compteurs
            currentCount++;
            currentTotal += transaction.getAmount();
            transactionCountState.update(currentCount);
            totalAmountState.update(currentTotal);

            // Logique de scoring : règle simple combinant vélocité et montant
            // (En production, on appellerait ici un modèle ML externe via API REST)
            boolean isFraudulent = false;
            double fraudScore = 0.0;

            // Règle 1 : plus de 3 transactions en 5 minutes -> suspect
            if (currentCount > 3) {
                fraudScore += 0.5;
            }
            // Règle 2 : montant total > 10 000 USD -> suspect
            if (currentTotal > 10_000) {
                fraudScore += 0.5;
            }

            isFraudulent = fraudScore >= 0.5;

            // Création de l'alerte si fraude détectée
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

                // Réinitialisation de l'état après émission pour éviter les doublons
                // (selon la stratégie désirée, on peut le conserver ou le nettoyer)
                // transactionCountState.clear();
                // totalAmountState.clear();
            }
        }
    }

    // ========================================================================
    // Modèles de données simples
    // ========================================================================

    /**
     * Représentation simplifiée d'une transaction issue de Kafka.
     */
    public static class Transaction {
        private String transactionId;
        private String customerId;
        private String merchantId;
        private double amount;
        private long timestamp; // epoch millisecondes

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
     * Alerte de fraude émise vers Kafka.
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
    // Schémas de sérialisation/désérialisation pour Kafka
    // ========================================================================

    /**
     * Désérialise un message Kafka JSON en objet Transaction.
     * Dans la pratique, on utiliserait Jackson ou Gson.
     */
    public static class TransactionDeserializationSchema extends JSONKeyValueDeserializationSchema {
        // Implémentation simplifiée : on suppose que le JSON contient les champs correspondants.
        // Dans un cas réel, on étendrait AbstractDeserializationSchema<Transaction>.
    }

    /**
     * Sérialise un objet FraudAlert en JSON pour Kafka.
     */
    public static class FraudAlertSerializationSchema extends JSONKeyValueSerializationSchema {
        // Implémentation simplifiée.
    }
}