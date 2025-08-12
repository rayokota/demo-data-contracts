package io.confluent.data.contracts;

import io.confluent.data.contracts.utils.ClientUtils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

public class ProducerApp implements Runnable {

  private static final Logger log = Logger.getLogger(ProducerApp.class);

  private Properties props;
  private String topic;

  public ProducerApp(String propertiesFile, String clientId) {
    try {
      props = ClientUtils.loadConfig(propertiesFile);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
      if (clientId != null) {
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
      }
      props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
      props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
      props.put(AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT, "false");

      topic = props.getProperty("topic");

      // Create the topic if it doesn't exist already
      ClientUtils.createTopic(props, topic);
      ClientUtils.createTopic(props, topic + "-dlq");

    } catch (Exception e) {
      log.error("Error in ProducerApp.constructor", e);
    }
  }

  @Override
  public void run() {
    try (Producer<String, Object> producer = new KafkaProducer<>(props)) {
      while (true) {
        // Create a producer record
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, OrderGen.getNewOrder());

        // Send the record
        producer.send(record);

        System.out.println("New order: " + record.value());

        Thread.sleep(1000);
      }
    } catch (Exception e) {
      log.error("Error in ProducerApp.run", e);
    }
  }

  public static void main(final String[] args) {
    if (args.length < 2) {
      log.error("Provide the properties file and client ID as arguments");
      System.exit(1);
    }
    ProducerApp producer = new ProducerApp(args[0], args[1]);
    producer.run();
  }
}
