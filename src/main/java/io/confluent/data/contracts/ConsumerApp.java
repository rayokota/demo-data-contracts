package io.confluent.data.contracts;

import io.confluent.data.contracts.rules.EmailAction;
import io.confluent.data.contracts.utils.ClientUtils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

public class ConsumerApp implements Runnable {

  private static final Logger log = Logger.getLogger(ConsumerApp.class);

  private Properties props;
  private String topic;

  public ConsumerApp(
      String propertiesFile,
      String groupId,
      String clientId,
      String username,
      String password
  ) {
    try {
      props = ClientUtils.loadConfig(propertiesFile);
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
      if (groupId != null) {
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      }
      if (clientId != null) {
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
      }
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
      props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS, "checkSloTimeliness");
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".checkSloTimeliness.class",
          EmailAction.class.getName());
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".checkSloTimeliness.param.username",
          username);
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".checkSloTimeliness.param.password",
          password);

      topic = props.getProperty("topic");
    } catch (Exception e) {
      log.error("Error in ConsumerApp.constructor", e);
    }
  }

  @Override
  public void run() {
    try (Consumer<String, Object> consumer = new KafkaConsumer<>(props)) {
      // Subscribe to topic
      consumer.subscribe(Collections.singletonList(topic));
      log.info("Starting consumer on ConsumerApp...");
      while (true) {
        // Consume records
        ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(10000));
        for (ConsumerRecord<String, Object> record : records) {
          System.out.println("New order: " + record.value());
        }
      }
    } catch (Exception e) {
      log.error("Error in ConsumerApp.run", e);
    }
  }

  public static void main(final String[] args) {
    if (args.length < 5) {
      System.out.println(
          "Please provide command line arguments: "
              + "propertiesFile groupID clientID emailUsername emailPassword");
      System.exit(1);
    }
    String propertiesFile = args[0];
    String groupId = args[1];
    String clientId = args[2];
    String username = args[3];
    String password = args[4];
    ConsumerApp consumer = new ConsumerApp(
        propertiesFile, groupId, clientId, username, password);
    consumer.run();
  }
}

