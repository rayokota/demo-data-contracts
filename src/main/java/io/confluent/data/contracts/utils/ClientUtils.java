package io.confluent.data.contracts.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

public class ClientUtils {

  public static Properties loadConfig(final String configFile) throws IOException {
    Path path = Paths.get(configFile);
    if (!Files.exists(path)) {
      throw new IOException(configFile + " not found.");
    }
    final Properties cfg = new Properties();
    try (InputStream inputStream = Files.newInputStream(path)) {
      cfg.load(inputStream);
    }
    return cfg;
  }


  public static void createTopic(Properties props, String topicName) {
    try (AdminClient adminClient = AdminClient.create(props)) {
      boolean topicExists;
      // Replace possible spaces with - on the topic name
      topicName = topicName.replace(" ", "-");

      // Check if topic already exists
      topicExists = adminClient.listTopics().names().get().contains(topicName);
      if (!topicExists) {
        int partitions = Integer.parseInt(props.getProperty("num.partitions"));
        int replication = Integer.parseInt(props.getProperty("replication.factor"));
        NewTopic newTopic = new NewTopic(topicName, partitions, (short) replication);
        adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
