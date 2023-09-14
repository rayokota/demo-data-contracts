Demo for Data Contracts
=======================

After cloning this repository, register the schema with the following command:

```bash
mvn schema-registry:register
```

To run the consumer:

```bash
mvn compile exec:java \
-Dexec.mainClass="io.confluent.data.contracts.ConsumerApp" \
-Dexec.args="./src/main/resources/data-contracts.properties group-1 client-1 <emailUsername> <emailPassword>"
```

To generate random Order data, run the producer:

```bash
mvn compile exec:java \
-Dexec.mainClass="io.confluent.data.contracts.ProducerApp" \
-Dexec.args="./src/main/resources/data-contracts.properties client-1"
````

You should now see emails being sent for any records that do not meet the timeliness SLO.