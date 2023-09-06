package io.confluent.data.contracts.rules;

import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import org.apache.avro.generic.GenericData;

public class CheckSloTimeliness implements RuleExecutor {

  public String type() {
    return "SLO_TIMELINESS";
  }


  public Object transform(RuleContext ctx, Object message) throws RuleException {
    try {
      String sloStr = ctx.getParameter("slo_timeliness_secs");
      int slo = Integer.parseInt(sloStr);
      long timestamp = (Long) ((GenericData.Record) message).get("timestamp");
      long now = System.currentTimeMillis();
      return now - timestamp <= slo;
    } catch (Exception e) {
      throw new RuleException(e);
    }
  }
}
