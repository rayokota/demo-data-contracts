package io.confluent.data.contracts.rules;

import com.acme.Order;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import java.time.Instant;

public class CheckSloTimeliness implements RuleExecutor {

  public String type() {
    return "SLO_TIMELINESS";
  }


  public Object transform(RuleContext ctx, Object message) throws RuleException {
    try {
      String sloStr = ctx.getParameter("slo_timeliness_secs");
      int slo = Integer.parseInt(sloStr);
      Instant timestamp = ((Order) message).getTimestamp();
      long now = System.currentTimeMillis();
      return now - timestamp.toEpochMilli() <= slo;
    } catch (Exception e) {
      throw new RuleException(e);
    }
  }
}
