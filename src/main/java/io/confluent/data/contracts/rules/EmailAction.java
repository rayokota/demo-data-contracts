package io.confluent.data.contracts.rules;

import io.confluent.kafka.schemaregistry.rules.RuleAction;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.Multipart;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.GenericData;

public class EmailAction implements RuleAction {

  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";

  private String username;
  private String password;

  public String type() {
    return "EMAIL";
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.username = (String) configs.get(USERNAME);
    this.password = (String) configs.get(PASSWORD);
  }

  public void run(RuleContext ctx, Object message, RuleException ex)
      throws RuleException {
    try {
      String from = ctx.getParameter("mail.smtp.from");
      String to = ctx.getParameter("owner_email");
      String sloTimeliness = ctx.getParameter("slo_timeliness_secs");

      Properties props = new Properties();
      props.put("mail.smtp.host", ctx.getParameter("mail.smtp.host"));
      props.put("mail.smtp.port", ctx.getParameter("mail.smtp.port"));
      props.put("mail.smtp.auth", "true");
      props.put("mail.smtp.starttls.enable", "true");
      Session session = Session.getInstance(props, new Authenticator() {
        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(username, password);
        }
      });

      Message mail = new MimeMessage(session);
      mail.setFrom(new InternetAddress(from));
      mail.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
      mail.setSubject("Mail Subject");

      String msg = "Order '" + ((GenericData.Record) message).get("orderId")
          + "' failed to meet the timeliness SLO of "
          + sloTimeliness + " seconds";

      MimeBodyPart mimeBodyPart = new MimeBodyPart();
      mimeBodyPart.setContent(msg, "text/html; charset=utf-8");
      Multipart multipart = new MimeMultipart();
      multipart.addBodyPart(mimeBodyPart);
      mail.setContent(multipart);

      Transport.send(mail);
    } catch (Exception e) {
      throw new RuleException(e);
    }
  }
}
