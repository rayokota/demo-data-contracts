package io.confluent.demo.travel.producer;

import com.acme.OrderStatus;
import com.github.javafaker.Faker;
import com.acme.Order;
import java.time.Instant;

public class OrderGen {

    public static Order getNewOrder() {
        Faker fake = new Faker();
        Order order = new Order(
            fake.number().numberBetween(1, 10000),
            fake.number().numberBetween(1, 10000),
            fake.number().numberBetween(1, 10000),
            OrderStatus.Pending,
            Instant.now()
        );
        return order;
    }
}
