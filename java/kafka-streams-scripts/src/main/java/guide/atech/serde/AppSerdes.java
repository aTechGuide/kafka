package guide.atech.serde;

import guide.atech.kstreams.json.types.Message;
import guide.atech.schema.json.Notification;
import guide.atech.schema.json.PosInvoice;
import guide.atech.schema.json.employee.DepartmentAggregate;
import guide.atech.schema.json.employee.Employee;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

public class AppSerdes extends Serdes {

    // Message Serde
    static final class MessageSerde extends Serdes.WrapperSerde<Message> {
        public MessageSerde() {
            super(new JsonSerializer<Message>(), new JsonDeserializer<Message>());
        }
    }

    public static Serde<Message> Message() {

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Message.class);

        MessageSerde serde = new MessageSerde();
        serde.configure(serdeConfigs, false);

        return serde;
    }

    // POS Invoice Serde
    static final class PosInvoiceSerde extends Serdes.WrapperSerde<PosInvoice> {
        public PosInvoiceSerde() {
            super(new JsonSerializer<PosInvoice>(), new JsonDeserializer<PosInvoice>());
        }
    }

    public static Serde<PosInvoice> PosInvoice() {

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Message.class);

        PosInvoiceSerde serde = new PosInvoiceSerde();
        serde.configure(serdeConfigs, false);

        return serde;
    }

    // Notification Serde
    static final class NotificationSerde extends Serdes.WrapperSerde<Notification> {
        public NotificationSerde() {
            super(new JsonSerializer<Notification>(), new JsonDeserializer<Notification>());
        }
    }

    public static Serde<Notification> Notification() {

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Notification.class);

        NotificationSerde serde = new NotificationSerde();
        serde.configure(serdeConfigs, false);

        return serde;
    }

    // Employee Serde
    static final class EmployeeSerde extends Serdes.WrapperSerde<Employee> {
        public EmployeeSerde() {
            super(new JsonSerializer<Employee>(), new JsonDeserializer<Employee>());
        }
    }

    public static Serde<Employee> Employee() {

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Notification.class);

        EmployeeSerde serde = new EmployeeSerde();
        serde.configure(serdeConfigs, false);

        return serde;
    }

    // Department Aggregate Serde
    static final class DepartmentAggregateSerde extends Serdes.WrapperSerde<DepartmentAggregate> {
        public DepartmentAggregateSerde() {
            super(new JsonSerializer<DepartmentAggregate>(), new JsonDeserializer<DepartmentAggregate>());
        }
    }

    public static Serde<DepartmentAggregate> DepartmentAggregate() {

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Notification.class);

        DepartmentAggregateSerde serde = new DepartmentAggregateSerde();
        serde.configure(serdeConfigs, false);

        return serde;
    }
}
