package fin.test;

import org.apache.activemq.ActiveMQConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Locale;

/**
 * Hello world!
 */
public class Main {

    public static void main(String[] args) throws Exception {

        thread(new AuditConsumer(), false);

        while (true) {

            try {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(System.in));
                String input = reader.readLine();
                thread(new AuditLog(input), false);

            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }

    public static class AuditLog implements Runnable {
        public AuditLog(String msgInput) {
            try {

                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
                Connection connection = connectionFactory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue("audit");
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                String text = msgInput + " " + Thread.currentThread().getName() + " : " + this.hashCode();
                TextMessage message = session.createTextMessage(text);

                System.out.println("Sent message: "+ message.hashCode() + " : " + Thread.currentThread().getName());
                producer.send(message);

                // Clean up
                session.close();
                connection.close();
            }
            catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }

        @Override
        public void run() {

        }
    }

    public static class AuditConsumer implements Runnable, ExceptionListener {

        public void run() {

            while (true) {
                try {
                    Thread.sleep(1000);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {

                    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
                    Connection connection = connectionFactory.createConnection();
                    connection.start();

                    connection.setExceptionListener(this);

                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Destination destination = session.createQueue("audit");

                    MessageConsumer consumer = session.createConsumer(destination);
                    Message message = consumer.receive(1000);

                    //if (message == null) continue;
                    if (message instanceof TextMessage textMessage) {
                        String text = textMessage.getText();
                        System.out.println("Received: " + text);
                    }


                    consumer.close();
                    session.close();
                    connection.close();
                } catch (Exception e) {
                    System.out.println("Caught: " + e);
                    e.printStackTrace();
                }
            }

        }

        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
        }
    }
}
