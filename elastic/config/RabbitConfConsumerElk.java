package elastic.config;

import com.rabbitmq.client.*;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.TimeoutException;

public class RabbitConfConsumerElk {
    public Scanner cin = new Scanner(System.in);
    public PrintStream pout = new PrintStream(System.out);
    private static final String TASK_QUEUE_NAME = "task_queue_elk";
    private CloseableHttpClient client = null;

    public static void runConsumer(Vector<byte[]> newsVec) throws Exception {
        RabbitConfConsumerElk consumers = new RabbitConfConsumerElk();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");
        factory.setPort(5672);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, false, false, true, null);
        channel.basicQos(1);

        channel.basicConsume(TASK_QUEUE_NAME, false, "myConsumerTag", new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException {
                long deliveryTag = envelope.getDeliveryTag();
                // (process the message components here ...)
                //System.out.println(" [x] Received '" + oneNewsVec.get(0) + "'" + "  " + Thread.currentThread());
                newsVec.add(body);
                channel.basicAck(deliveryTag, false);
            }
        });

        AMQP.Queue.DeclareOk response = channel.queueDeclarePassive(TASK_QUEUE_NAME);

        if (response.getMessageCount() != 0) {
            Thread.sleep(5000);
        }
        channel.basicCancel("myConsumerTag");
        channel.close(); //<--- this close the channel and eventually all listening
        connection.close();//<-- this close the connection and all channels
    }


    public RabbitConfConsumerElk() throws IOException, TimeoutException {
        CookieStore httpCookieStore = new BasicCookieStore();
        client = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                .setDefaultCookieStore(new BasicCookieStore()).build();
    }
}