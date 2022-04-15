package consumers.Rabbit;

import com.rabbitmq.client.*;
import org.apache.http.HttpEntity;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class RabbitConfigurationConsumers {
    public Scanner cin = new Scanner(System.in);
    public PrintStream pout = new PrintStream(System.out);
    private static final String TASK_QUEUE_NAME = "task_queue";
    private int retryDelay = 5 * 1000;
    private int retryCount = 2;
    private int metadataTimeout = 30 * 1000;


    private CloseableHttpClient client = null;

    public static void runConsumer(HashMap<String, Document> docVec) throws Exception {
        RabbitConfigurationConsumers consumers = new RabbitConfigurationConsumers();
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
                String message = new String(body, StandardCharsets.UTF_8);
                //System.out.println(" [x] Received '" + message + "'" + "  " + Thread.currentThread());
                consumers.getDataUri(message, docVec);
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


    public RabbitConfigurationConsumers() throws IOException, TimeoutException {
        CookieStore httpCookieStore = new BasicCookieStore();
        client = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                .setDefaultCookieStore(new BasicCookieStore()).build();
    }

    public void getDataUri(String url, HashMap<String, Document> docVec) {
        int code = 0;
        boolean bStop = false;
        Document doc = null;
        for (int iTry = 0; iTry < retryCount && !bStop; iTry++) {
            //  log.info("getting page from url " + url);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(metadataTimeout)
                    .setConnectTimeout(metadataTimeout)
                    .setConnectionRequestTimeout(metadataTimeout)
                    .setExpectContinueEnabled(true)
                    .build();
            HttpGet request = new HttpGet(url);
            request.setConfig(requestConfig);
            CloseableHttpResponse response = null;
            try {
                pout.println(Thread.currentThread() + "start");
                response = client.execute(request);
                pout.println(Thread.currentThread() + "stop");
                code = response.getStatusLine().getStatusCode();
                if (code == 404) {
                    pout.println("error get url " + url + " code " + code);
                    // log.warn("error get url " + url + " code " + code);
                    bStop = true;//break;
                } else if (code == 200) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        try {
                            doc = Jsoup.parse(entity.getContent(), "UTF-8", url);
                            docVec.put(url, doc);
                            break;
                        } catch (IOException e) {
                            // log.error(e);
                        }
                    }
                    bStop = true;
                } else {
                    pout.println("error get url " + url + " code " + code);
                    // log.warn("error get url " + url + " code " + code);
                    response.close();
                    response = null;
                    client.close();
                    // CookieStore httpCookieStore = new BasicCookieStore();
                    client = HttpClients.custom()
                            .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                            .setDefaultCookieStore(new BasicCookieStore()).build();
                    int delay = retryDelay * 1000 * (iTry + 1);
                    // log.info("wait " + delay / 1000 + " s...");
                    try {
                        Thread.sleep(delay);
                        continue;
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            } catch (IOException e) {
                // log.error(e);
            }
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    // log.error(e);
                }
            }
        }
    }
}