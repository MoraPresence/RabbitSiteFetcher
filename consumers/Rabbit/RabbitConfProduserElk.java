package consumers.Rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeoutException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RabbitConfProduserElk extends Thread {
    public Scanner cin = new Scanner(System.in);
    public PrintStream pout = new PrintStream(System.out);
    private CloseableHttpClient client = null;
    private static final String TASK_QUEUE_NAME = "task_queue_elk";

    private final ObjectMapper mapper = new ObjectMapper();

    public static class OneNews {
        private String _header;
        private String _author;
        private String _text;
        private String _URI;

        public String getHeader() {
            return _header;
        }

        public String getAuthor() {
            return _author;
        }

        public String getText() {
            return _text;
        }

        public String getURI() {
            return _URI;
        }

        public void setHeader(String header) {
            this._header = header;
        }

        public void setAuthor(String author) {
            this._author = author;
        }

        public void setText(String text) {
            this._text = text;
        }

        public void setURI(String URI) {
            this._URI = URI;
        }

    }

    public static void runProduser(HashMap<String, Document> docVec) throws IOException, TimeoutException {
        RabbitConfProduserElk produsers = new RabbitConfProduserElk();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");
        factory.setPort(5672);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, false, false, true, null);
        channel.basicQos(1);

        for (HashMap.Entry<String, Document> entry : docVec.entrySet()) {
            produsers.sendDataToRabbitElk(entry.getKey(), entry.getValue(), channel);
        }

        channel.close(); //<--- this close the channel and eventually all listening
        connection.close();//<-- this close the connection and all channels
    }

    public RabbitConfProduserElk() throws IOException, TimeoutException {
        CookieStore httpCookieStore = new BasicCookieStore();
        client = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                .setDefaultCookieStore(new BasicCookieStore()).build();
    }

    public void sendDataToRabbitElk(String uri, Document doc, Channel channel) {
        OneNews oneNewsVec = new OneNews();
        Elements spans = doc.select("div [class=article__text__overview]");
        for (Element element : spans) {
            try {
                oneNewsVec.setHeader(doc.select("div [class=article__header__title]").get(0).text());
                oneNewsVec.setAuthor(doc.select("div [class=article__authors]").get(0).text());
                oneNewsVec.setText(element.text());
                oneNewsVec.setURI(uri);
                channel.basicPublish("", TASK_QUEUE_NAME, null, mapper.writeValueAsBytes(oneNewsVec));
            } catch (Exception e) {
                // log.error(e);
            }
        }
    }
}