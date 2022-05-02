package produsers.Rabbit;

import com.rabbitmq.client.*;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
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

import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;

public class RabbitConfigurationProduser extends Thread {
    private int depth_count = 1;
    public String zeroSite;
    public Deque<String> urlVec = null;
    public Deque<String> resultUrlVec = null;
    public Scanner cin = new Scanner(System.in);
    public PrintStream pout = new PrintStream(System.out);

    private CloseableHttpClient client = null;


    private static final String TASK_QUEUE_NAME = "task_queue";


    public static void runProduser() throws IOException, TimeoutException {
        RabbitConfigurationProduser produsers = new RabbitConfigurationProduser();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");
        factory.setPort(5672);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, false, false, true, null);
        channel.basicQos(1);

        produsers.multiGetLinks(channel);

        channel.close(); //<--- this close the channel and eventually all listening
        connection.close();//<-- this close the connection and all channels
    }

    public RabbitConfigurationProduser() throws IOException, TimeoutException {
        urlVec = new ArrayDeque<String>();
        resultUrlVec = new ArrayDeque<String>();
        this.zeroSite = "https://www.rbc.ru/newspaper/";
        urlVec.add(zeroSite);

        CookieStore httpCookieStore = new BasicCookieStore();
        client = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                .setDefaultCookieStore(new BasicCookieStore()).build();
    }

    /*public <T> Deque<String> removeDuplicates(Deque<String> queue) {
        Set<String> set = new LinkedHashSet<>(queue);
        set.removeIf(s -> !s.startsWith("https://www.rbc.ru/newspaper/")); //удяляю лишние ссылки, потому что на некоторых сайтах бывает совершенный мусор
        return new ArrayDeque<String>(set);
    }*/

    public Deque<String> get_links(String url, Channel channel) throws IOException {
        Deque<String> tmp = new ArrayDeque<String>();
        Document doc = Jsoup.connect(url).get();
        Elements links = doc.select("a[href]");
        for (Element link : links) {
            if (!link.attr("abs:href").startsWith("https://www.rbc.ru/newspaper/")) continue;
            channel.basicPublish("", TASK_QUEUE_NAME, null, link.attr("abs:href").getBytes(StandardCharsets.UTF_8));
            tmp.add(link.attr("abs:href"));
        }
        return tmp;
    }

    public void multiNesting(Channel channel) throws IOException {
        String current = null;
        while ((current = urlVec.pollFirst()) != null) {
            resultUrlVec.addAll(get_links(current, channel));
        }
    }

    public void multiGetLinks(Channel channel) throws IOException { //main func of producer
        for (int i = 0; i < depth_count; ++i) {
            multiNesting(channel);
            pout.println(Thread.currentThread() + "start work");
            urlVec.addAll(resultUrlVec);
        }
        pout.println(Thread.currentThread() + "end work");
    }
}
