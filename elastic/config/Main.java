package elastic.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static elastic.config.RabbitConfConsumerElk.runConsumer;


public class Main {
    public static final int header = 0;
    public static final int author = 1;
    public static final int text = 2;
    public static final int uri = 3;

    public static void main(String[] args) throws Exception {
        // write your code here
        Vector<byte[]> newsVector = new Vector<byte[]>();
        Config conf = ConfigFactory.load();
        ElasticConfigurator esCon = new ElasticConfigurator();
        esCon.initialize(conf.getConfig("es"));
        ThreadPool threadPoolConsumer = new ThreadPool(2);

        Runnable task1 = () -> {
            try {
                runConsumer(newsVector);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        threadPoolConsumer.execute(task1);
        threadPoolConsumer.execute(task1);

        threadPoolConsumer.shutdown();

        for (int i = 0; i < threadPoolConsumer.nThreads; ++i){
            threadPoolConsumer.Threads.get(i).join();
        }

        newsVector.forEach((oneNewsVec) -> {
            try {
                esCon.updateNews(UUID.randomUUID().toString(), oneNewsVec);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });

        esCon.getSomeDataAll();
        List<ElasticConfigurator.OneNews> testSearch1 = esCon.search("author", "Иван Ткачёв");
        List<ElasticConfigurator.OneNews> testSearch2 = esCon.search("", "");

        System.out.println(Arrays.toString(testSearch1.toArray()));
        System.out.println(Arrays.toString(testSearch2.toArray()));
        //System.out.println(Arrays.toString(resultOfSearch2.toArray()));

    }
}
