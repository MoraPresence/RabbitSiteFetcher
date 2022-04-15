package consumers.Rabbit;

import org.jsoup.nodes.Document;

import java.util.HashMap;

import static consumers.Rabbit.RabbitConfigurationConsumers.runConsumer;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Hello, World!");
        System.out.println(Thread.currentThread());

        ThreadPool threadPoolConsumer = new ThreadPool(2);
        Printer printer = new Printer();
        HashMap<String, Document> docVec = new HashMap<String, Document>();

        Runnable task1 = () -> {
            try {
                runConsumer(docVec);
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

        printer.printNews(docVec);
    }
}
