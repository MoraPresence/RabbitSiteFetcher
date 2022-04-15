package produsers.Rabbit;


import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        System.out.println("Hello, World!");
        System.out.println(Thread.currentThread());

        ThreadPool threadPoolProducer = new ThreadPool(2);

        Runnable task1 = () -> {
            try {
                RabbitConfigurationProduser.runProduser();
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        };

        threadPoolProducer.execute(task1);
        threadPoolProducer.execute(task1);


        threadPoolProducer.shutdown();

        for (int i = 0; i < threadPoolProducer.nThreads; ++i){
            threadPoolProducer.Threads.get(i).join();
        }
    }
}
