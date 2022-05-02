package elastic.config;

import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

public class ThreadPool implements Executor {
    private final Queue<Runnable> workQueue = new ConcurrentLinkedQueue<>();
    private volatile boolean isRunning = true;
    public Vector<Thread> Threads = null;
    int nThreads = 0;

    public ThreadPool(int _nThreads) {
        Threads = new Vector<Thread>();
        for (int i = 0; i < _nThreads; i++) {
            Threads.add(new Thread(new TaskWorker()));
            Threads.get(i).start();
        }
        nThreads = _nThreads;
    }

    @Override
    public void execute(Runnable command) {
        if (isRunning) {
            workQueue.offer(command);
        }
    }

    public void shutdown() {
        isRunning = false;
    }

    private final class TaskWorker implements Runnable {

        @Override
        public void run() {
            while (isRunning) {
                Runnable nextTask = workQueue.poll();
                if (nextTask != null) {
                    nextTask.run();
                }
            }
        }
    }
}