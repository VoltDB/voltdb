import java.util.ArrayDeque;
import java.util.ArrayList;

public class IntrinsicLockPatternTest {
    final int CAP = 10000;
    final ArrayDeque<Runnable> m_tasks = new ArrayDeque<Runnable>();
    final ArrayList<Executor> m_executors = new ArrayList<Executor>();
    int m_counter;
    volatile int m_watch = 0;

    class Poller implements Runnable {
        
        private ThreadLocal<Long> m_counter = new ThreadLocal<Long>() {
            protected Long initialValue() {
                return Long.MIN_VALUE;
            }
        };
        
        public void run() {
            m_counter.set(m_counter.get().longValue() + 1);
        }
    }
    
    class Executor extends Thread {
        
        public Executor(int index) {
            super("Executor " + index);
        }
        
        public void run() {
            try {
                while (true) {
                    Runnable nextTask = null;
                    synchronized (m_tasks) {
                        nextTask = m_tasks.poll();
                        while (nextTask == null) {
                            m_tasks.wait();
                            nextTask = m_tasks.poll();
                        }
                    }
                    if (nextTask == null) {
                        return;
                    }
    
                    nextTask.run();
                }
            } catch (InterruptedException e) {
                
            }
        }
    }
    
    public IntrinsicLockPatternTest() {
        for (int ii = 0; ii < 8; ii++) {
            final Executor exec = new Executor(ii);
            exec.start();
            m_executors.add(exec);
        }
    }
    
    public void runTest() {
        while (true) {
            for (int i=0; i < CAP/4; ++i) {
                synchronized (m_tasks) {
                    m_tasks.offer(new Poller());
                    m_tasks.notify();
                }
            }

            System.out.printf(".");

            boolean shouldSleep = false;
            synchronized (m_tasks) {
                if (m_tasks.size() > CAP / 2) {
                    shouldSleep = true;
                }
            }
            
            if (shouldSleep) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
    
    public static void main(String[] args) {
        IntrinsicLockPatternTest that = new IntrinsicLockPatternTest();
        that.runTest();
    }

}
