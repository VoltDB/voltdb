import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;

public class LBDLockPatternTest {

    final int numTasks = 32;
    final int CAP = 10000;
    final ExecutorService m_executor;
    int m_counter;
    volatile int m_watch = 0;
    final LinkedBlockingDeque<Integer> m_lbd = new LinkedBlockingDeque<Integer>(CAP);
    final HashMap<Integer, LimitedTask> m_taskMap = new HashMap<Integer, LimitedTask>();
    
    class LimitedTask {
        volatile boolean m_running = false;
        private final Object m_lock = new Object();
        private long m_lastRunIndex = Long.MIN_VALUE;
        private Integer m_id;
        
        public LimitedTask(Integer id) {
            m_id = id;
        }
        
        public void lockForHandlingWork() {
            synchronized(m_lock) {
                if (m_running) {
                    System.err.println("Attempted to run a limited task while it was already running");
                    System.exit(-1);
                }
                assert m_running == false;
                m_running = true;
            }
        }
        
        public void run(long runIndex) {
            try {
                if (runIndex < m_lastRunIndex) {
                    System.err.println("Attempted to run a limited task out of order or with an old invocation");
                    System.exit(-1);
                }
                m_lastRunIndex = runIndex;
                m_lbd.poll();
            } finally {
                synchronized (m_lock) {
                    m_running = false;
                }
                synchronized (m_taskMap) {
                    if (m_taskMap.put(m_id, this) != null) {
                        System.err.println("Attempted to place limited task back in map when it was already present");
                        System.exit(-1);
                    }
                }
            }
        }
    }
    
    class Offerer extends Thread {
        public void run() {
            while (true) {
                for (int i=0; i < CAP/4; ++i)
                    m_lbd.offer(0);

                System.out.printf("."); 

                if (m_lbd.size() > CAP / 2) {
                    try { 
                        Thread.sleep(100); 
                    } catch (Exception e) { 
                        e.printStackTrace(); 
                    }
                }
            }
        }
    }
    
    public LBDLockPatternTest() {
        m_executor = 
          new ThreadPoolExecutor(8, 8, 0L, TimeUnit.MILLISECONDS,
                                 new LinkedBlockingQueue<Runnable>(CAP));
        
        ((ThreadPoolExecutor)m_executor).setRejectedExecutionHandler(
            new ThreadPoolExecutor.DiscardPolicy());   
    }
    
    public void runTest() {
        for (int ii = 0; ii < numTasks; ii++) {
            m_taskMap.put(ii, new LimitedTask(ii));
        }
        new Offerer().start();
        long runIndex = Long.MIN_VALUE;
        java.util.Random r = new java.util.Random();
        while (true) {
            runIndex++;
            LimitedTask nextTask = null;
            while (nextTask == null) {
                synchronized (m_taskMap) {
                    nextTask = m_taskMap.remove(r.nextInt(numTasks));
                }
            }
            final LimitedTask theTask = nextTask;
            final long theIndex = runIndex;
            theTask.lockForHandlingWork();
            m_executor.execute(new Runnable() {
                @Override
                public void run() {
                    theTask.run(theIndex);
                }
            });
        }
    }
    
    public static void main(String[] args) {
        LBDLockPatternTest that = new LBDLockPatternTest();
        that.runTest();
    }

}
