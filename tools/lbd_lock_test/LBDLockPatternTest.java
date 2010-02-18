import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LBDLockPatternTest {

    final int CAP = 10000;
    final LinkedBlockingDeque<Integer> m_lbd = new LinkedBlockingDeque<Integer>(CAP);
    final ExecutorService m_executor;
    int m_counter;
    volatile int m_watch = 0;

    class Poller implements Runnable {
        public void run() {
            m_lbd.poll();
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
        new Offerer().start();
        while (true) {
            m_executor.execute(new Poller());
        }
    }
    
    public static void main(String[] args) {
        LBDLockPatternTest that = new LBDLockPatternTest();
        that.runTest();
    }

}
