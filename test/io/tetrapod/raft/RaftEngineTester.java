package io.tetrapod.raft;

import java.io.*;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;

import org.junit.*;
import org.slf4j.*;

/**
 * Runs a full system simulation with fake RPC
 */
public class RaftEngineTester implements RaftRPC {

   public static final Logger              logger    = LoggerFactory.getLogger(RaftEngineTester.class);
   private static ScheduledExecutorService executor;
   private static File                     logDir;
   private static final int                NUM_PEERS = 5;

   @BeforeClass
   public static void makeTestDir() throws IOException {
      logDir = Files.createTempDirectory(null).toFile();
      executor = Executors.newScheduledThreadPool(4);
   }

   @AfterClass
   public static void deleteTestDir() {
      for (File file : logDir.listFiles()) {
         file.delete();
      }
      logDir.delete();
      executor.shutdownNow();
   }

   private static int randomDelay() {
      return 10 + (int) (Math.random() * 50);
   }

   private Map<Integer, RaftEngine<TestStateMachine>> rafts  = new HashMap<>();
   private RaftEngine<TestStateMachine>               dead   = null;
   private SecureRandom                               random = new SecureRandom();

   @Test
   public void testRaftEngine() {
      for (int i = 1; i <= NUM_PEERS; i++) {
         RaftEngine<TestStateMachine> raft = new RaftEngine<TestStateMachine>(logDir, "TEST", new TestStateMachine(), this);
         raft.setPeerId(i);
         for (int j = 1; j <= NUM_PEERS; j++) {
            if (j != i) {
               raft.addPeer(j);
            }
         }
         rafts.put(i, raft);
      }

      for (RaftEngine<?> raft : rafts.values()) {
         raft.start();
      }

      Thread t = new Thread(new Runnable() {
         public void run() {
            while (true) {
               try {
                  synchronized (rafts) {
                     for (RaftEngine<TestStateMachine> raft : rafts.values()) {
                        raft.executeCommand(new TestStateMachine.TestCommand(random.nextLong()));
                     }
                  }
                  sleep(5);
               } catch (Throwable t) {
                  logger.error(t.getMessage(), t);
               }
            }
         }
      });
      t.start();

      // WARNING: this will currently run until OOM since we don't have log compaction or journaling yet
      while (true) {

         synchronized (rafts) {
            sleep(1000);
            logger.info("=====================================================================================================================");
            for (RaftEngine<?> raft : rafts.values()) {
               logger.info(String.format("%d) %9s term=%d, lastIndex=%d, lastTerm=%d commitIndex=%d, %s", raft.getPeerId(), raft.getRole(),
                     raft.getCurrentTerm(), raft.getLog().getLastIndex(), raft.getLog().getLastTerm(), raft.getLog().getCommitIndex(),
                     raft.getStateMachine()));
            }
            logger.info("=====================================================================================================================");
            logger.info("");

            for (RaftEngine<TestStateMachine> r1 : rafts.values()) {
               for (RaftEngine<TestStateMachine> r2 : rafts.values()) {
                  if (r2 != r1) {
                     synchronized (r1.getStateMachine()) {
                        synchronized (r1.getStateMachine()) {
                           if (r1.getStateMachine().getIndex() == r2.getStateMachine().getIndex()) {
                              Assert.assertEquals(r1.getStateMachine().getCheckSum(), r2.getStateMachine().getCheckSum());
                           }
                        }
                     }
                  }
               }
            }

         }

         sleep(1000);

         if (dead == null) {
            if (random.nextDouble() < 0.1) {
               List<Integer> items = new ArrayList<Integer>(rafts.keySet());
               int peerId = items.get(random.nextInt(items.size()));
               synchronized (rafts) {
                  dead = rafts.remove(peerId);
               }
               logger.info("Killing {}", dead);
               dead.stop();
            }
         } else {
            if (random.nextDouble() < 0.25) {
               synchronized (rafts) {
                  rafts.put(dead.getPeerId(), dead);
               }
               logger.info("Reviving {}", dead);
               dead.start();
               dead.DEBUG = true;
               dead = null;

            }
         }

      }

   }

   private void sleep(int millis) {
      try {
         Thread.sleep(millis);
      } catch (InterruptedException e) {}
   }

   @Override
   public void sendRequestVote(final String clusterName, int peerId, final long term, final int candidateId, final long lastLogIndex,
         final long lastLogTerm, final RequestVoteResponseHandler handler) {
      final RaftEngine<?> r = rafts.get(peerId);
      if (r != null) {
         executor.schedule(new Runnable() {
            public void run() {
               try {
                  final RequestVoteResponse res = r.handleRequestVote(clusterName, term, candidateId, lastLogIndex, lastLogTerm);
                  executor.schedule(new Runnable() {
                     public void run() {
                        try {
                           handler.handleResponse(res.term, res.voteGranted);
                        } catch (Throwable t) {
                           logger.error(t.getMessage(), t);
                        }
                     }
                  }, randomDelay(), TimeUnit.MILLISECONDS);
               } catch (Throwable t) {
                  logger.error(t.getMessage(), t);
               }

            }
         }, randomDelay(), TimeUnit.MILLISECONDS);
      }
   }

   @Override
   public void sendAppendEntries(int peerId, final long term, final int leaderId, final long prevLogIndex, final long prevLogTerm,
         final Entry<?>[] entries, final long leaderCommit, final AppendEntriesResponseHandler handler) {
      final RaftEngine<?> r = rafts.get(peerId);
      if (r != null) {
         executor.schedule(new Runnable() {
            public void run() {
               try {
                  final AppendEntriesResponse res = r.handleAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
                  executor.schedule(new Runnable() {
                     public void run() {
                        try {
                           handler.handleResponse(res.term, res.success, res.lastLogIndex);
                        } catch (Throwable t) {
                           logger.error(t.getMessage(), t);
                        }
                     }
                  }, randomDelay(), TimeUnit.MILLISECONDS);
               } catch (Throwable t) {
                  logger.error(t.getMessage(), t);
               }
            }
         }, randomDelay(), TimeUnit.MILLISECONDS);
      }
   }

}
