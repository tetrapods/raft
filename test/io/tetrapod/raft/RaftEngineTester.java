package io.tetrapod.raft;

import io.tetrapod.raft.TestStateMachine.TestCommand;

import java.io.*;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;

import org.junit.*;
import org.slf4j.*;

/**
 * Runs a full system simulation with fake RPC
 */
public class RaftEngineTester implements RaftRPC<TestStateMachine> {

   public static final Logger              logger    = LoggerFactory.getLogger(RaftEngineTester.class);
   private static final int                NUM_PEERS = 3;

   private static ScheduledExecutorService executor;
   private static File[]                   logDirs   = new File[NUM_PEERS];

   @BeforeClass
   public static void makeTestDir() throws IOException {
      for (int i = 0; i < NUM_PEERS; i++) {
         logDirs[i] = new File("logs/test-" + (i + 1)); //Files.createTempDirectory(null).toFile();
         logDirs[i].mkdirs();
      }
      executor = Executors.newScheduledThreadPool(NUM_PEERS);
   }

   @AfterClass
   public static void deleteTestDir() {
      //      for (int i = 0; i <  NUM_PEERS; i++) {
      //         for (File file : logDirs[i].listFiles()) {
      //            file.delete();
      //         }
      //         logDirs[i].delete();
      //      }
      executor.shutdownNow();
   }

   private static int randomDelay() {
      return 1 + (int) (Math.random() * 10);
   }

   private Map<Integer, RaftEngine<TestStateMachine>> rafts  = new HashMap<>();
   private SecureRandom                               random = new SecureRandom();

   @Test
   public void testRaftEngine() throws IOException {
      for (int i = 1; i <= NUM_PEERS; i++) {
         Config cfg = new Config().setLogDir(logDirs[i - 1]).setClusterName("TEST");
         RaftEngine<TestStateMachine> raft = new RaftEngine<TestStateMachine>(cfg, new TestStateMachine.Factory(), this);
         raft.setPeerId(i);
         for (int j = 1; j <= NUM_PEERS; j++) {
            if (j != i) {
               raft.addPeer(j);
            }
         }
         rafts.put(i, raft);
      }

      for (RaftEngine<?> raft : rafts.values()) {
         raft.start(raft.getPeerId());
      }

      logRafts();
      sleep(5000);
      logRafts();

      Thread t = new Thread(() -> {
         sleep(3000);
         while (true) {
            try {
               synchronized (rafts) {
                  for (RaftEngine<TestStateMachine> raft : rafts.values()) {
                     raft.executeCommand(new TestStateMachine.TestCommand(random.nextLong()), null);
                  }
               }
               sleep(5);
            } catch (Throwable t1) {
               logger.error(t1.getMessage(), t1);
            }
         }
      });
      t.start();

      sleep(1000);

      // WARNING: this will currently run until OOM since we don't have log compaction or journaling yet
      while (true) {

         synchronized (rafts) {
            sleep(1000);
            logRafts();
         }

         sleep(1000);

         //         if (dead == null) {
         //            if (random.nextDouble() < 0.1) {
         //               List<Integer> items = new ArrayList<Integer>(rafts.keySet());
         //               int peerId = items.get(random.nextInt(items.size()));
         //               synchronized (rafts) {
         //                  dead = rafts.remove(peerId);
         //               }
         //               logger.info("Killing {}", dead);
         //               dead.stop();
         //            }
         //         } else {
         //            if (random.nextDouble() < 0.25) {
         //               synchronized (rafts) {
         //                  rafts.put(dead.getPeerId(), dead);
         //               }
         //               logger.info("Reviving {}", dead);
         //               dead.start();
         //               dead.DEBUG = true;
         //               dead = null;
         //
         //            }
         //         }

         sleep(1000);
      }

   }

   void checkConsistency() {

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

      long maxIndex = 0;
      long minIndex = Integer.MAX_VALUE;
      for (RaftEngine<TestStateMachine> raft : rafts.values()) {
         maxIndex = Math.max(raft.getLog().getLastIndex(), maxIndex);
         minIndex = Math.min(raft.getLog().getFirstIndex(), minIndex);
      }
      for (long index = minIndex; index <= maxIndex; index++) {
         Entry<TestStateMachine> entry = null;
         for (RaftEngine<TestStateMachine> raft : rafts.values()) {
            if (entry == null) {
               entry = raft.getLog().getEntry(index);
            } else {
               Entry<TestStateMachine> e = raft.getLog().getEntry(index);
               if (e != null) {
                  Assert.assertEquals(entry.index, e.index);
                  Assert.assertEquals(entry.term, e.term);
                  Assert.assertEquals(((TestCommand) entry.command).getVal(), ((TestCommand) e.command).getVal());
               }
            }
         }
      }

   }

   private void logRafts() {
      logger.info("=====================================================================================================================");
      for (RaftEngine<?> raft : rafts.values()) {
         logger.info(String.format("%d) %9s term=%d, lastIndex=%d, lastTerm=%d commitIndex=%d, %s", raft.getPeerId(), raft.getRole(),
               raft.getCurrentTerm(), raft.getLog().getLastIndex(), raft.getLog().getLastTerm(), raft.getLog().getCommitIndex(),
               raft.getStateMachine(), raft.getLog().getEntries().size()));
      }
      logger.info("=====================================================================================================================");
      logger.info("");
      //   checkConsistency();
   }

   private void sleep(int millis) {
      try {
         Thread.sleep(millis);
      } catch (InterruptedException e) {}
   }

   @Override
   public void sendRequestVote(final String clusterName, int peerId, final long term, final int candidateId, final long lastLogIndex,
         final long lastLogTerm, final VoteResponseHandler handler) {
      final RaftEngine<?> r = rafts.get(peerId);
      if (r != null) {
         executor.schedule(() -> {
            try {
               r.handleVoteRequest(clusterName, term, candidateId, lastLogIndex, lastLogTerm, handler);
            } catch (Throwable t) {
               logger.error(t.getMessage(), t);
            }

         }, randomDelay(), TimeUnit.MILLISECONDS);
      }
   }

   @Override
   public void sendAppendEntries(int peerId, final long term, final int leaderId, final long prevLogIndex, final long prevLogTerm,
         final Entry<TestStateMachine>[] entries, final long leaderCommit, final AppendEntriesResponseHandler handler) {
      final RaftEngine<TestStateMachine> r = rafts.get(peerId);
      if (r != null) {
         executor.schedule(() -> {
            try {
               r.handleAppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit, handler);
            } catch (Throwable t) {
               logger.error(t.getMessage(), t);
            }
         }, randomDelay(), TimeUnit.MILLISECONDS);
      }
   }

   @Override
   public void sendInstallSnapshot(int peerId, final long term, final long index, final long length, final int partSize, final int part,
         final byte[] data, final InstallSnapshotResponseHandler handler) {
      final RaftEngine<TestStateMachine> r = rafts.get(peerId);
      if (r != null) {
         executor.schedule(() -> {
            try {
               r.handleInstallSnapshotRequest(term, index, length, partSize, part, data, handler);
            } catch (Throwable t) {
               logger.error(t.getMessage(), t);
            }
         }, randomDelay(), TimeUnit.MILLISECONDS);
      }
   }

   @Override
   public void sendIssueCommand(int peerId, final Command<TestStateMachine> command, final ClientResponseHandler<TestStateMachine> handler) {
      final RaftEngine<TestStateMachine> r = rafts.get(peerId);
      if (r != null) {
         executor.schedule(() -> {
            try {
               r.handleClientRequest(command, handler);
            } catch (Throwable t) {
               logger.error(t.getMessage(), t);
            }
         }, randomDelay(), TimeUnit.MILLISECONDS);
      }
   }

}
