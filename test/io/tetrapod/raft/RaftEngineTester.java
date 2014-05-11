package io.tetrapod.raft;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

import org.junit.*;
import org.slf4j.*;

/**
 * Runs a full system test with fake RPC
 */
public class RaftEngineTester implements RaftRPC {

   public static final Logger              logger = LoggerFactory.getLogger(RaftEngineTester.class);

   private static ScheduledExecutorService executor;
   private static File                     logDir;

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
      return 10 + (int) (Math.random() * 100);
   }

   private Map<Integer, RaftEngine<?>> rafts = new HashMap<>();

   @Test
   public void testRaftEngine() {
      for (int i = 1; i <= 3; i++) {
         RaftEngine<?> raft = new RaftEngine<TestStateMachine>(logDir, "TEST", new TestStateMachine(), this);
         raft.setPeerId(i);
         for (int j = 1; j <= 3; j++) {
            if (j != i) {
               raft.addPeer(j);
            }
         }
         rafts.put(i, raft);
      }

      for (RaftEngine<?> raft : rafts.values()) {
         raft.start();
      }

      while (true) { 
         logger.info("======================================================================");
         for (RaftEngine<?> raft : rafts.values()) {
            logger.info(String.format("%d) %10s term=%04d, lastIndex=%04d, lastTerm=%04d", raft.getPeerId(), raft.getRole(),
                  raft.getCurrentTerm(), raft.getLog().getLastIndex(), raft.getLog().getLastTerm()));
         }
         logger.info("======================================================================");
         logger.info("");
         try {
            Thread.sleep(5000);
         } catch (InterruptedException e) {}
      }

   }

   @Override
   public void sendRequestVote(int peerId, final long term, final int candidateId, final long lastLogIndex, final long lastLogTerm,
         final RequestVoteResponseHandler handler) {
      final RaftEngine<?> r = rafts.get(peerId);
      if (r != null) {
         executor.schedule(new Runnable() {
            public void run() {
               final RequestVoteResponse res = r.handleRequestVote(term, candidateId, lastLogIndex, lastLogTerm);
               executor.schedule(new Runnable() {
                  public void run() {
                     handler.handleResponse(res.term, res.voteGranted);
                  }
               }, randomDelay(), TimeUnit.MILLISECONDS);
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
               final AppendEntriesResponse res = r.handleAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
               executor.schedule(new Runnable() {
                  public void run() {
                     handler.handleResponse(res.term, res.success);
                  }
               }, randomDelay(), TimeUnit.MILLISECONDS);
            }
         }, randomDelay(), TimeUnit.MILLISECONDS);
      }
   }

}
