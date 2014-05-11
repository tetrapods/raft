package io.tetrapod.raft;

import io.tetrapod.raft.RaftRPC.AppendEntriesResponse;
import io.tetrapod.raft.RaftRPC.AppendEntriesResponseHandler;
import io.tetrapod.raft.RaftRPC.RequestVoteResponse;

import java.io.File;
import java.security.SecureRandom;
import java.util.*;

import org.slf4j.*;

public class RaftEngine<T extends StateMachine<T>> implements RaftRPC.Requests {

   public static final Logger logger                         = LoggerFactory.getLogger(RaftEngine.class);

   public static final int    ELECTION_TIMEOUT_FIXED_MILLIS  = 1000;
   public static final int    ELECTION_TIMEOUT_RANDOM_MILLIS = 2000;
   public static final int    HEARTBEAT_MILLIS               = 150;

   /**
    * These are the major raft roles we can be in
    */
   public enum Role {
      Joining, Observer, Follower, Candidate, Leader, Leaving
   }

   public final SecureRandom        random = new SecureRandom();
   private final Map<Integer, Peer> peers  = new HashMap<Integer, Peer>();
   private final Log<T>             log;
   private final RaftRPC            rpc;

   /**
    * The state machine we are coordinating via raft
    */
   private final T                  stateMachine;
   private final String             clusterName;

   private Role                     role   = Role.Joining;
   private int                      myPeerId;
   private long                     currentTerm;
   private int                      votedFor;
   private long                     electionTimeout;

   public class Peer {
      private final int peerId;
      private Role      role;
      private long      lastAppendMillis;
      private long      nextIndex;
      private long      matchIndex;

      public Peer(int peerId) {
         this.peerId = peerId;
      }
   }

   public class Value<X> {
      public X val;

      public Value(X val) {
         this.val = val;
      }
   }

   public RaftEngine(File logDir, String clusterName, T stateMachine, RaftRPC rpc) {
      this.log = new Log<T>(logDir);
      this.rpc = rpc;
      this.stateMachine = stateMachine;
      this.clusterName = clusterName;
   }

   public synchronized void start() {
      assert (myPeerId != 0);
      role = Role.Follower; // hack
      scheduleElection();
      launchPeriodicTasksThread();
   }

   public String getClusterName() {
      return clusterName;
   }

   public synchronized void setPeerId(int peerId) {
      this.myPeerId = peerId;
   }

   public int getPeerId() {
      return myPeerId;
   }

   public synchronized Role getRole() {
      return role;
   }

   public synchronized long getCurrentTerm() {
      return currentTerm;
   }

   public Log<T> getLog() {
      return log;
   }

   public synchronized void addPeer(int peerId) {
      peers.put(peerId, new Peer(peerId));
   }

   private synchronized void scheduleElection() {
      this.electionTimeout = System.currentTimeMillis() + ELECTION_TIMEOUT_FIXED_MILLIS + random.nextInt(ELECTION_TIMEOUT_RANDOM_MILLIS);
   }

   private void launchPeriodicTasksThread() {
      final Thread t = new Thread(new Runnable() {
         public void run() {
            while (getRole() != Role.Leaving) {
               try {
                  runPeriodicTasks();
                  Thread.sleep(10);
               } catch (Throwable t) {
                  logger.error(t.getMessage(), t);
               }
            }
         }
      }, "RaftEngine");
      t.start();
   }

   /**
    * Called periodically to do recurring work
    */
   private synchronized void runPeriodicTasks() {
      // TODO: probably do this elsewhere
      while (log.getCommitIndex() > stateMachine.getIndex()) {
         final Entry<T> e = log.getEntry(stateMachine.getIndex() + 1);
         e.command.applyTo(stateMachine);
         stateMachine.apply(e.index, e.term);
      }

      switch (role) {
         case Joining:
            break;
         case Observer:
            break;
         case Follower:
         case Candidate:
            if (System.currentTimeMillis() > electionTimeout) {
               callElection();
            }
            break;
         case Leader:
            updatePeers();
            break;
         case Leaving:
            break;
      }
   }

   @Override
   public synchronized String toString() {
      return String.format("Raft[%d] %s", myPeerId, role);
   }

   private synchronized void callElection() {
      final int votesNeeded = 1 + (peers.size() / 2);
      final Value<Integer> votes = new Value<>(1);
      role = Role.Candidate;
      ++currentTerm;
      votedFor = myPeerId;
      logger.info("{} is calling an election (term {})", this, currentTerm);
      for (Peer peer : peers.values()) {
         rpc.sendRequestVote(peer.peerId, currentTerm, myPeerId, log.getLastIndex(), log.getLastTerm(),
               new RaftRPC.RequestVoteResponseHandler() {
                  @Override
                  public void handleResponse(long term, boolean voteGranted) {
                     synchronized (RaftEngine.this) {
                        if (term == currentTerm && role == Role.Candidate) {
                           if (voteGranted) {
                              votes.val++;
                           }
                           if (votes.val >= votesNeeded) {
                              becomeLeader();
                           }
                        }
                     }
                  }
               });
      }
      scheduleElection();
   }

   @Override
   public synchronized RequestVoteResponse handleRequestVote(long term, int candidateId, long lastLogIndex, long lastLogTerm) {
      if (term > currentTerm) {
         stepDown(term);
      }
      if (term >= currentTerm && votedFor == 0 || votedFor == candidateId && lastLogIndex >= log.getLastIndex()
            && lastLogTerm >= log.getLastTerm()) {
         votedFor = candidateId;
         scheduleElection();
         return new RequestVoteResponse(currentTerm, true);
      }
      return new RequestVoteResponse(currentTerm, false);
   }

   private synchronized void stepDown(long term) {
      currentTerm = term;
      votedFor = 0;
      if (role == Role.Candidate || role == Role.Leader) {
         logger.info("{} is stepping down (term {})", this, currentTerm);
         role = Role.Follower;
      }
      scheduleElection();
   }

   private synchronized void becomeLeader() {
      logger.info("{} is becoming the leader (term {})", this, currentTerm);
      role = Role.Leader;
      updatePeers();
   }

   /**
    * As leader, we need to make sure we continually keep our peers up to date, and when no entries are made, to send a heart beat so that
    * they do not call an election
    */
   private synchronized void updatePeers() {
      assert role == Role.Leader;
      for (Peer peer : peers.values()) {
         if (peer.nextIndex < log.getLastIndex() || System.currentTimeMillis() > peer.lastAppendMillis + HEARTBEAT_MILLIS) {
            updatePeer(peer);
         }
      }
   }

   private synchronized void updatePeer(Peer peer) {
      rpc.sendAppendEntries(peer.peerId, currentTerm, myPeerId, 0, 0, null, log.getCommitIndex(), new AppendEntriesResponseHandler() {
         @Override
         public void handleResponse(long term, boolean success) {
            synchronized (RaftEngine.this) {

            }
         }
      });
   }

   @Override
   public synchronized AppendEntriesResponse handleAppendEntries(long term, int leaderId, long prevLogIndex, long prevLogTerm,
         Entry<?>[] entries, long leaderCommit) {

      scheduleElection();
      return new AppendEntriesResponse(currentTerm, false);
   }

}
