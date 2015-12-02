package io.tetrapod.raft;

import io.tetrapod.raft.RaftRPC.AppendEntriesResponseHandler;
import io.tetrapod.raft.RaftRPC.ClientResponseHandler;
import io.tetrapod.raft.RaftRPC.InstallSnapshotResponseHandler;
import io.tetrapod.raft.RaftRPC.VoteResponseHandler;
import io.tetrapod.raft.StateMachine.Listener;

import java.io.*;
import java.security.SecureRandom;
import java.util.*;

import org.slf4j.*;

/**
 * Major TODOS:
 * <ul>
 * <li>Smooth Cluster membership changes</li>
 * <li>More Unit Tests & Robust Simulator
 * <li>Idempotent client requests
 * </ul>
 */

public class RaftEngine<T extends StateMachine<T>> implements RaftRPC.Requests<T>, Listener<T> {

   public static final Logger logger = LoggerFactory.getLogger(RaftEngine.class);

   /**
    * These are the major raft roles we can be in
    */
   public enum Role {
      Joining, Observer, Follower, Candidate, Leader, Failed, Leaving
   }

   public volatile boolean                     DEBUG             = false;

   private final SecureRandom                  random            = new SecureRandom();
   private final Map<Integer, Peer>            peers             = new HashMap<>();
   private final LinkedList<PendingCommand<T>> pendingCommands   = new LinkedList<>();
   private final Log<T>                        log;
   private final RaftRPC<T>                    rpc;
   private final Config                        config;

   private Role                                role              = Role.Joining;
   private int                                 myPeerId;
   private long                                currentTerm;
   private int                                 votedFor;
   private int                                 leaderId;
   private long                                electionTimeout;
   private long                                firstIndexOfTerm;
   private long                                lastTermCommitted = 0;

   public static class Peer {
      public final int peerId;
      private long     lastAppendMillis;
      private long     nextIndex = 1;
      private long     matchIndex;
      private boolean  appendPending;
      private boolean  fresh     = true;
      private File     snapshotTransfer;

      public Peer(int peerId) {
         this.peerId = peerId;
      }

      @Override
      public String toString() {
         return String.format("Peer-%d", peerId);
      }
   }

   public static class Value<X> {
      public X val;

      public Value(X val) {
         this.val = val;
      }
   }

   public RaftEngine(Config config, StateMachine.Factory<T> stateMachineFactory, RaftRPC<T> rpc) throws IOException {
      this.rpc = rpc;
      this.config = config;
      T stateMachine = stateMachineFactory.makeStateMachine();
      stateMachine.addListener(this);
      this.log = new Log<T>(config, stateMachine);
      this.lastTermCommitted = this.currentTerm = log.getLastTerm();
   }

   public synchronized void start(int peerId) {
      setPeerId(peerId);
      this.role = Role.Follower;
      rescheduleElection();
      this.electionTimeout += 2000; // add an initial grace period on startup
      launchPeriodicTasksThread();
   }

   public synchronized void startAsObserver() {
      this.role = Role.Observer;
      this.electionTimeout = Long.MAX_VALUE;
   }

   public synchronized void stop() {
      clearAllPendingRequests();
      log.stop();
      for (Peer p : peers.values()) {
         logger.info(" - {} has matchIndex {}", p, p.matchIndex);
         if (role == Role.Leader) {
            // be nice and send a final update to all the peers with our  current commit index before we shutdown
            long prevLogIndex = p.nextIndex - 1;
            long prevLogTerm = log.getTerm(prevLogIndex);
            rpc.sendAppendEntries(p.peerId, currentTerm, myPeerId, prevLogIndex, prevLogTerm, null, log.getCommitIndex(), null);
         }
      }
      role = Role.Leaving;
      logger.info("Raft Stopped");
   }

   @Override
   public synchronized String toString() {
      return String.format("Raft[%d] %s", myPeerId, role);
   }

   public synchronized T getStateMachine() {
      return log.getStateMachine();
   }

   public synchronized void setPeerId(int peerId) {
      this.myPeerId = peerId;
      peers.remove(peerId);
   }

   public int getPeerId() {
      return myPeerId;
   }

   public synchronized int getClusterSize() {
      return 1 + peers.size();
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

   public synchronized int getLeader() {
      return leaderId;
   }

   public boolean isValidPeer(int peerId) {
      return peers.containsKey(peerId);
   }

   private synchronized void rescheduleElection() {
      this.electionTimeout = System.currentTimeMillis() + config.getElectionTimeoutFixedMillis()
               + random.nextInt(config.getElectionTimeoutRandomMillis());
   }

   private void launchPeriodicTasksThread() {
      final Thread t = new Thread(() -> {
         while (getRole() != Role.Leaving) {
            try {
               runPeriodicTasks();
               Thread.sleep(10);
            } catch (Throwable e) {
               logger.error(e.getMessage(), e);
            }
         }
      } , "RaftEngine");
      t.start();
   }

   /**
    * Called periodically to do recurring work
    */
   private synchronized void runPeriodicTasks() {
      if (!log.isRunning() && role != Role.Leaving) {
         role = Role.Failed;
      }
      switch (role) {
         case Joining:
            if (role == Role.Joining) {
               role = Role.Follower;
               rescheduleElection();
            }
            break;
         case Observer:
            break;
         case Follower:
         case Candidate:
            if (System.currentTimeMillis() > electionTimeout) {
               callElection();
            }
            updatePendingRequests();
            break;
         case Leader:
            updateCommitIndex();
            updatePeers();
            updatePendingRequests();
            break;
         case Failed:
         case Leaving:
            break;
      }
   }

   private synchronized boolean isCommittable(long index) {
      int count = 1;
      int needed = 1 + (1 + peers.size()) / 2;

      for (Peer p : peers.values()) {
         if (p.matchIndex >= index) {
            count++;
            if (count >= needed)
               return true;
         }
      }
      return count >= needed;
   }

   private synchronized void updateCommitIndex() {
      assert (role == Role.Leader);
      // we can't commit anything until we've replicated something from this term
      if (isCommittable(firstIndexOfTerm)) {
         // we can commit any entry a majority of peers have replicated
         long index = log.getLastIndex();
         for (Peer peer : peers.values()) {
            index = Math.min(index, peer.matchIndex);
         }
         index = Math.max(index, log.getCommitIndex());
         while (index <= log.getLastIndex() && isCommittable(index)) {

            // Logging for PT #102932910
            Entry<?> e = log.getEntry(index);
            if (e != null && lastTermCommitted != e.term) {
               logger.info("Committed new term {}", e.term);
               peers.forEach((k, p) -> logger.info(" - {} has matchIndex {} >= {} ({})", p, p.matchIndex, firstIndexOfTerm,
                        p.matchIndex >= firstIndexOfTerm));
               lastTermCommitted = e.term;
            }

            log.setCommitIndex(index);
            index++;
         }
      }
   }

   private synchronized void callElection() {
      log.updateStateMachine();

      if (role == Role.Leaving) {
         logger.error("Can't call election when leaving");
         return;
      }

      final int votesNeeded = (1 + peers.size()) / 2;
      final Value<Integer> votes = new Value<>(1);
      role = Role.Candidate;
      ++currentTerm;
      leaderId = 0;
      votedFor = myPeerId;
      logger.info("{} is calling an election (term {})", this, currentTerm);
      if (peers.size() > 0) {

         for (Peer peer : peers.values()) {
            peer.nextIndex = 1;
            peer.matchIndex = 0;
            rpc.sendRequestVote(config.getClusterName(), peer.peerId, currentTerm, myPeerId, log.getLastIndex(), log.getLastTerm(),
                     (term, voteGranted) -> {
                        synchronized (RaftEngine.this) {
                           if (!stepDown(term)) {
                              if (term == currentTerm && role == Role.Candidate) {
                                 if (voteGranted) {
                                    votes.val++;
                                 }
                                 if (votes.val > votesNeeded) {
                                    becomeLeader();
                                 }
                              }
                           }
                        }
                     });
         }
      } else {
         becomeLeader();
      }
      rescheduleElection();
   }

   @Override
   public synchronized void handleVoteRequest(String clusterName, long term, int candidateId, long lastLogIndex, long lastLogTerm,
            VoteResponseHandler handler) {
      if (!config.getClusterName().equals(clusterName) || !isValidPeer(candidateId)) {
         return;
      }
      if (term > currentTerm) {
         stepDown(term);
      }
      if (term >= currentTerm && (votedFor == 0 || votedFor == candidateId) && lastLogIndex >= log.getLastIndex()
               && lastLogTerm >= log.getLastTerm()) {
         votedFor = candidateId;
         rescheduleElection();

         logger.info(String.format("%s I'm voting YES for %d (term %d)", this, candidateId, currentTerm));
         handler.handleResponse(currentTerm, true);
      } else {
         logger.info(String.format("%s I'm voting NO for %d (term %d)", this, candidateId, currentTerm));
         handler.handleResponse(currentTerm, false);
      }
   }

   private synchronized boolean stepDown(long term) {
      if (term > currentTerm) {
         currentTerm = term;
         votedFor = 0;
         if (role == Role.Candidate || role == Role.Leader) {
            logger.info("{} is stepping down (term {})", this, currentTerm);
            role = Role.Follower;
            clearAllPendingRequests();
         }
         rescheduleElection();
         return true;
      }
      return false;
   }

   public synchronized void becomeLeader() {
      logger.info("{} is becoming the leader (term {})", this, currentTerm);
      role = Role.Leader;
      leaderId = myPeerId;
      firstIndexOfTerm = log.getLastIndex() + 1;
      for (Peer peer : peers.values()) {
         peer.matchIndex = 0;
         peer.nextIndex = log.getLastIndex() + 1;
         peer.appendPending = false;
         assert peer.nextIndex != 0;
      }

      // Force a new term command to mark the occasion and hasten
      // commitment of any older entries in our log from the 
      // previous term
      executeCommand(new NewTermCommand<T>(myPeerId, currentTerm), null);

      updatePeers();
   }

   /**
    * As leader, we need to make sure we continually keep our peers up to date, and when no entries are made, to send a heart beat so that
    * they do not call an election
    */
   private synchronized void updatePeers() {
      assert role == Role.Leader;
      peers.forEach((peerId, peer) -> updatePeer(peer));
   }

   private synchronized void updatePeer(final Peer peer) {
      final long now = System.currentTimeMillis();
      if (peer.appendPending && now > peer.lastAppendMillis + 5000) {
         peer.appendPending = false; // time out the last append
      }
      if (!peer.appendPending && (peer.nextIndex < log.getLastIndex() || now > peer.lastAppendMillis + config.getHeartbeatMillis())) {
         assert (peer.nextIndex > 0);

         // for a fresh peer we'll start with an empty list of entries so we can learn what index the node is already on in it's log
         // fetch entries from log to send to the peer
         final Entry<T>[] entries = (!peer.fresh && peer.snapshotTransfer == null)
                  ? log.getEntries(peer.nextIndex, config.getMaxEntriesPerRequest()) : null;

         // if this peer needs entries we no longer have, then send them a snapshot
         if (!peer.fresh && peer.nextIndex > 0 && peer.nextIndex < log.getFirstIndex() && entries == null) {
            installSnapshot(peer);
         } else {
            long prevLogIndex = peer.nextIndex - 1;
            long prevLogTerm = log.getTerm(prevLogIndex);

            logger.trace("{} is sending append entries to {}", this, peer.peerId);
            peer.lastAppendMillis = now;
            peer.appendPending = true;
            rpc.sendAppendEntries(peer.peerId, currentTerm, myPeerId, prevLogIndex, prevLogTerm, entries, log.getCommitIndex(),
                     (term, success, lastLogIndex) -> {
                        synchronized (RaftEngine.this) {
                           peer.appendPending = false;
                           if (role == Role.Leader) {
                              if (!stepDown(term)) {
                                 peer.fresh = false;
                                 if (success) {
                                    if (entries != null) {
                                       peer.matchIndex = entries[entries.length - 1].index;
                                       peer.nextIndex = peer.matchIndex + 1;
                                       assert peer.nextIndex != 0;
                                    } else {
                                       peer.nextIndex = Math.max(lastLogIndex + 1, 1);
                                    }
                                    updatePeer(peer);
                                 } else {
                                    //assert peer.nextIndex > 1 : "peer.nextIndex = " + peer.nextIndex;
                                    if (peer.nextIndex > lastLogIndex) {
                                       peer.nextIndex = Math.max(lastLogIndex + 1, 1);
                                    } else if (peer.nextIndex > 1) {
                                       peer.nextIndex--;
                                    }
                                 }
                              }
                           }
                        }
                     });
         }
      }
   }

   @Override
   public synchronized void handleAppendEntriesRequest(long term, int leaderId, long prevLogIndex, long prevLogTerm, Entry<T>[] entries,
            long leaderCommit, AppendEntriesResponseHandler handler) {

      logger.trace(String.format("%s append entries from %d: from <%d:%d>", this, leaderId, prevLogTerm, prevLogIndex));
      if (term >= currentTerm) {
         if (term > currentTerm) {
            stepDown(term);
         }
         if (this.leaderId != leaderId) {
            this.leaderId = leaderId;
            stepDown(term);
            role = Role.Follower;
         }
         rescheduleElection();

         if (log.isConsistentWith(prevLogIndex, prevLogTerm)) {
            if (entries != null) {
               for (Entry<T> e : entries) {
                  if (!log.append(e)) {
                     logger.warn(String.format("%s is failing append entries from %d: %s", this, leaderId, e));
                     handler.handleResponse(currentTerm, false, log.getLastIndex());
                     stop();
                     return;
                  }
               }
            }

            log.setCommitIndex(Math.min(leaderCommit, log.getLastIndex()));

            logger.trace("{} is fine with append entries from {}", this, leaderId);
            handler.handleResponse(currentTerm, true, log.getLastIndex());
            return;
         } else {
            logger.warn("{} is failing with inconsistent append entries from {}", this, leaderId);
            logger.warn("Leader prevLogTerm={}, prevLogIndex={}", prevLogTerm, prevLogIndex);
            logger.warn("Follower firstTerm={}, firstIndex={}", log.getFirstTerm(), log.getFirstIndex());
            logger.warn("Follower lastTerm={}, lastIndex={}", log.getLastTerm(), log.getLastIndex());

            if (prevLogIndex > log.getCommitIndex()) {
               log.wipeConflictedEntries(prevLogIndex);
            }

         }
      }

      logger.trace("{} is rejecting append entries from {}", this, leaderId);
      handler.handleResponse(currentTerm, false, log.getLastIndex());
   }

   private synchronized void installSnapshot(final Peer peer) {
      if (peer.snapshotTransfer == null) {
         peer.snapshotTransfer = new File(log.getLogDirectory(), "raft.snapshot");
         installSnapshot(peer, 0);
      }
   }

   private synchronized void installSnapshot(final Peer peer, final int part) {
      // we don't have log entries this old on record, so we need to send them a viable snapshot instead
      // we need to be able to send this snapshot before we delete log entries after the snapshot, or
      // we won't be able to catch them up. We also need to make sure we don't delete the snapshot file
      // we're sending. 
      if (peer.snapshotTransfer != null) {
         logger.info("Installing Snapshot to {} Part #{}", peer, part);
         final long snapshotIndex = StateMachine.getSnapshotIndex(peer.snapshotTransfer);
         if (snapshotIndex > 0) {
            final int partSize = config.getSnapshotPartSize();
            final long len = peer.snapshotTransfer.length();
            final byte data[] = RaftUtil.getFilePart(peer.snapshotTransfer, part * partSize,
                     (int) Math.min(partSize, len - part * partSize));

            rpc.sendInstallSnapshot(peer.peerId, currentTerm, myPeerId, len, partSize, part, data, new InstallSnapshotResponseHandler() {
               @Override
               public void handleResponse(boolean success) {
                  synchronized (RaftEngine.this) {
                     if (success) {
                        if ((part + 1) * partSize < len) {
                           // send the next part
                           installSnapshot(peer, part + 1);
                        } else {
                           logger.info("InstallSnapshot: done-{}", peer.nextIndex);
                           peer.snapshotTransfer = null;
                           peer.nextIndex = snapshotIndex + 1;
                        }
                     } else {
                        logger.error("{} Failed to install snapshot on {}", this, peer);
                        // TODO: Hmmmmm
                        //peer.snapshotTransfer = null;
                     }
                  }
               }
            });
         }
      }
   }

   @Override
   public void handleInstallSnapshotRequest(long term, long index, long length, int partSize, int part, byte[] data,
            InstallSnapshotResponseHandler handler) {
      logger.info("handleInstallSnapshot: length={} part={}", length, part);
      rescheduleElection();

      File file = new File(log.getLogDirectory(), "raft.installing.snapshot");
      if (file.exists() && part == 0) {
         file.delete();
      }

      if (part == 0 || file.exists()) {
         if (file.length() == partSize * part) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
               raf.seek(partSize * part);
               raf.write(data);

               if (raf.length() == length) {
                  file.renameTo(new File(log.getLogDirectory(), "raft.snapshot"));
                  log.loadSnapshot();
                  rescheduleElection();
               }

               handler.handleResponse(true);
               return;
            } catch (IOException e) {
               logger.error(e.getMessage(), e);
            }
         }
      }
      handler.handleResponse(false);
   }

   @Override
   public synchronized void handleClientRequest(Command<T> command, ClientResponseHandler<T> handler) {
      executeCommand(command, handler);
   }

   public synchronized boolean executeCommand(Command<T> command, ClientResponseHandler<T> handler) {
      if (role == Role.Leader) {
         final Entry<T> e = log.append(currentTerm, command);
         if (e != null) {
            if (handler != null) {
               synchronized (pendingCommands) {
                  pendingCommands.add(new PendingCommand<T>(e, handler));
               }
            }
            return true;
         }
         if (handler != null) {
            handler.handleResponse(null);
         }
      }
      return false;
   }

   public void executeAfterCommandProcessed(final Entry<T> e, ClientResponseHandler<T> handler) {
      if (e.index <= log.getStateMachineIndex()) {
         handler.handleResponse(e);
      } else {
         synchronized (pendingCommands) {
            pendingCommands.add(new PendingCommand<T>(e, handler));
         }
      }
   }

   /**
    * Pop all the pending command requests from our list that are now safely replicated to the majority and applied to our state machine
    */
   private void updatePendingRequests() {

      synchronized (pendingCommands) {
         while (!pendingCommands.isEmpty()) {
            //  logger.info("Updating All Pending Requests {} > {} ", pendingCommands.size(), log.getCommitIndex());
            final PendingCommand<T> item = pendingCommands.poll();
            if (item.entry.index <= log.getStateMachineIndex()) {
               logger.debug("Returning Pending Command Response To Client {}", item.entry);
               item.handler.handleResponse(item.entry);
            } else {
               pendingCommands.addFirst(item);
               return;
            }
         }
      }
   }

   private synchronized void clearAllPendingRequests() {
      logger.info("Clearing All Pending Requests");
      synchronized (pendingCommands) {
         pendingCommands.forEach((item) -> item.handler.handleResponse(null));
         pendingCommands.clear();
      }
   }

   public synchronized void addPeer(int peerId) {
      if (peerId != 0 && peerId != this.myPeerId) {
         peers.put(peerId, new Peer(peerId));
      }
   }

   @Override
   public void onLogEntryApplied(Entry<T> entry) {
      //      final Command<T> command = entry.getCommand();
      //      if (command.getCommandType() == StateMachine.COMMAND_ID_ADD_PEER) {
      //         final AddPeerCommand<T> addPeerCommand = ((AddPeerCommand<T>) command);
      //         if (addPeerCommand.bootstrap) {
      //            logger.info(" ********************** BOOTSTRAP **********************", addPeerCommand.peerId);
      //            peers.clear();
      //         }
      //         if (addPeerCommand.peerId != this.myPeerId) {
      //            logger.info(" ********************** AddPeer #{} ********************** ", addPeerCommand.peerId);
      //            peers.put(addPeerCommand.peerId, new Peer(addPeerCommand.peerId));
      //         }
      //      } else if (command.getCommandType() == StateMachine.COMMAND_ID_DEL_PEER) {
      //         final DelPeerCommand<T> delPeerCommand = ((DelPeerCommand<T>) command);
      //         logger.info(" ********************** DelPeer #{} ********************** ", delPeerCommand.peerId);
      //         peers.remove(delPeerCommand.peerId);
      //      }
   }

   public Collection<Peer> getPeers() {
      return peers.values();
   }
}
