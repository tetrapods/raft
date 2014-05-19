package io.tetrapod.raft;

/**
 * Delegates all the asynchronous RPC implementation for raft to a third party.
 */
public interface RaftRPC {

   ///////// Request Handlers ///////// 

   public interface Requests {
      public RequestVoteResponse handleRequestVote(String clusterName, long term, int candidateId, long lastLogIndex, long lastLogTerm);

      public AppendEntriesResponse handleAppendEntries(long term, int leaderId, long prevLogIndex, long prevLogTerm, Entry<?>[] entries,
            long leaderCommit);

      public InstallSnapshotResponse handleInstallSnapshot(long term, long index, long length, int partSize, int part, byte[] data);
   }

   ///////// Request Senders ///////// 

   public void sendRequestVote(String clusterName, int peerId, long term, int candidateId, long lastLogIndex, long lastLogTerm,
         RequestVoteResponseHandler handler);

   public void sendAppendEntries(int peerId, long term, int leaderId, long prevLogIndex, long prevLogTerm, Entry<?>[] entries,
         long leaderCommit, AppendEntriesResponseHandler handler);

   public void sendInstallSnapshot(int peerId, long term, long index, long length, int partSize, int part, byte[] data,
         InstallSnapshotResponseHandler handler);

   ///////// Response Handlers ///////// 

   public interface RequestVoteResponseHandler {
      public void handleResponse(long term, boolean voteGranted);
   }

   public interface AppendEntriesResponseHandler {
      public void handleResponse(long term, boolean success, long lastLogIndex);
   }

   public interface InstallSnapshotResponseHandler {
      public void handleResponse(boolean success);
   }

   ///////// Response Objects ///////// 

   public class RequestVoteResponse {
      final long    term;
      final boolean voteGranted;

      public RequestVoteResponse(long term, boolean voteGranted) {
         this.term = term;
         this.voteGranted = voteGranted;
      }
   }

   public class AppendEntriesResponse {
      final long    term;
      final boolean success;
      final long    lastLogIndex;

      public AppendEntriesResponse(long term, boolean success, long lastLogIndex) {
         this.term = term;
         this.success = success;
         this.lastLogIndex = lastLogIndex;
      }
   }

   public class InstallSnapshotResponse {
      final boolean success;

      public InstallSnapshotResponse(boolean success) {
         this.success = success;
      }
   }

}
