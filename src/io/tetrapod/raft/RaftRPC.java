package io.tetrapod.raft;

/**
 * Delegates all the asynchronous RPC implementation for raft to a third party.
 */
public interface RaftRPC {

   public interface Requests {
      public RequestVoteResponse handleRequestVote(String clusterName, long term, int candidateId, long lastLogIndex, long lastLogTerm);

      public AppendEntriesResponse handleAppendEntries(long term, int leaderId, long prevLogIndex, long prevLogTerm, Entry<?>[] entries,
            long leaderCommit);
   }

   public void sendRequestVote(String clusterName, int peerId, long term, int candidateId, long lastLogIndex, long lastLogTerm,
         RequestVoteResponseHandler handler);

   public interface RequestVoteResponseHandler {
      public void handleResponse(long term, boolean voteGranted);
   }

   public class RequestVoteResponse {
      final long    term;
      final boolean voteGranted;

      public RequestVoteResponse(long term, boolean voteGranted) {
         this.term = term;
         this.voteGranted = voteGranted;
      }
   }

   public void sendAppendEntries(int peerId, long term, int leaderId, long prevLogIndex, long prevLogTerm, Entry<?>[] entries,
         long leaderCommit, AppendEntriesResponseHandler handler);

   public interface AppendEntriesResponseHandler {
      public void handleResponse(long term, boolean success, long lastLogIndex);
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

}
