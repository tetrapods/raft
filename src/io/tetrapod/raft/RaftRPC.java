package io.tetrapod.raft;

/**
 * Delegates all the asynchronous RPC implementation for raft to a third party.
 */
public interface RaftRPC<T extends StateMachine<T>> {

   ///////// Request Handlers ///////// 

   public interface Requests<T extends StateMachine<T>> {
      public void handleVoteRequest(String clusterName, long term, int candidateId, long lastLogIndex, long lastLogTerm,
            VoteResponseHandler handler);

      public void handleAppendEntriesRequest(long term, int leaderId, long prevLogIndex, long prevLogTerm, Entry<T>[] entries,
            long leaderCommit, AppendEntriesResponseHandler handler);

      public void handleInstallSnapshotRequest(long term, long index, long length, int partSize, int part, byte[] data,
            InstallSnapshotResponseHandler handler);

      public void handleClientRequest(Command<T> command, ClientResponseHandler<T> handler);
   }

   ///////// Request Senders ///////// 

   public void sendRequestVote(String clusterName, int peerId, long term, int candidateId, long lastLogIndex, long lastLogTerm,
         VoteResponseHandler handler);

   public void sendAppendEntries(int peerId, long term, int leaderId, long prevLogIndex, long prevLogTerm, Entry<T>[] entries,
         long leaderCommit, AppendEntriesResponseHandler handler);

   public void sendInstallSnapshot(int peerId, long term, long index, long length, int partSize, int part, byte[] data,
         InstallSnapshotResponseHandler handler);

   public void sendIssueCommand(int peerId, final Command<T> command, ClientResponseHandler<T> handler);

   ///////// Response Handlers ///////// 

   public interface VoteResponseHandler {
      public void handleResponse(long term, boolean voteGranted);
   }

   public interface AppendEntriesResponseHandler {
      public void handleResponse(long term, boolean success, long lastLogIndex);
   }

   public interface InstallSnapshotResponseHandler {
      public void handleResponse(boolean success);
   }

   public interface ClientResponseHandler<T extends StateMachine<T>> {
      public void handleResponse(final Command<T> command);
   }

}
