package io.tetrapod.raft;

import io.tetrapod.raft.RaftRPC.ClientResponseHandler;

public class PendingCommand<T extends StateMachine<T>> implements Comparable<PendingCommand<T>> {
   public final Entry<T>                 entry;
   public final ClientResponseHandler<T> handler;

   public PendingCommand(Entry<T> entry, ClientResponseHandler<T> handler) {
      this.entry = entry;
      this.handler = handler;
   }

   @Override
   public int compareTo(PendingCommand<T> o) {
      final Long index = entry.index;
      return index.compareTo(o.entry.index);
   }
}
