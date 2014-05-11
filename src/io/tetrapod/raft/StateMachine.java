package io.tetrapod.raft;

import java.io.*;

/**
 * The state machine applies commands to update state.
 * 
 * It contains the state we want to coordinate across a distributed cluster.
 */
public abstract class StateMachine<T extends StateMachine<T>> {

   private long index;
   private long term;

   public StateMachine() {}

   public void saveState(DataOutputStream out) throws IOException {}

   public void loadState(DataInputStream in) throws IOException {}

   public long getIndex() {
      return index;
   }

   public long getTerm() {
      return term;
   }

   protected void apply(long index, long term) {
      assert (this.index + 1 == index);
      assert (this.term <= term);
      this.index = index;
      this.term = term;
   }

   public abstract Command<T> makeCommand(int id);

}
