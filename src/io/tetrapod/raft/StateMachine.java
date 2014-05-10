package io.tetrapod.raft;

import java.io.*;
import java.util.*;

/**
 * The state machine applies commands to update state.
 * 
 * It contains the state we want to coordinate across a distributed cluster.
 */
public abstract class StateMachine {

   private Map<Integer, Class<Command<?>>> commandTypes = new HashMap<>();
   private long                            index;
   private long                            term;

   public void saveState(DataOutputStream out) throws IOException {}

   public void loadState(DataInputStream in) throws IOException {}

   public void restoreToIndex(long index) {/*TODO*/}

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

   public Command<?> makeCommand(int id) throws IOException {
      final Class<Command<?>> c = (Class<Command<?>>) commandTypes.get(id);
      try {
         return c.newInstance();
      } catch (Exception e) {
         throw new RuntimeException("Failed to create class for id=" + id, e);
      }
   }

}
