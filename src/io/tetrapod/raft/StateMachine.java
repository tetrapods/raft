package io.tetrapod.raft;

import java.io.*;
import java.util.zip.*;

/**
 * The state machine applies commands to update state.
 * 
 * It contains the state we want to coordinate across a distributed cluster.
 */
public abstract class StateMachine<T extends StateMachine<T>> {
   public static final int  SNAPSHOT_FILE_VERSION = 1;

   private long             index;
   private long             term;

   private volatile boolean loading               = false;

   public StateMachine() {}

   public abstract Command<T> makeCommand(int id);

   public abstract void saveState(DataOutputStream out) throws IOException;

   public abstract void loadState(DataInputStream in) throws IOException;

   public void writeSnapshot(File file) throws IOException {
      try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(file))))) {
         out.writeInt(SNAPSHOT_FILE_VERSION);
         out.writeLong(term);
         out.writeLong(index);
         saveState(out);
      }
   }

   public void readSnapshot(File file) throws IOException {
      loading = true;
      try (DataInputStream in = new DataInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file))))) {
         int version = in.readInt();
         assert (version <= SNAPSHOT_FILE_VERSION);
         term = in.readLong();
         index = in.readLong();
         loadState(in);
      } finally {
         loading = false;
      }
   }

   public boolean isLoading() {
      return loading;
   }

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

   public static interface Factory<T> {
      public T makeStateMachine();
   }

}
