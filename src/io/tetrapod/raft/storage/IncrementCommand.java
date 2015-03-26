package io.tetrapod.raft.storage;

import io.tetrapod.raft.Command;

import java.io.*;

public class IncrementCommand<T extends StorageStateMachine<T>> implements Command<T> {
   public static final int COMMAND_ID = 3;

   private String          key;
   private long            amount;
   private long            result     = 0;

   public IncrementCommand() {}

   public IncrementCommand(String key) {
      this.key = key;
      this.amount = 1;
   }

   public IncrementCommand(String key, int amount) {
      this.key = key;
      this.amount = amount;
   }

   @Override
   public void applyTo(T state) {
      result = state.increment(key, amount);
   }

   @Override
   public void write(DataOutputStream out) throws IOException {
      out.writeUTF(key);
      out.writeLong(amount);
      out.writeLong(result);
   }

   @Override
   public void read(DataInputStream in) throws IOException {
      key = in.readUTF();
      amount = in.readLong();
      result = in.readLong();
   }

   @Override
   public int getCommandType() {
      return COMMAND_ID;
   }

   public long getResult() {
      return result;
   }

}
