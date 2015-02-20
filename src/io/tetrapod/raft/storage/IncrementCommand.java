package io.tetrapod.raft.storage;

import java.io.*;

import io.tetrapod.raft.Command;

public class IncrementCommand implements Command<StorageStateMachine> {
   public static final int COMMAND_ID = 3;

   private String          key;
   private long            amount;

   public IncrementCommand() {}

   public IncrementCommand(String key) {
      this.key = key;
   }

   @Override
   public void applyTo(StorageStateMachine state) {
      state.increment(key, amount);
   }

   @Override
   public void write(DataOutputStream out) throws IOException {
      out.writeUTF(key);
      out.writeLong(amount);
   }

   @Override
   public void read(DataInputStream in) throws IOException {
      key = in.readUTF();
      amount = in.readLong();
   }

   @Override
   public int getCommandType() {
      return COMMAND_ID;
   }

}
