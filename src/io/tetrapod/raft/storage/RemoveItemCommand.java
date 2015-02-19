package io.tetrapod.raft.storage;

import java.io.*;

import io.tetrapod.raft.Command;

public class RemoveItemCommand implements Command<StorageStateMachine> {
   public static final int COMMAND_ID = 2;

   private String          key;

   public RemoveItemCommand() {}

   public RemoveItemCommand(String key) {
      this.key = key;
   }

   @Override
   public void applyTo(StorageStateMachine state) {
      state.removeItem(key);
   }

   @Override
   public void write(DataOutputStream out) throws IOException {
      out.writeUTF(key);
   }

   @Override
   public void read(DataInputStream in) throws IOException {
      key = in.readUTF();
   }

   @Override
   public int getCommandType() {
      return COMMAND_ID;
   }

}
