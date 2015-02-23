package io.tetrapod.raft.storage;

import io.tetrapod.raft.Command;

import java.io.*;

public class RemoveItemCommand<T extends StorageStateMachine<T>> implements Command<T> {
   public static final int COMMAND_ID = 2;

   private String          key;

   public RemoveItemCommand() {}

   public RemoveItemCommand(String key) {
      this.key = key;
   }

   @Override
   public void applyTo(T state) {
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
