package io.tetrapod.raft.storage;

import io.tetrapod.raft.Command;

import java.io.*;

public class UnlockCommand<T extends StorageStateMachine<T>> implements Command<T> {
   public static final int COMMAND_ID = 5;

   private String          key;
   private String          uuid;

   public UnlockCommand() {}

   public UnlockCommand(String key) {
      this.key = key;
   }

   @Override
   public void applyTo(T state) {
      state.unlock(key, uuid);
   }

   @Override
   public void write(DataOutputStream out) throws IOException {
      out.writeUTF(key);
      out.writeBoolean(uuid != null);
      if (uuid != null) {
         out.writeUTF(uuid);
      }
   }

   @Override
   public void read(DataInputStream in, int fileVersion) throws IOException {
      key = in.readUTF();
      if (fileVersion > 1) {
         if (in.readBoolean()) {
            uuid = in.readUTF();
         }
      }
   }

   @Override
   public int getCommandType() {
      return COMMAND_ID;
   }

}
