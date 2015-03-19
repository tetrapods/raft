package io.tetrapod.raft.storage;

import io.tetrapod.raft.Command;

import java.io.*;

public class LockCommand<T extends StorageStateMachine<T>> implements Command<T> {
   public static final int COMMAND_ID = 4;

   private String          key;
   private long            leaseForMillis;
   private boolean         acquired;

   public LockCommand() {}

   public LockCommand(String key, long expiry) {
      this.key = key;
      this.leaseForMillis = expiry;
   }

   @Override
   public void applyTo(T state) {
      acquired = state.lock(key, leaseForMillis);
   }

   @Override
   public void write(DataOutputStream out) throws IOException {
      out.writeUTF(key);
      out.writeLong(leaseForMillis);
      out.writeBoolean(acquired);
   }

   @Override
   public void read(DataInputStream in) throws IOException {
      key = in.readUTF();
      leaseForMillis = in.readLong();
      acquired = in.readBoolean();
   }

   @Override
   public int getCommandType() {
      return COMMAND_ID;
   }

   public boolean wasAcquired() { 
      return acquired;
   }

}
