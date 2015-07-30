package io.tetrapod.raft.storage;

import io.tetrapod.raft.Command;

import java.io.*;

public class LockCommand<T extends StorageStateMachine<T>> implements Command<T> {
   public static final int COMMAND_ID = 4;

   private String          key;
   private String          uuid;
   private long            leaseForMillis;
   private long            curTime;
   private boolean         acquired;

   public LockCommand() {}

   public LockCommand(String key, String uuid, long expiry, long curTime) {
      this.key = key;
      this.leaseForMillis = expiry;
      this.uuid = uuid;
   }

   @Override
   public void applyTo(T state) {
      acquired = state.lock(key, uuid, leaseForMillis, curTime);
   }

   @Override
   public void write(DataOutputStream out) throws IOException {
      out.writeUTF(key);
      out.writeBoolean(uuid != null);
      if (uuid != null) {
         out.writeUTF(uuid);
      }
      out.writeLong(leaseForMillis);
      out.writeBoolean(acquired);
      out.writeLong(curTime);
   }

   @Override
   public void read(DataInputStream in, int fileVersion) throws IOException {
      key = in.readUTF();
      if (in.readBoolean()) {
         uuid = in.readUTF();
      }
      leaseForMillis = in.readLong();
      acquired = in.readBoolean();
      if (fileVersion > 1) {
         curTime = in.readLong();
      }
   }

   @Override
   public int getCommandType() {
      return COMMAND_ID;
   }

   public boolean wasAcquired() {
      return acquired;
   }

   @Override
   public String toString() {
      return "LockCommand(" + key + ", " + leaseForMillis + ", " + acquired + ")";
   }

}
