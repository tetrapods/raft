package io.tetrapod.raft.storage;

import java.io.*;
import java.util.Arrays;

/**
 * A wrapper for a document in the Storage database
 */
public class StorageItem {
   public final String key;
   private int         version;
   private byte[]      data;
   private LockInfo    lock;

   public StorageItem(String key) {
      this.key = key;
   }

   public StorageItem(String key, byte[] data) {
      this.key = key;
      this.data = data;
   }

   public StorageItem(StorageItem copy) {
      this.key = copy.key;
      this.version = copy.version;
      if (copy.data != null) {
         this.data = Arrays.copyOf(copy.data, copy.data.length);
      }
      if (copy.lock != null) {
         this.lock = copy.lock.copy();
      }
   }

   public StorageItem(DataInputStream in, int fileVersion) throws IOException {
      this.key = in.readUTF();
      this.data = new byte[in.readInt()];
      in.readFully(this.data);
      this.version = in.readInt();

      if (fileVersion >= 1) {
         if (in.readBoolean()) {
            this.lock = new LockInfo(in.readLong(), in.readUTF());
         }
      } else {
         this.lock = new LockInfo(in.readLong(), null);
      }
   }

   public void write(DataOutputStream out) throws IOException {
      out.writeUTF(key);
      out.writeInt(data == null ? 0 : data.length);
      if (data != null) {
         out.write(data);
      }
      out.writeInt(version);

      out.writeBoolean(lock != null);
      if (lock != null) {
         out.writeLong(lock.lockExpiry);
         out.writeUTF(lock.lockOwner != null ? lock.lockOwner : "");
      }
   }

   public void setData(byte[] data) {
      this.data = data;
   }

   public byte[] getData() {
      return this.data;
   }

   public String getDataAsString() {
      if (data != null) {
         try {
            return new String(data, "UTF-8");
         } catch (UnsupportedEncodingException e) {}
      }
      return null;
   }

   public int getVersion() {
      return version;
   }

   public void modify(byte[] data) {
      this.data = data;
      this.version++;
   }

   public boolean lock(long leaseForMillis, long curTime, String lockOwner) {
      if (lock != null && lock.lockExpiry > curTime)
         return false;
      this.version++;
      this.lock = new LockInfo(curTime + leaseForMillis, lockOwner);
      return true;
   }

   public void unlock() {
      this.version++;
      this.lock = null;
   }

   public boolean isOwned(String lockOwner) {
      return lock != null && lockOwner.equals(lock.lockOwner);
   }

   public static class LockInfo {
      private long   lockExpiry;
      private String lockOwner;

      public LockInfo(long lockExpiry, String lockOwner) {
         this.lockExpiry = lockExpiry;
         this.lockOwner = lockOwner;
      }

      public LockInfo copy() {
         return new LockInfo(lockExpiry, lockOwner);
      }
   }

}
