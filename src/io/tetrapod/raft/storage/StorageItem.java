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
   private long        lockExpiry;

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
   }

   public StorageItem(DataInputStream in) throws IOException {
      this.key = in.readUTF();
      this.data = new byte[in.readInt()];
      in.readFully(this.data);
      this.version = in.readInt();
      this.lockExpiry = in.readLong();
   }

   public void write(DataOutputStream out) throws IOException {
      out.writeUTF(key);
      out.writeInt(data == null ? 0 : data.length);
      if (data != null) {
         out.write(data);
      }
      out.writeInt(version);
      out.writeLong(lockExpiry);
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

   public boolean lock(long leaseForMillis, long curTime) {
      if (lockExpiry > curTime)
         return false;
      this.version++;
      this.lockExpiry = curTime + leaseForMillis;
      return true;
   }

   public void unlock() {
      this.version++;
      this.lockExpiry = 0;
   }

}
