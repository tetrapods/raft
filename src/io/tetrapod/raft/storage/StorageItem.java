package io.tetrapod.raft.storage;

import java.io.*;

/**
 * A wrapper for a document in the Storage database
 */
public class StorageItem {
   public final String key;
   private int         version;
   private byte[]      data;

   public StorageItem(String key) {
      this.key = key;
   }

   public StorageItem(DataInputStream in) throws IOException {
      this.key = in.readUTF();
      this.data = new byte[in.readInt()];
      in.readFully(this.data);
      this.version = in.readInt();
   }

   public void setData(byte[] data) {
      this.data = data;
   }

   public byte[] getData() {
      return this.data;
   }

   public int getVersion() {
      return version;
   }

}
