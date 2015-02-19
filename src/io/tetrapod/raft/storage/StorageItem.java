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
      this.data = Arrays.copyOf(copy.data, copy.data.length);
   }

   public StorageItem(DataInputStream in) throws IOException {
      this.key = in.readUTF();
      this.data = new byte[in.readInt()];
      in.readFully(this.data);
      this.version = in.readInt();
   }

   public void write(DataOutputStream out) throws IOException {
      out.writeUTF(key);
      out.writeInt(data.length);
      out.write(data);
      out.writeInt(version);
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

   public void modify(byte[] data) {
      this.data = data;
      this.version++;
   }

}
