package io.tetrapod.raft.storage;

import io.tetrapod.raft.Command;

import java.io.*;
import java.nio.charset.Charset;

public class PutItemCommand<T extends StorageStateMachine<T>> implements Command<T> {
   public static final int COMMAND_ID = 1;

   public static Charset   UTF8       = Charset.forName("UTF-8");

   private String          key;
   private byte[]          data;

   public PutItemCommand() {}

   public PutItemCommand(String key, byte[] data) {
      this.key = key;
      this.data = data;
   }

   public PutItemCommand(String key, String data) {
      this.key = key;
      this.data = data.getBytes(UTF8);
   }

   @Override
   public void applyTo(T state) {
      state.putItem(key, data);
   }

   @Override
   public void write(DataOutputStream out) throws IOException {
      out.writeUTF(key);
      out.writeInt(data.length);
      out.write(data);
   }

   @Override
   public void read(DataInputStream in, int fileVersion) throws IOException {
      key = in.readUTF();
      data = new byte[in.readInt()];
      in.readFully(data);
   }

   @Override
   public int getCommandType() {
      return COMMAND_ID;
   }

}
