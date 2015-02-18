package io.tetrapod.raft.storage;

import java.io.*;

import io.tetrapod.raft.Command;

public class PutItemCommand implements Command<StorageStateMachine> {
   public static final int COMMAND_ID = 1;

   private String          key;
   private byte[]          data;

   public PutItemCommand() {}

   public PutItemCommand(String key, byte[] data) {
      this.key = key;
      this.data = data;
   }

   @Override
   public void applyTo(StorageStateMachine state) {
      StorageItem item = state.getItem(key);
      if (item == null) {
         item = new StorageItem(key);
      }
      item.setData(data);
   }

   @Override
   public void write(DataOutputStream out) throws IOException {
      out.writeUTF(key);
      out.writeInt(data.length);
      out.write(data);
   }

   @Override
   public void read(DataInputStream in) throws IOException {
      key = in.readUTF();
      data = new byte[in.readInt()];
      in.readFully(data);
   }

   @Override
   public int getCommandType() {
      return COMMAND_ID;
   }

}
