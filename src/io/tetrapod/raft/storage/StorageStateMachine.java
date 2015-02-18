package io.tetrapod.raft.storage;

import io.tetrapod.raft.*;

import java.io.*;
import java.util.*;

public class StorageStateMachine extends StateMachine<StorageStateMachine> {

   private Map<String, StorageItem> items = new HashMap<>();

   public static class Factory implements StateMachine.Factory<StorageStateMachine> {
      public StorageStateMachine makeStateMachine() {
         return new StorageStateMachine();
      }
   }

   @Override
   public Command<StorageStateMachine> makeCommand(int id) {
      switch (id) {
         case PutItemCommand.COMMAND_ID:
            return new PutItemCommand();
      }
      return null;
   }

   @Override
   public void saveState(DataOutputStream out) throws IOException {
      out.writeInt(items.size());

      // TODO: Figure out copy-on-write non-blocking writer
   }

   @Override
   public void loadState(DataInputStream in) throws IOException {
      int numItems = in.readInt();
      while (numItems-- > 0) {
         StorageItem item = new StorageItem(in);
         items.put(item.key, item);
      }
   }

   public StorageItem getItem(String key) {
      return items.get(key);
   }

   @Override
   public String toString() {
      StorageItem item = items.get("foo");
      return item == null ? "<null>" : new String(item.getData());
   }

   public void putItem(String key, StorageItem item) {
      // TODO: copy-on-write support
      items.put(key, item);
   }

}
