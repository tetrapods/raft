package io.tetrapod.raft.storage;

import io.tetrapod.raft.*;

import java.io.*;
import java.util.*;

public class StorageStateMachine extends StateMachine<StorageStateMachine> {

   private Map<String, StorageItem>            items       = new HashMap<>();

   private Map<Long, Map<String, StorageItem>> copyOnWrite = new HashMap<>();

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
         case RemoveItemCommand.COMMAND_ID:
            return new RemoveItemCommand();
      }
      return null;
   }

   @Override
   public void saveState(DataOutputStream out) throws IOException {
      final Map<String, StorageItem> modified = new HashMap<>();

      long writeIndex;
      List<StorageItem> list;

      synchronized (copyOnWrite) {
         writeIndex = getIndex();
         copyOnWrite.put(writeIndex, modified);
         list = new ArrayList<StorageItem>(items.values());
      }

      // now we write the items out, checking the modified list to make sure we write the
      // items as they were at the time of this snapshot index
      out.writeInt(list.size());
      for (StorageItem item : list) {
         synchronized (copyOnWrite) {
            StorageItem modItem = modified.get(item.key);
            if (modItem != null) {
               modItem.write(out);
            } else {
               item.write(out);
            }
         }
      }

      synchronized (copyOnWrite) {
         copyOnWrite.remove(writeIndex);
      }
      modified.clear();
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
      // HACK FOR TESTING
      StorageItem item = items.get("foo");
      return item == null ? "<null>" : new String(item.getData());
   }

   protected void putItem(String key, byte[] data) {
      StorageItem item = items.get(key);
      if (item != null) {
         copyOnWrite(item);
         item.modify(data);
      } else {
         items.put(key, new StorageItem(key, data));
      }
   }

   public void removeItem(String key) {
      StorageItem item = items.get(key);
      if (item != null) {
         copyOnWrite(item);
         items.remove(key);
      }
   }

   private void copyOnWrite(StorageItem item) {
      synchronized (copyOnWrite) {
         for (Map<String, StorageItem> modified : copyOnWrite.values()) {
            // copy it only if it isn't copied yet
            if (!modified.containsKey(item.key)) {
               modified.put(item.key, new StorageItem(item));
            }
         }
      }
   }

}
