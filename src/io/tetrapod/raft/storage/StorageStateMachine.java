package io.tetrapod.raft.storage;

import io.tetrapod.raft.*;

import java.io.*;
import java.util.*;

/**
 * TODO:
 * <ul>
 * <li>Unit Testing
 * <li>Conditional Puts
 * <li>Annotations to auto-build makeCommand()
 * </ul>
 */
public class StorageStateMachine extends StateMachine<StorageStateMachine> {

   private final Map<String, StorageItem>            items       = new HashMap<>();

   private final Map<Long, Map<String, StorageItem>> copyOnWrite = new HashMap<>();

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

   public StorageItem getItem(String key) {
      return items.get(key);
   }

   @Override
   public String toString() {
      // HACK FOR TESTING
      StorageItem item = items.get("foo");
      return item == null ? "<null>" : new String(item.getData());
   }

   @RaftCommand(id = 1)
   protected void putItem(String key, byte[] data) {
      StorageItem item = items.get(key);
      if (item != null) {
         copyOnWrite(item);
         item.modify(data);
      } else {
         items.put(key, new StorageItem(key, data));
      }
   }

   @RaftCommand(id = 2)
   public void removeItem(String key) {
      StorageItem item = items.get(key);
      if (item != null) {
         copyOnWrite(item);
         items.remove(key);
      }
   }

   @RaftCommand(id = 3)
   public long increment(String key, long amount) {
      StorageItem item = items.get(key);
      if (item != null) {
         copyOnWrite(item);
         long val = RaftUtil.fromBytes(item.getData());
         val += amount;
         // TODO: put val into command so it can be used by caller
         item.modify(RaftUtil.toBytes(val));
         return val;
      } else {
         items.put(key, new StorageItem(key, RaftUtil.toBytes(amount)));
         return amount;
      }
   }

}
