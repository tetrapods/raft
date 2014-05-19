package io.tetrapod.raft;

import java.io.*;
import java.util.*;

import org.slf4j.*;

/**
 * A raft log is the backbone of the raft algorithm. It stores an ordered list of commands that have been agreed upon by consensus, as well
 * as the tentative list of future commands yet to be confirmed.
 * 
 * 
 * <ul>
 * <li>TODO: Add a proper file lock so we can ensure only one raft process to a raft-dir</li>
 * <li>TODO: Make constants configurable
 * </ul>
 * 
 */
public class Log<T extends StateMachine<T>> {

   public static final Logger   logger           = LoggerFactory.getLogger(Log.class);

   public static final int      LOG_FILE_VERSION = 1;

   /**
    * The log's in-memory buffer of log entries
    */
   private final List<Entry<T>> entries          = new ArrayList<>();

   private final Config         config;

   /**
    * Our current journal file's output stream
    */
   private DataOutputStream     out;
   private boolean              running          = true;

   // We keep some key index & term variables that may or 
   // may not be in our buffer and are accessed frequently:

   private long                 firstIndex       = 0;
   private long                 firstTerm        = 0;
   private long                 lastIndex        = 0;
   private long                 lastTerm         = 0;
   private long                 commitIndex      = 0;

   /**
    * The state machine we are coordinating via raft
    */
   private final T              stateMachine;

   public Log(Config config, T stateMachine) throws IOException {
      this.stateMachine = stateMachine;
      this.config = config;
      this.config.getLogDir().mkdirs();

      loadSnapshot();
      replayLogs();
      updateStateMachine();

      final Thread t = new Thread(new Runnable() {
         public void run() {
            writeLoop();
         }
      }, "Raft Log Writer");
      t.start();
   }

   public synchronized T getStateMachine() {
      return stateMachine;
   }

   /**
    * Attempts to append the log entry to our log.
    * 
    * @return true if the entry was successfully appended to the log, or was already in our log to begin with
    */
   public synchronized boolean append(Entry<T> entry) {
      assert entry != null;
      // check if the entry is already in our log
      if (entry.index <= lastIndex) {
         assert entry.index >= commitIndex : entry.index + " >= " + commitIndex;
         if (getTerm(entry.index) != entry.term) {
            logger.warn("Log is conflicted at {} : {} ", entry, getTerm(entry.index));
            wipeConflictedEntries(entry.index);
         } else {
            return true; // we already have this entry
         }
      }

      // validate that this is an acceptable entry to append next
      if (entry.index == lastIndex + 1 && entry.term >= lastTerm) {

         // append to log
         entries.add(entry);

         // update our indexes
         if (firstIndex == 0) {
            assert (entries.size() == 1);
            firstIndex = entry.index;
            firstTerm = entry.term;
         }
         lastIndex = entry.index;
         lastTerm = entry.term;

         return true;
      }

      return false;
   }

   /**
    * Append a new command to the log. Should only be called by a Leader
    */
   public synchronized boolean append(long term, Command<T> command) {
      return append(new Entry<T>(term, lastIndex + 1, command));
   }

   /**
    * Get an entry from our log, by index
    */
   public synchronized Entry<T> getEntry(long index) {
      if (index > 0 && index <= lastIndex) {
         if (index >= firstIndex && entries.size() > 0) {
            assert index - firstIndex < Integer.MAX_VALUE;
            assert (index - firstIndex) < entries.size() : "index=" + index + ", first=" + firstIndex;
            return entries.get((int) (index - firstIndex));
         } else {
            return getEntryFromDisk(index);
         }
      }
      return null; // we don't have it!
   }

   public Entry<T>[] getEntries(long fromIndex, int maxEntries) {
      if (fromIndex > lastIndex) {
         return null;
      }
      @SuppressWarnings("unchecked")
      final Entry<T>[] list = (Entry<T>[]) new Entry<?>[(int) Math.min(maxEntries, (lastIndex - fromIndex) + 1)];
      for (int i = 0; i < list.length; i++) {
         list[i] = getEntry(fromIndex + i);
         if (list[i] == null) {
            logger.warn("Could not find log entry {}", fromIndex + i);
            return null;
         }
      }
      return list;
   }

   public long getTerm(long index) {
      if (index == 0) {
         return 0;
      }
      if (index == stateMachine.getPrevIndex()) {
         return stateMachine.getPrevTerm();
      }
      if (index == stateMachine.getIndex()) {
         return stateMachine.getTerm();
      }
      return getEntry(index).term;
   }

   private synchronized void wipeConflictedEntries(long index) {
      // we have a conflict -- we need to throw away all entries from our log from this point on
      while (lastIndex >= index) {
         entries.remove((int) (lastIndex-- - firstIndex));
      }
      if (lastIndex > 0) {
         lastTerm = getTerm(lastIndex);
      } else {
         lastTerm = 0;
      }
   }

   public List<Entry<T>> getEntries() {
      return entries;
   }

   public File getLogDirectory() {
      return config.getLogDir();
   }

   public synchronized long getFirstIndex() {
      return firstIndex;
   }

   public synchronized long getFirstTerm() {
      return firstTerm;
   }

   public synchronized long getLastIndex() {
      return lastIndex;
   }

   public synchronized long getLastTerm() {
      return lastTerm;
   }

   public synchronized long getCommitIndex() {
      return commitIndex;
   }

   public synchronized void setCommitIndex(long index) {
      commitIndex = index;
   }

   /**
    * See if our log is consistent with the purported leader
    * 
    * @return false if log doesn't contain an entry at index whose term matches
    */
   public boolean isConsistentWith(final long index, final long term) {
      if (index == 0 && term == 0 || index > lastIndex) {
         return true;
      }
      final Entry<T> entry = getEntry(index);

      if (entry == null) {
         if (index == stateMachine.getPrevIndex()) {
            return term == stateMachine.getPrevTerm();
         }
      }

      return (entry != null && entry.term == term);
   }

   public synchronized boolean isRunning() {
      return running;
   }

   public void stop() {
      synchronized (this) {
         running = false;
      }
      try {
         updateStateMachine();
         synchronized (this) {
            if (out != null) {
               out.close();
               out = null;
            }
         }
      } catch (Throwable t) {
         logger.error(t.getMessage(), t);
      }
   }

   private void writeLoop() {
      while (isRunning()) {
         try {
            updateStateMachine();
            compact();
            synchronized (this) {
               wait(100);
            }
         } catch (Exception t) {
            logger.error(t.getMessage(), t);
         }
      }
   }

   /**
    * Get the canonical file name for this index
    */
   private File getFile(long index) {
      long firstIndexInFile = (index / config.getEntriesPerFile()) * config.getEntriesPerFile();
      return new File(getLogDirectory(), String.format("%016X.log", firstIndexInFile));
   }

   private synchronized void ensureCorrectLogFile(long index) throws IOException {
      if (index % config.getEntriesPerFile() == 0) {
         if (out != null) {
            out.close();
            out = null;
         }
      }
      if (out == null) {
         File file = getFile(index);
         if (file.exists()) {
            file.renameTo(new File(getLogDirectory(), "old." + file.getName()));
         }
         logger.info("Raft Log File : {}", file.getAbsolutePath());
         out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
         out.writeInt(LOG_FILE_VERSION);
      }
   }

   /**
    * Applies log entries to our state machine until it is at the given index
    */
   private synchronized void updateStateMachine() {
      try {
         synchronized (stateMachine) {
            while (commitIndex > stateMachine.getIndex()) {
               final Entry<T> e = getEntry(stateMachine.getIndex() + 1);
               assert (e != null);
               stateMachine.apply(e);
               ensureCorrectLogFile(e.index);
               e.write(out);
               if ((e.index % config.getEntriesPerSnapshot()) == 0) {
                  saveSnapshot();
               }
            }
         }
      } catch (IOException e) {
         logger.error(e.getMessage(), e);
         running = false; // revisit this, but should probably halt
      }
   }

   protected synchronized void loadSnapshot() throws IOException {
      File file = new File(getLogDirectory(), "raft.snapshot");
      if (file.exists()) {
         logger.info("Loading snapshot {} ", file);
         stateMachine.readSnapshot(file);
         logger.info("Loaded snapshot @ {}:{}", stateMachine.getTerm(), stateMachine.getIndex());
         lastIndex = stateMachine.getIndex();
         lastTerm = stateMachine.getTerm();
      }
   }

   /**
    * Read and apply all available entries in the log from disk
    * 
    * @throws FileNotFoundException
    */
   private synchronized void replayLogs() throws IOException {
      Entry<T> entry = null;
      do {
         entry = getEntryFromDisk(stateMachine.getIndex() + 1);
         if (entry != null) {
            stateMachine.apply(entry);
         }
      } while (entry != null);

      // get the most recent file of entries
      List<Entry<T>> list = loadLogFile(getFile(stateMachine.getIndex()));
      if (list != null && list.size() > 0) {
         assert (entries.size() == 0);
         entries.addAll(list);
         firstIndex = entries.get(0).index;
         firstTerm = entries.get(0).term;
         lastIndex = entries.get(entries.size() - 1).index;
         lastTerm = entries.get(entries.size() - 1).term;
         // TODO: rename existing file in case of failure
         // re-write out the last file
         out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(getFile(firstIndex))));
         out.writeInt(LOG_FILE_VERSION);
         for (Entry<T> e : list) {
            e.write(out);
         }
      }
   }

   /**
    * An LRU cache of entries loaded from disk
    */
   private final Map<String, List<Entry<T>>> entryFileCache = new LinkedHashMap<String, List<Entry<T>>>(3, 0.75f, true) {
                                                               @Override
                                                               protected boolean removeEldestEntry(Map.Entry<String, List<Entry<T>>> eldest) {
                                                                  return size() > 2;
                                                               }
                                                            };

   private Entry<T> getEntryFromDisk(long index) {
      File file = getFile(index);
      if (file.exists()) {
         List<Entry<T>> list = loadLogFile(file);
         if (list != null && list.size() > 0) {
            int i = (int) (index - list.get(0).index);
            if (i >= 0 && i < list.size()) {
               assert list.get(i).index == index;
               return list.get(i);
            }
         }
      }
      return null;
   }

   public List<Entry<T>> loadLogFile(File file) {
      synchronized (entryFileCache) {
         List<Entry<T>> list = entryFileCache.get(file.getName());
         if (list == null) {
            list = new ArrayList<>();
            if (file.exists()) {
               try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
                  final int version = in.readInt();
                  assert (version <= LOG_FILE_VERSION);
                  while (true) {
                     list.add(new Entry<T>(in, stateMachine));
                  }
               } catch (EOFException t) {
                  logger.debug("Read {} from {}", list.size(), file);
               } catch (Throwable t) {
                  logger.error(t.getMessage(), t);
               }
            }
            entryFileCache.put(file.getName(), list);
         }
         return list;
      }
   }

   /**
    * Discards entries from our buffer that we no longer need to store in memory
    */
   private synchronized void compact() {
      if (entries.size() > config.getEntriesPerFile() * 2) {
         logger.info("Compacting log size = {}", entries.size());
         List<Entry<T>> entriesToKeep = new ArrayList<>();
         for (Entry<T> e : entries) {
            if (e.index > commitIndex || e.index > stateMachine.getIndex() || e.index > lastIndex - config.getEntriesPerFile()) {
               entriesToKeep.add(e);
            }
         }
         entries.clear();
         entries.addAll(entriesToKeep);
         Entry<T> first = entries.get(0);
         firstIndex = first.index;
         firstTerm = first.term;
         logger.info("Compacted log new size = {}", entries.size());
      }

      if (config.getDeleteOldFiles()) {
         long index = commitIndex - (config.getEntriesPerSnapshot() * 2);
         while (index > 0) {
            File file = getFile(index);
            if (file.exists()) {
               logger.info("Deleting old log file {}", file);
               file.delete();
            } else {
               break;
            }
            index -= config.getEntriesPerFile();
         }
      }

   }

   /**
    * Currently is a pause-the-world snapshot
    */
   private long saveSnapshot() throws IOException {
      // currently pauses the world to save a snapshot
      File openFile = new File(getLogDirectory(), "raft.open.snapshot");
      synchronized (stateMachine) {
         stateMachine.writeSnapshot(openFile, getTerm(stateMachine.getPrevIndex()));
         File file = new File(getLogDirectory(), "raft.snapshot");
         if (file.exists()) {
            file.renameTo(new File(getLogDirectory(), "raft.old.snapshot"));
         }
         openFile.renameTo(file);
         return stateMachine.getIndex();
      }
   }

}
