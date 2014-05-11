package io.tetrapod.raft;

import java.io.File;
import java.util.*;

import org.slf4j.*;

/**
 * A raft log is the backbone of the raft algorithm. It stores an ordered list of commands that have been agreed upon by consensus, as well
 * as the tentative list of future commands yet to be confirmed.
 */
public class Log<T extends StateMachine<T>> {

   public static final Logger   logger        = LoggerFactory.getLogger(Log.class);

   /**
    * The log's in-memory buffer of log entries
    */
   private final List<Entry<T>> entries       = new ArrayList<>();

   /**
    * The directory where we will read and write raft data files
    */
   private final File           logDirectory;

   /**
    * Handlers serialization of the log to disk
    */
   private final LogWriter      logWriter;

   // We keep some key index & term variables that may or 
   // may not be in our buffer and are accessed frequently:

   private long                 firstIndex    = 0;
   private long                 firstTerm     = 0;
   private long                 lastIndex     = 0;
   private long                 lastTerm      = 0;
   private long                 commitIndex   = 0;
   private long                 snapshotIndex = 0;
   private long                 snapshotTerm  = 0;

   public Log(File logDir) {
      this.logDirectory = logDir;
      this.logDirectory.mkdirs();
      this.logWriter = new LogWriter(logDirectory);
   }

   /**
    * Attempts to append the log entry to our log.
    * 
    * @return true if the entry was successfully appended to the log, or was already in our log to begin with
    */
   public synchronized boolean append(Entry<T> entry) {
      // check if the entry is already in our log
      if (entry.index <= lastIndex) {
         final Entry<T> oldEntry = getEntry(entry.index);
         if (oldEntry.term != entry.term) {
            logger.warn("Log is conflicted at {}", oldEntry);
            wipeConflictedEntries(oldEntry.index);
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
      if (index <= lastIndex) {
         if (index >= firstIndex) {
            assert index - firstIndex < Integer.MAX_VALUE;
            return entries.get((int) (index - firstIndex));
         } else if (index > snapshotIndex) {
            // TODO: we need to fetch it from disk
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
         assert list[i] != null;
      }
      return list;
   }

   public long getTerm(long index) {
      if (index > snapshotIndex) {
         return getEntry(index).term;
      } else if (index > snapshotIndex) {
         return snapshotTerm;
      } else if (index == 0) {
         return 0;
      }
      return -1; // maybe throw exception?
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
      return logDirectory;
   }

   public LogWriter getLogWriter() {
      return logWriter;
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

   public synchronized long getSnapshotIndex() {
      return snapshotIndex;
   }

   public synchronized long getSnapshotTerm() {
      return snapshotTerm;
   }

   /**
    * See if our log is consistent with the purported leader
    * 
    * @return false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
    */
   public boolean isConsistentWith(long index, long term) {
      if (index == 0 && term == 0) {
         return true;
      }
      final Entry<T> entry = getEntry(index);
      return (entry != null && entry.term == term);
   }

}
