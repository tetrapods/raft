package io.tetrapod.raft;

import java.io.File;
import java.util.*;

import org.slf4j.*;

/**
 * A raft log is the backbone of the raft algorithm. It stores an ordered list of commands that have been agreed upon by consensus, as well
 * as the tentative list of future commands yet to be confirmed.
 */
public class Log<T extends StateMachine> {

   public static final Logger   logger             = LoggerFactory.getLogger(Log.class);

   /**
    * The log's in-memory buffer of log entries
    */
   private final List<Entry<T>> entries            = new ArrayList<>();

   /**
    * The directory where we will read and write raft data files
    */
   private final File           logDirectory;

   /**
    * Handlers serialization of the log to disk
    */
   private final LogWriter      logWriter;

   /**
    * The state machine we are coordinating via raft
    */
   private final T              state;

   // We keep some key index & term variables that may or 
   // may not be in our buffer and are accessed frequently:

   private long                 firstEntryIndex    = 0;
   private long                 firstEntryTerm     = 0;
   private long                 lastEntryIndex     = 0;
   private long                 lastEntryTerm      = 0;
   private long                 commitEntryIndex   = 0;
   private long                 commitEntryTerm    = 0;
   private long                 snapshotEntryIndex = 0;
   private long                 snapshotEntryTerm  = 0;

   public Log(File logDir, T stateMachine) {
      this.logDirectory = logDir;
      this.logDirectory.mkdirs();
      this.logWriter = new LogWriter(logDirectory);
      this.state = stateMachine;
   }

   /**
    * Attempts to append the log entry to our log.
    * 
    * @return true if the entry was successfully appended to the log
    */
   public synchronized boolean append(Entry<T> entry) {
      // check if the entry is already in our log
      if (entry.index <= lastEntryIndex) {
         final Entry<T> oldEntry = getEntry(entry.index);
         if (oldEntry.term != entry.term) {
            logger.warn("Log is conflicted at {}", oldEntry);
            wipeConflictedEntries(oldEntry.index);
         }
      }

      // validate that this is an acceptable entry to append next
      if (entry.index == lastEntryIndex + 1 && entry.term >= lastEntryTerm) {

         // apply the event
         entry.command.applyTo(state);
         state.apply(entry.index, entry.term);

         // append to log
         entries.add(entry);

         // update our indexes
         if (firstEntryIndex == 0) {
            firstEntryIndex = entry.index;
            firstEntryTerm = entry.term;
         }
         lastEntryIndex = entry.index;
         lastEntryTerm = entry.term;

         return true;
      }

      return false;
   }

   /**
    * Append a new command to the log. Should only be called by a Leader
    */
   public boolean append(long term, Command<T> command) {
      return append(new Entry<T>(term, lastEntryIndex + 1, command));
   }

   /**
    * Get an entry from our log, by index
    */
   public Entry<T> getEntry(long index) {
      if (index <= lastEntryIndex) {
         if (index >= firstEntryIndex) {
            assert index - firstEntryIndex < Integer.MAX_VALUE;
            return entries.get((int) (index - firstEntryIndex));
         } else if (index >= snapshotEntryIndex) {
            // we need to fetch from disk
         }
      }
      return null; // we don't have it!
   }

   private void wipeConflictedEntries(long index) {
      // we have a conflict -- we need to throw away all entries from our log from this point on
      while (lastEntryIndex >= index) {
         entries.remove((int) (lastEntryIndex-- - firstEntryIndex));
      }
      state.restoreToIndex(index - 1);
      lastEntryTerm = state.getTerm();
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

   public T getState() {
      return state;
   }

   public long getFirstEntryIndex() {
      return firstEntryIndex;
   }

   public long getFirstEntryTerm() {
      return firstEntryTerm;
   }

   public long getLastEntryIndex() {
      return lastEntryIndex;
   }

   public long getLastEntryTerm() {
      return lastEntryTerm;
   }

   public long getCommitEntryIndex() {
      return commitEntryIndex;
   }

   public long getCommitEntryTerm() {
      return commitEntryTerm;
   }

   public long getSnapshotEntryIndex() {
      return snapshotEntryIndex;
   }

   public long getSnapshotEntryTerm() {
      return snapshotEntryTerm;
   }

}
