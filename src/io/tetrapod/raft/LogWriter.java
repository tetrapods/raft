package io.tetrapod.raft;

import java.io.File;

/**
 * Handles journaling of log entries
 */
public class LogWriter {

   private final File logDirectory;

   public LogWriter(File logDirectory) {
      this.logDirectory = logDirectory;
   }

   public File getLogDirectory() {
      return logDirectory;
   }

}
