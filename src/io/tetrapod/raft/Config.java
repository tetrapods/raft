package io.tetrapod.raft;

import java.io.File;

/**
 * Raft Configuration. All the magic numbers for turning performance to your specific needs.
 */
public class Config {
   public static final int     DEFAULT_ELECTION_TIMEOUT_FIXED_MILLIS  = 1000;
   public static final int     DEFAULT_ELECTION_TIMEOUT_RANDOM_MILLIS = 2000;
   public static final int     DEFAULT_HEARTBEAT_MILLIS               = 250;
   public static final int     DEFAULT_MAX_ENTRIES_PER_REQUEST        = 250;
   public static final int     DEFAULT_PART_SIZE                      = 1024 * 256;

   public static final int     NUM_ENTRIES_PER_LOGFILE                = 0x2000;
   public static final int     NUM_ENTRIES_PER_SNAPSHOT               = 0x10000;
   public static final boolean DELETE_OLD_FILES                       = true;

   private int                 electionTimeoutFixedMillis             = DEFAULT_ELECTION_TIMEOUT_FIXED_MILLIS;
   private int                 electionTimeoutRandomMillis            = DEFAULT_ELECTION_TIMEOUT_RANDOM_MILLIS;
   private int                 heartbeatMillis                        = DEFAULT_HEARTBEAT_MILLIS;
   private int                 maxEntriesPerRequest                   = DEFAULT_MAX_ENTRIES_PER_REQUEST;
   private int                 snapshotPartSize                       = DEFAULT_PART_SIZE;
   private int                 entriesPerFile                         = NUM_ENTRIES_PER_LOGFILE;
   private int                 entriesPerSnapshot                     = NUM_ENTRIES_PER_SNAPSHOT;
   private boolean             deleteOldFiles                         = DELETE_OLD_FILES;

   /**
    * The directory where we will read and write raft data files
    */
   private File                logDir                                 = new File("raft");

   /**
    * The name of our cluster which provides a fail-safe to prevent nodes from accidentally joining the wrong raft cluster.
    */
   private String              clusterName;

   /**
    * Get the fixed timeout length in millis before we will call an election.
    */
   public int getElectionTimeoutFixedMillis() {
      return electionTimeoutFixedMillis;
   }

   /**
    * Set the fixed timeout length in millis before we will call an election.
    * 
    * This should be significantly greater than the RPC latency and heartbeat timeout.
    */
   public Config setElectionTimeoutFixedMillis(int electionTimeoutFixedMillis) {
      this.electionTimeoutFixedMillis = electionTimeoutFixedMillis;
      return this;
   }

   /**
    * Get the randomized timeout length in millis before we will call an election.
    */
   public int getElectionTimeoutRandomMillis() {
      return electionTimeoutRandomMillis;
   }

   /**
    * Set the randomized timeout length in millis before we will call an election. Larger values reduce the chances of failed elections, but
    * increase the time it may take to call an election after the leader fails.
    */
   public Config setElectionTimeoutRandomMillis(int electionTimeoutRandomMillis) {
      this.electionTimeoutRandomMillis = electionTimeoutRandomMillis;
      return this;
   }

   /**
    * Get the heart beat interval. The leader will send a hear tbeat this often, even if there are no new entries.
    */
   public int getHeartbeatMillis() {
      return heartbeatMillis;
   }

   /**
    * Set the heart beat interval.
    */
   public Config setHeartbeatMillis(int heartbeatMillis) {
      this.heartbeatMillis = heartbeatMillis;
      return this;
   }

   /**
    * Get the maximum number of log entries per AppendEntries RPC
    */
   public int getMaxEntriesPerRequest() {
      return maxEntriesPerRequest;
   }

   /**
    * Set the maximum number of log entries per AppendEntries RPC. This should be tuned for your particular RPC implementation and typical
    * Command data size and rates.
    */
   public Config setMaxEntriesPerRequest(int maxEntriesPerRequest) {
      this.maxEntriesPerRequest = maxEntriesPerRequest;
      return this;
   }

   /**
    * Get the directory where we store snapshots and log files
    */
   public File getLogDir() {
      return logDir;
   }

   /**
    * Set the directory where we store snapshots and log files
    */
   public Config setLogDir(File logDir) {
      this.logDir = logDir;
      return this;
   }

   /**
    * Get our configured cluster name.
    */
   public String getClusterName() {
      return clusterName;
   }

   /**
    * Get our configured cluster name. Our peers must all have matching cluster names to vote in elections. This prevents misconfigured
    * nodes from accidentally joining or voting in the wrong cluster.
    */
   public Config setClusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
   }

   /**
    * Get the maximum bytes sent per InstallSnapshot RPC.
    */
   public int getSnapshotPartSize() {
      return snapshotPartSize;
   }

   /**
    * Set the maximum bytes sent per InstallSnapshot RPC. This should be tuned appropriately for your RPC implementation and network
    * conditions.
    */
   public Config setSnapshotPartSize(int snapshotPartSize) {
      this.snapshotPartSize = snapshotPartSize;
      return this;
   }

   /**
    * Get the number of log entries we will store per log file
    */
   public int getEntriesPerFile() {
      return entriesPerFile;
   }

   /**
    * Set the number of log entries we will store per log file
    */
   public Config setEntriesPerFile(int entriesPerFile) {
      this.entriesPerFile = entriesPerFile;
      return this;
   }

   /**
    * Get if we delete old log files to recover disk space, or keep a permanent log of all entries.
    */
   public boolean getDeleteOldFiles() {
      return deleteOldFiles;
   }

   /**
    * Set if we will delete older log files to recover disk space, or keep a permanent log of all entries.
    */
   public Config setDeleteOldFiles(boolean deleteOldFiles) {
      this.deleteOldFiles = deleteOldFiles;
      return this;
   }

   /**
    * Get the number of log entries we will process between taking snapshots.
    */
   public int getEntriesPerSnapshot() {
      return entriesPerSnapshot;
   }

   /**
    * Set the number of log entries we will process between taking snapshots.
    */
   public Config setEntriesPerSnapshot(int entriesPerSnapshot) {
      this.entriesPerSnapshot = entriesPerSnapshot;
      return this;
   }
}
