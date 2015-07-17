package io.tetrapod.raft;

import java.io.*;
import java.util.*;
import java.util.zip.*;

import org.slf4j.*;

/**
 * The state machine applies commands to update state.
 * 
 * It contains the state we want to coordinate across a distributed cluster.
 * 
 */
public abstract class StateMachine<T extends StateMachine<T>> {

   public static final Logger logger                  = LoggerFactory.getLogger(StateMachine.class);

   public static final int    SNAPSHOT_FILE_VERSION   = 1;

   public static final int    COMMAND_ID_ADD_PEER     = -1;
   public static final int    COMMAND_ID_DEL_PEER     = -2;
   public static final int    COMMAND_ID_NEW_TERM     = -3;
   public static final int    COMMAND_ID_HEALTH_CHECK = -4;

   public enum SnapshotMode {
      /**
       * Blocking mode is memory efficient, but blocks all changes while writing the snapshot. Only suitable for small state machines that
       * can write out very quickly
       */
      Blocking,

      /**
       * Dedicated mode maintains an entire secondary copy of the state machine in memory for snapshots. This allows easy non-blocking
       * snapshots, at the expense of using more memory to hold the second state machine, and the processing time to apply commands twice.
       */
      Dedicated,

      /**
       * If your state machine can support copy-on-writes, this is the most efficient mode for non-blocking snapshots
       */
      CopyOnWrite
   }

   public static interface CommandFactory<T extends StateMachine<T>> {
      public Command<T> makeCommand();
   }

   public final Map<Integer, CommandFactory<T>> commandFactories = new HashMap<>();

   private final List<Listener<T>>              listeners        = new ArrayList<>();

   // State
   private long                                 index;
   private long                                 term;
   private long                                 checksum         = 0;
   private long                                 count            = 0;
   private long                                 prevIndex;
   private long                                 prevTerm;
   private final Map<Integer, Peer>             peers            = new HashMap<>();

   public static class Peer {
      public final int    peerId;
      public final String host;
      public final int    port;

      Peer(DataInputStream in) throws IOException {
         peerId = in.readInt();
         host = in.readUTF();
         port = in.readInt();
      }

      public Peer(int peerId, String host, int port) {
         this.peerId = peerId;
         this.host = host;
         this.port = port;
      }

      private void write(DataOutputStream out) throws IOException {
         out.writeInt(peerId);
         out.writeUTF(host);
         out.writeInt(port);
      }

      @Override
      public String toString() {
         return String.format("Peer-%d{%s:%d}", peerId, host, port);
      }
   }

   public StateMachine() {
      registerCommand(COMMAND_ID_ADD_PEER, new CommandFactory<T>() {
         @Override
         public Command<T> makeCommand() {
            return new AddPeerCommand<T>();
         }
      });
      registerCommand(COMMAND_ID_DEL_PEER, new CommandFactory<T>() {
         @Override
         public Command<T> makeCommand() {
            return new DelPeerCommand<T>();
         }
      });
      registerCommand(COMMAND_ID_NEW_TERM, new CommandFactory<T>() {
         @Override
         public Command<T> makeCommand() {
            return new NewTermCommand<T>();
         }
      });
      registerCommand(COMMAND_ID_HEALTH_CHECK, new CommandFactory<T>() {
         @Override
         public Command<T> makeCommand() {
            return new HealthCheckCommand<T>();
         }
      });
   }

   public SnapshotMode getSnapshotMode() {
      return SnapshotMode.Blocking;
   }

   public void registerCommand(int id, CommandFactory<T> factory) {
      assert !commandFactories.containsKey(id);
      commandFactories.put(id, factory);
   }

   public Command<T> makeCommandById(int id) {
      CommandFactory<T> factory = commandFactories.get(id);
      if (factory == null) {
         throw new RuntimeException("Could not find command factory for command type " + id);
      }
      return factory.makeCommand();
   }

   public abstract void saveState(DataOutputStream out) throws IOException;

   public abstract void loadState(DataInputStream in) throws IOException;

   public void writeSnapshot(File file, long prevTerm) throws IOException {
      try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(file))))) {
         out.writeInt(SNAPSHOT_FILE_VERSION);
         out.writeLong(term);
         out.writeLong(index);
         out.writeLong(prevTerm);
         out.writeLong(count);
         out.writeLong(checksum);
         out.writeInt(peers.size());
         for (Peer peer : peers.values()) {
            peer.write(out);
         }
         saveState(out);
      }
   }

   public void readSnapshot(File file) throws IOException {
      try (DataInputStream in = new DataInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file))))) {
         final int fileVersion = in.readInt();
         if (fileVersion > SNAPSHOT_FILE_VERSION) {
            throw new IOException("Incompatible Snapshot Format: " + fileVersion + " > " + SNAPSHOT_FILE_VERSION);
         }
         term = in.readLong();
         index = in.readLong();
         prevIndex = index - 1;
         prevTerm = in.readLong();
         count = in.readLong();
         checksum = in.readLong();
         peers.clear();
         final int numPeers = in.readInt();
         for (int i = 0; i < numPeers; i++) {
            Peer p = new Peer(in);
            peers.put(p.peerId, p);
         }
         loadState(in);
      }
   }

   public static long getSnapshotIndex(File file) {
      try (DataInputStream in = new DataInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file))))) {
         int version = in.readInt();
         assert (version <= SNAPSHOT_FILE_VERSION);
         @SuppressWarnings("unused")
         long term = in.readLong();
         long index = in.readLong();
         return index;
      } catch (IOException e) {
         logger.error(e.getMessage(), e);
         return 0;
      }
   }

   public long getIndex() {
      return index;
   }

   public long getTerm() {
      return term;
   }

   public long getPrevIndex() {
      return prevIndex;
   }

   public long getPrevTerm() {
      return prevTerm;
   }

   @SuppressWarnings("unchecked")
   protected void apply(Entry<T> entry) {
      assert (this.index + 1 == entry.index) : (this.index + 1) + "!=" + entry.index;
      assert (this.term <= entry.term);
      entry.command.applyTo((T) this);
      this.index = entry.index;
      this.term = entry.term;
      fireEntryAppliedEvent(entry);
   }

   private void fireEntryAppliedEvent(Entry<T> entry) {
      synchronized (listeners) {
         for (Listener<T> listener : listeners) {
            listener.onLogEntryApplied(entry);
         }
      }
   }

   public void addListener(Listener<T> listener) {
      synchronized (listeners) {
         this.listeners.add(listener);
      }
   }

   public interface Listener<T extends StateMachine<T>> {
      public void onLogEntryApplied(Entry<T> entry);
   }

   public static interface Factory<T> {
      public T makeStateMachine();
   }

   public Peer addPeer(String host, int port, boolean bootstrap) {
      if (bootstrap) {
         peers.clear();
      }
      int peerId = 1;
      // find first available peerId
      while (peers.containsKey(peerId)) {
         peerId++;
      }
      Peer p = new Peer(peerId, host, port);
      peers.put(peerId, p);
      return p;
   }

   public void delPeer(int peerId) {
      peers.remove(peerId);
   }

   public Collection<Peer> getPeers() {
      return peers.values();
   }

   protected void applyHealthCheck(long val) {
      checksum ^= (val * index * ++count);
      //logger.info("CHECKSUM {} = {}:{}", val, checksum, count);
   }

   public long getChecksum() {
      return checksum;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName();
   }

}
