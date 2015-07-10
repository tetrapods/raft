package io.tetrapod.raft;

import java.io.*;

/**
 * A somewhat dummy command to mark the start of a new term. The new leader always publishes this as the first command of their new term,
 * which helps facilitate advancing the commitIndex, as we cannot commit any older log entries until we've replicated something in the new
 * term to a majority.
 */
public class NewTermCommand<T extends StateMachine<T>> implements Command<T> {

   private long term;
   private int  peerId;

   public NewTermCommand() {}

   public NewTermCommand(int peerId, long term) {
      this.peerId = peerId;
      this.term = term;
   }

   @Override
   public void applyTo(T state) {}

   @Override
   public void write(DataOutputStream out) throws IOException {
      out.writeLong(term);
      out.writeInt(peerId);
   }

   @Override
   public void read(DataInputStream in, int fileVersion) throws IOException {
      term = in.readLong();
      peerId = in.readInt();
   }

   @Override
   public int getCommandType() {
      return StateMachine.COMMAND_ID_NEW_TERM;
   }

}
