package io.tetrapod.raft;

import java.io.*;

public class Entry<T extends StateMachine> {

   final long       term;
   final long       index;
   final Command<T> command;

   public Entry(long term, long index, Command<T> command) {
      this.term = term;
      this.index = index;
      this.command = command;
   }

   /**
    * Read this command to from an input stream
    */
   @SuppressWarnings("unchecked")
   public Entry(DataInputStream in, T state) throws IOException {
      term = in.readLong();
      index = in.readLong();
      command = (Command<T>) state.makeCommand(in.readInt());
      command.read(in);
   }

   /**
    * Writes this entry to an output stream
    */
   public void write(DataOutputStream out, T state) throws IOException {
      out.writeLong(term);
      out.writeLong(index);
      out.writeInt(command.getCommandType());
      command.write(out);
   }

}
