package io.tetrapod.raft;

import java.io.*;

/**
 * A command deterministically updates a state machine
 */
public interface Command<T extends StateMachine<T>> {

   /**
    * Based on this command, deterministically update the state machine
    */
   public void applyTo(T state);

   /**
    * Writes this command to an output stream
    */
   public void write(DataOutputStream out) throws IOException;

   /**
    * Read this command to from an input stream
    */
   public void read(DataInputStream in) throws IOException;

   /**
    * Get a unique and stable integer id for this command type
    */
   public int getCommandType();

}
