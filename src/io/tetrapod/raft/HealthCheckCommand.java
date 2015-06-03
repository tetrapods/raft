package io.tetrapod.raft;

import java.io.*;

public class HealthCheckCommand<T extends StateMachine<T>> implements Command<T> {
   public static final int COMMAND_ID = StateMachine.COMMAND_ID_HEALTH_CHECK;

   public HealthCheckCommand() {}

   @Override
   public void applyTo(T state) {}

   @Override
   public void write(DataOutputStream out) throws IOException {}

   @Override
   public void read(DataInputStream in) throws IOException {}

   @Override
   public int getCommandType() {
      return COMMAND_ID;
   }

}
