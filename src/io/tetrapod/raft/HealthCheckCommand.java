package io.tetrapod.raft;

import java.io.*;
import java.security.SecureRandom;

public class HealthCheckCommand<T extends StateMachine<T>> implements Command<T> {
   public static final int COMMAND_ID = StateMachine.COMMAND_ID_HEALTH_CHECK;

   private static final SecureRandom random                  = new SecureRandom();
   
   private long            val;

   
   public HealthCheckCommand() {
     val = random.nextLong();
   }

   @Override
   public void applyTo(T state) {
      state.applyHealthCheck(val);
   }

   @Override
   public void write(DataOutputStream out) throws IOException {     out.writeLong(val);}

   @Override
   public void read(DataInputStream in) throws IOException { this.val = in.readLong();}

   @Override
   public int getCommandType() {
      return COMMAND_ID;
   }

}
