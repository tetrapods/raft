package io.tetrapod.raft;

import java.io.*;

/**
 * Formally removes a raft peer from the cluster. The peer is considered to have left the of quorum after this command is committed.
 */
public class DelPeerCommand<T extends StateMachine<T>> implements Command<T> {

   public int peerId;

   public DelPeerCommand() {}

   public DelPeerCommand(int peerId) {
      this.peerId = peerId;
   }

   @Override
   public void applyTo(T state) {
      state.delPeer(peerId);
   }

   @Override
   public void write(DataOutputStream out) throws IOException {
      out.writeInt(peerId);

   }

   @Override
   public void read(DataInputStream in) throws IOException {
      peerId = in.readInt();
   }

   @Override
   public int getCommandType() {
      return StateMachine.COMMAND_ID_DEL_PEER;
   }

}
