package io.tetrapod.raft;

import java.io.*;

/**
 * Formally add a raft peer to the cluster. The peer is considered part of quorum after this command is committed.
 */
public class AddPeerCommand<T extends StateMachine<T>> implements Command<T> {

   public String  host;
   public int     port;
   public int     peerId;
   public boolean bootstrap;

   public AddPeerCommand() {}

   public AddPeerCommand(String host, int port) {
      this.host = host;
      this.port = port;
      this.bootstrap = false;
   }

   public AddPeerCommand(String host, int port, boolean bootstrap) {
      this.host = host;
      this.port = port;
      this.bootstrap = bootstrap;
   }

   @Override
   public void applyTo(T state) {
      peerId = state.addPeer(host, port, bootstrap).peerId;
   }

   @Override
   public void write(DataOutputStream out) throws IOException {
      out.writeInt(peerId);
      out.writeUTF(host);
      out.writeInt(port);
      out.writeBoolean(bootstrap);
   }

   @Override
   public void read(DataInputStream in) throws IOException {
      peerId = in.readInt();
      host = in.readUTF();
      port = in.readInt();
      bootstrap = in.readBoolean();
   }

   @Override
   public int getCommandType() {
      return StateMachine.COMMAND_ID_ADD_PEER;
   }

}
