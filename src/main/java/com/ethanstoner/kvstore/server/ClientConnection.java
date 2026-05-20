package com.ethanstoner.kvstore.server;

import com.ethanstoner.kvstore.server.resp.RespParser;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

/** One per accepted TCP connection. Runs in its own virtual thread. */
final class ClientConnection implements Runnable {

    private final Socket socket;
    private final KvServer server;
    private final CommandHandler handler;

    ClientConnection(Socket socket, KvServer server, CommandHandler handler) {
        this.socket = socket;
        this.server = server;
        this.handler = handler;
    }

    @Override
    public void run() {
        server.registerConnection(this);
        try (InputStream in = new BufferedInputStream(socket.getInputStream());
             OutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            while (true) {
                String[] cmd;
                try {
                    cmd = RespParser.readCommand(in);
                } catch (EOFException | SocketException eof) {
                    return;
                }
                if (cmd == null) return;

                handler.handle(cmd, out);
                out.flush();
                server.recordCommand();

                if (cmd.length > 0 && "QUIT".equalsIgnoreCase(cmd[0])) {
                    return;
                }
            }
        } catch (IOException ignored) {
        } finally {
            closeQuietly();
            server.unregisterConnection(this);
        }
    }

    void requestStop() {
        closeQuietly();
    }

    private void closeQuietly() {
        try { socket.close(); } catch (IOException ignored) {}
    }
}
