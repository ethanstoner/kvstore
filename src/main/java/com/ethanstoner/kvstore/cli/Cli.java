package com.ethanstoner.kvstore.cli;

/** Front-door command dispatcher: routes the first arg to a handler. */
public final class Cli {

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && "serve".equalsIgnoreCase(args[0])) {
            String[] sub = new String[args.length - 1];
            System.arraycopy(args, 1, sub, 0, sub.length);
            ServeCommand.run(sub);
            return;
        }
        Main.main(args);
    }
}
