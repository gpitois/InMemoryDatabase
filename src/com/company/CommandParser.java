package com.company;

import java.util.Objects;

public class CommandParser {
    public void parseCommand(Database db, String cmdLine) {
        String[] args = cmdLine.split(" ");
        String command = args[0];
        switch (command) {
            case "END":
                System.exit(0);
                break;
            case "GET":
                if (!checkArgN(args, 1)) {
                    System.out.println("Wrong arguments");
                }
                System.out.println(db.get(args[1]));
                break;
            case "SET":
                if (!checkArgN(args, 1) || !checkArgN(args, 2)) {
                    System.out.println("Wrong arguments");
                }
                db.set(args[1], args[2]);
                break;
            case "UNSET":
                if (!checkArgN(args, 1)) {
                    System.out.println("Wrong arguments");
                }
                db.unset(args[1]);
                break;
            case "NUMEQUALTO":
                if (!checkArgN(args, 1)) {
                    System.out.println("Wrong arguments");
                }
                System.out.println(db.numEqualTo(args[1]));
                break;
            case "BEGIN":
                db.begin();
                break;
            case "ROLLBACK":
                System.out.println(db.rollback());
                break;
            case "COMMIT":
                System.out.println(db.commit());
                break;
            default:
                System.out.println("Unknown command");
        }
    }

    private boolean checkArgN(String [] args, int n) {
        if (n >= args.length) {
            return false;
        }
        String arg = args[n];
        return !arg.trim().isEmpty();
    }
}
