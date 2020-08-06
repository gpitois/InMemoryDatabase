package com.company;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {

    public static void main(String[] args) {
        Database db = new Database();
        CommandParser parser = new CommandParser();
        while (true) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String command = null;
            try {
                command = reader.readLine();
                parser.parseCommand(db, command);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
