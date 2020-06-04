package com.cisco.wap.server;

import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.Map;

public class ServerOptions {
    final String[] shortForm;
    final String[] longForm;
    final String[] desc;

    public ServerOptions(String[] shortForm, String[] longForm, String[] desc) {
        this.shortForm = shortForm;
        this.longForm = longForm;
        this.desc = desc;
    }

    public Map<String, String> parseOptions(String tag, String[] args) {
        Options options = new Options();
        for (int i = 0; i < shortForm.length; i++) {
            Option input = new Option(shortForm[i], longForm[i], true, desc[i]);
            input.setRequired(false);
            options.addOption(input);
        }

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter  = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp(tag, options);
            System.exit(1);
        }

        Map<String, String> ret = new HashMap<>();
        for (int i = 0; i < shortForm.length; i++) {
            String retThis = cmd.getOptionValue(longForm[i]);
            if (retThis != null)
                ret.put(longForm[i], retThis);
        }
        return ret;
    }
}
