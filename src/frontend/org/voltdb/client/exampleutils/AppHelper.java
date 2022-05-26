/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.client.exampleutils;

import java.util.Set;
import java.util.TreeSet;

/**
 * AppHelper provides a basic framework to manage user arguments for a
 * command line application. A typical usage for the helper would go as:
 * <pre>
 * {@code
 *
 * // Define the application arguments and assign them from the command line
 * AppHelper apph = new AppHelper(MyApplication.class.getCanonicalName())
 *     .add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost")
 *     .add("port", "port_number", "Client port to connect to on cluster nodes.", 21212)
 *     .add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 120)
 *     .setArguments(args)
 * ;
 *
 * // Retrieve parameters as strongly-typed values
 * String servers       = apph.stringValue("servers");
 * int port             = apph.intValue("port");
 * long duration        = apph.longValue("duration");
 *
 * // Validate parameters
 * apph.validate("port", (port > 0) && (port < 65535))
 *     .validate("duration", (duration > 0))
 * ;
 *
 * // Display actual parameters, for reference
 * apph.printActualUsage();
 *
 * // Proceed with application: all arguments have been parsed, validated and can now be used.
 *
 * }
 * </pre>
 *
 * @author Seb Coursol
 * @since 2.0
 */
public class AppHelper
{
    /**
     * A wrapper for a user argument that defines its properties and value.
     */
    private static class Argument implements Comparable<Argument>
    {
        private static int ArgumentCounter = 0;
        private final int ArgumentOrder;
        public final String Name;
        public final String DisplayValue;
        public final String Description;
        public final String DefaultValue;
        public final boolean Optional;
        public String Value = null;
        /**
         * Creates a new argument with specific properties such as a name, description or default value (for an optional argument).
         *
         * @param name the name of the argument on the command line (case-sensitive).
         * @param displayValue the sample argument value to show in the usage print-out.
         * @param description the description for the argument as displayed in the usage print-out.
         * @param defaultValue the value the argument should have if not defined on the command line call.
         */
        public Argument(String name, String displayValue, String description, String defaultValue)
        {
            this.ArgumentOrder = ArgumentCounter++;
            this.Name = name;
            this.DisplayValue = displayValue;
            this.Description = description;
            this.DefaultValue = defaultValue;
            this.Optional = (defaultValue != null);
        }
        /**
         * {@inheritDoc}
         */
        @Override
        public int compareTo(Argument a)
        {
            if (this.ArgumentOrder < a.ArgumentOrder)
                return -1;
            else if (this.ArgumentOrder == a.ArgumentOrder)
                return 0;
            return 1;
        }
    }

    /**
     * List of arguments for this application.
     */
    private final Set<Argument> Arguments = new TreeSet<Argument>();

    /**
     * Name (Class name) for this application.
     */
    private final String Name;

    /**
     * Create a new helper for the application with the given name. The name is
     * used in the usage printout to document the generic command line call.
     *
     * A default command-line argument with the name "stats" will be added. It
     * is the file to save statistics to at the end of run. If it is not set by
     * the user, no file will be created.
     *
     * @param name
     *            the name of the application used for display in the usage
     *            print-out.
     */
    public AppHelper(String name)
    {
        this.Name = name;

        // Default command-line argument: file to save the statistics
        add("statsfile", "statsfile", "File to save statistics to", "");
    }

    /**
     * Adds an argument to this application helper.
     *
     * @param name the (case-sensitive) argument name.
     * @param description the description for the argument to be displayed in the usage print-out.
     * @return reference to this application helper, allowing command-chaining.
     */
    public AppHelper add(String name, String description)
    {
        add(name, null, description, null);
        return this;
    }

    /**
     * Adds an argument to this application helper.
     *
     * @param name the (case-sensitive) argument name.
     * @param displayValue the display value to be shown in the usage print-out for this argument.  For instance, a number or specific expected format.
     * @param description the description for the argument to be displayed in the usage print-out.
     * @return reference to this application helper, allowing command-chaining.
     */
    public AppHelper add(String name, String displayValue, String description)
    {
        add(name, displayValue, description, null);
        return this;
    }

    /**
     * Adds an argument to this application helper.
     *
     * @param name the (case-sensitive) argument name.
     * @param displayValue the display value to be shown in the usage print-out for this argument.  For instance, a number or specific expected format.
     * @param description the description for the argument to be displayed in the usage print-out.
     * @param defaultValue the default value (as an Object) to be used for an optional argument.  If no default value is provided, the argument will be required in the command line call.
     * @return reference to this application helper, allowing command-chaining.
     */
    public AppHelper add(String name, String displayValue, String description, Object defaultValue)
    {
        add(name, displayValue, description, defaultValue.toString());
        return this;
    }

    /**
     * Adds an argument to this application helper.
     *
     * @param name the (case-sensitive) argument name.
     * @param displayValue the display value to be shown in the usage print-out for this argument.  For instance, a number or specific expected format.
     * @param description the description for the argument to be displayed in the usage print-out.
     * @param defaultValue the default value (as a String) to be used for an optional argument.  If no default value is provided, the argument will be required in the command line call.
     * @return reference to this application helper, allowing command-chaining.
     */
    public AppHelper add(String name, String displayValue, String description, String defaultValue)
    {
        Arguments.add(new Argument(name, displayValue, description, defaultValue));
        return this;
    }

    /**
     * Prints out the application usage based on the provided list of arguments and their properties.
     * This print-out is provided automatically if the user calls the application with the --help flag, or provides an invalid command line.
     */
    public void printUsage()
    {
        System.out.printf("Usage: %s --help\n   or: %s ", this.Name, this.Name);
        for(Argument a : Arguments)
        {
            if(a.Optional)
                System.out.printf("[--%s=%s]\n           ", a.Name, (a.DisplayValue == null ? "value" : a.DisplayValue));
            else
                System.out.printf("--%s=%s\n           ", a.Name, (a.DisplayValue == null ? "value" : a.DisplayValue));
        }
        for(Argument a : Arguments)
        {
            if(a.Optional)
                System.out.printf("\n[--%s=%s]\n", a.Name, (a.DisplayValue == null ? "value" : a.DisplayValue));
            else
                System.out.printf("\n--%s=%s\n", a.Name, (a.DisplayValue == null ? "value" : a.DisplayValue));
            System.out.printf("  %s\n", a.Description);
            if(a.Optional)
                System.out.printf("  Default: %s\n", a.DefaultValue);
        }
    }

    /**
     * Prints out an error message and the application usage print-out, then terminates the application.
     */
    private void printErrorAndQuit(String message)
    {
        System.out.println(message + "\n-------------------------------------------------------------------------------------\n");
        printUsage();
        System.exit(-1);
    }

    /**
     * Prints a full list of actual arguments that will be used by the application after interpretation of defaults and actual argument values as passed by the user on the command line.
     *
     * @return reference to this application helper, allowing command-chaining.
     */
    public AppHelper printActualUsage()
    {
        System.out.println("-------------------------------------------------------------------------------------");
        int maxLength = 24;
        for(Argument a : Arguments)
            if (maxLength < a.Name.length())
                maxLength = a.Name.length();
        for(Argument a : Arguments)
        {
            String template = "%1$" + String.valueOf(maxLength-1) + "s : ";
            System.out.printf(template, a.Name);
            System.out.println(a.Value);
        }
        System.out.println("-------------------------------------------------------------------------------------");
        return this;
    }

    /**
     * Performs a pass-through or application termination as a result of an argument validation.  For instance, the application could call: <code>helper.validate("number_argument", val > 0 && val < 100);</code> to ensure the argument value is in the specified range and force the application to terminate if the test fails, printing an adequate error message and usage information.
     *
     * @param name the (case-sensitive) name of the argument to validate.
     * @param valid the boolean result of the validation test for the argument.
     * @return reference to this application helper, allowing command-chaining.
     */
    public AppHelper validate(String name, boolean valid)
    {
        if (!valid)
            printErrorAndQuit("Invalid parameter value: " + name);
        return this;
    }

    /**
     * Parses the command line to retrieve all the arguments.  This method should be called after all arguments have been defined (added) for this application.
     *
     * @param args the command line arguments (typically, the String[] passed to the "main" method).
     * @return reference to this application helper, allowing command-chaining.
     */
    public AppHelper setArguments(String[] args)
    {
        for(int i=0;i<args.length;i++)
        {
            if (args[i].equals("--help"))
            {
                printUsage();
                System.exit(0);
            }
            else
            {
                boolean isValid = false;
                for(Argument a : Arguments)
                {
                    if (args[i].startsWith("--" + a.Name + "="))
                    {
                        a.Value = args[i].split("=")[1];
                        isValid = true;
                    }
                    else if (args[i].equals("--" + a.Name))
                    {
                        a.Value = args[++i];
                        isValid = true;
                    }
                }
                if (!isValid)
                    printErrorAndQuit("Invalid Parameter: " + args[i]);
            }
        }
        for(Argument a : Arguments)
        {
            if (a.Value == null)
            {
                if (a.Optional)
                    a.Value = a.DefaultValue;
                else
                    printErrorAndQuit("Missing Required Parameter: " + a.Name);
            }
        }
        return this;
    }

    /**
     * Retrieves an argument instance given the (case-sensitive) argument name.
     *
     * @param name the (case-sensitive) name of the argument to retrieve.
     * @return argument instance holding the properties and value of the command line argument.
     */
    private Argument getArgumentByName(String name)
    {
        for(Argument a : Arguments)
            if (a.Name.equals(name))
                return a;
        return null;
    }

    /**
     * Retrieves the value of an argument as a byte.
     *
     * @param name the (case-sensitive) name of the argument to retrieve.
     * @return the value of the argument cast as a byte.  Will terminate the application if a required argument is found missing or the casting call fails, printing out detailed usage.
     */
    public byte byteValue(String name)
    {
        try
        {
            return Byte.valueOf(this.getArgumentByName(name).Value);
        }
        catch(NullPointerException npe)
        {
            printErrorAndQuit(String.format("Argument '%s' was not provided.", name));
        }
        catch(Exception x)
        {
            printErrorAndQuit(String.format("Argument '%s' could not be cast to type: 'byte'.", name));
        }
        return -1; // We will never get here: printErrorAndQuit will have terminated the application!
    }

    /**
     * Retrieves the value of an argument as a short.
     *
     * @param name the (case-sensitive) name of the argument to retrieve.
     * @return the value of the argument cast as a short.  Will terminate the application if a required argument is found missing or the casting call fails, printing out detailed usage.
     */
    public short shortValue(String name)
    {
        try
        {
            return Short.valueOf(this.getArgumentByName(name).Value);
        }
        catch(NullPointerException npe)
        {
            printErrorAndQuit(String.format("Argument '%s' was not provided.", name));
        }
        catch(Exception x)
        {
            printErrorAndQuit(String.format("Argument '%s' could not be cast to type: 'short'.", name));
        }
        return -1; // We will never get here: printErrorAndQuit will have terminated the application!
    }

    /**
     * Retrieves the value of an argument as a int.
     *
     * @param name the (case-sensitive) name of the argument to retrieve.
     * @return the value of the argument cast as a int.  Will terminate the application if a required argument is found missing or the casting call fails, printing out detailed usage.
     */
    public int intValue(String name)
    {
        try
        {
            return Integer.valueOf(this.getArgumentByName(name).Value);
        }
        catch(NullPointerException npe)
        {
            printErrorAndQuit(String.format("Argument '%s' was not provided.", name));
        }
        catch(Exception x)
        {
            printErrorAndQuit(String.format("Argument '%s' could not be cast to type: 'int'.", name));
        }
        return -1; // We will never get here: printErrorAndQuit will have terminated the application!
    }

    /**
     * Retrieves the value of an argument as a long.
     *
     * @param name the (case-sensitive) name of the argument to retrieve.
     * @return the value of the argument cast as a long.  Will terminate the application if a required argument is found missing or the casting call fails, printing out detailed usage.
     */
    public long longValue(String name)
    {
        try
        {
            return Long.valueOf(this.getArgumentByName(name).Value);
        }
        catch(NullPointerException npe)
        {
            printErrorAndQuit(String.format("Argument '%s' was not provided.", name));
        }
        catch(Exception x)
        {
            printErrorAndQuit(String.format("Argument '%s' could not be cast to type: 'long'.", name));
        }
        return -1; // We will never get here: printErrorAndQuit will have terminated the application!
    }

    /**
     * Retrieves the value of an argument as a double.
     *
     * @param name the (case-sensitive) name of the argument to retrieve.
     * @return the value of the argument cast as a double.  Will terminate the application if a required argument is found missing or the casting call fails, printing out detailed usage.
     */
    public double doubleValue(String name)
    {
        try
        {
            return Double.valueOf(this.getArgumentByName(name).Value);
        }
        catch(NullPointerException npe)
        {
            printErrorAndQuit(String.format("Argument '%s' was not provided.", name));
        }
        catch(Exception x)
        {
            printErrorAndQuit(String.format("Argument '%s' could not be cast to type: 'double'.", name));
        }
        return -1; // We will never get here: printErrorAndQuit will have terminated the application!
    }

    /**
     * Retrieves the value of an argument as a string.
     *
     * @param name the (case-sensitive) name of the argument to retrieve.
     * @return the value of the argument as a string (as passed on the command line).  Will terminate the application if a required argument is found missing, printing out detailed usage.
     */
    public String stringValue(String name)
    {
        try
        {
            return this.getArgumentByName(name).Value;
        }
        catch(Exception npe)
        {
            printErrorAndQuit(String.format("Argument '%s' was not provided.", name));
        }
        return null; // We will never get here: printErrorAndQuit will have terminated the application!
    }

    /**
     * Retrieves the value of an argument as a boolean.
     *
     * @param name the (case-sensitive) name of the argument to retrieve.
     * @return the value of the argument cast as a boolean.  Will terminate the application if a required argument is found missing or the casting call fails, printing out detailed usage.
     */
    public boolean booleanValue(String name)
    {
        try
        {
            return Boolean.valueOf(this.getArgumentByName(name).Value);
        }
        catch(NullPointerException npe)
        {
            printErrorAndQuit(String.format("Argument '%s' was not provided.", name));
        }
        catch(Exception x)
        {
            printErrorAndQuit(String.format("Argument '%s' could not be cast to type: 'boolean'.", name));
        }
        return false; // We will never get here: printErrorAndQuit will have terminated the application!
    }
}
