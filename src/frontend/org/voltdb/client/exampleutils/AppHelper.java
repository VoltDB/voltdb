/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.client.exampleutils;

import java.util.Set;
import java.util.TreeSet;

public class AppHelper
{
    static class Argument implements Comparable<Argument>
    {
        private static int ArgumentCounter = 0;
        private final int ArgumentOrder;
        public final String Name;
        public final String DisplayValue;
        public final String Description;
        public final String DefaultValue;
        public final boolean Optional;
        public String Value = null;
        public Argument(String name, String displayValue, String description, String defaultValue)
        {
            this.ArgumentOrder = ArgumentCounter++;
            this.Name = name;
            this.DisplayValue = displayValue;
            this.Description = description;
            this.DefaultValue = defaultValue;
            this.Optional = (defaultValue != null);
        }
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
    public final Set<Argument> Arguments = new TreeSet<Argument>();
    public final String AppName;
    public AppHelper(String appName)
    {
        this.AppName = appName;
    }

    public AppHelper add(String name, String description)
    {
        add(name, null, description, null);
        return this;
    }
    public AppHelper add(String name, String displayValue, String description)
    {
        add(name, displayValue, description, null);
        return this;
    }
    public AppHelper add(String name, String displayValue, String description, Object defaultValue)
    {
        add(name, displayValue, description, defaultValue.toString());
        return this;
    }
    public AppHelper add(String name, String displayValue, String description, String defaultValue)
    {
        Arguments.add(new Argument(name, displayValue, description, defaultValue));
        return this;
    }

    public void printUsage()
    {
        System.out.printf("Usage: %s --help\n   or: %s ", this.AppName, this.AppName);
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

    private void printErrorAndQuit(String message)
    {
        System.out.println(message + "\n-------------------------------------------------------------------------------------\n");
        printUsage();
        System.exit(-1);
    }

    public AppHelper printActualUsage()
    {
        System.out.println("-------------------------------------------------------------------------------------");
        int maxLength = 24;
        for(Argument a : Arguments)
            if (maxLength < a.Name.length())
                maxLength = a.Name.length();
        for(Argument a : Arguments)
        {
            System.out.printf("%1$#" + (maxLength-1) + "s : ", a.Name);
            System.out.println(a.Value);
        }
        System.out.println("-------------------------------------------------------------------------------------");
        return this;
    }

    public AppHelper validate(String name, boolean valid)
    {
        if (!valid)
            printErrorAndQuit("Invalid parameter value: " + name);
        return this;
    }

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

    private Argument getArgumentByName(String name)
    {
        for(Argument a : Arguments)
            if (a.Name.equals(name))
                return a;
        return null;
    }
    public byte byteValue(String name)
    {
        return Byte.valueOf(this.getArgumentByName(name).Value);
    }
    public short shortValue(String name)
    {
        return Short.valueOf(this.getArgumentByName(name).Value);
    }
    public int intValue(String name)
    {
        return Integer.valueOf(this.getArgumentByName(name).Value);
    }
    public long longValue(String name)
    {
        return Long.valueOf(this.getArgumentByName(name).Value);
    }
    public double doubleValue(String name)
    {
        return Double.valueOf(this.getArgumentByName(name).Value);
    }
    public String stringValue(String name)
    {
        return this.getArgumentByName(name).Value;
    }
    public boolean booleanValue(String name)
    {
        return Boolean.valueOf(this.getArgumentByName(name).Value);
    }
}
