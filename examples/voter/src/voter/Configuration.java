package voter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;

public abstract class Configuration {

    @Retention(RetentionPolicy.RUNTIME) // Make this annotation accessible at runtime via reflection.
    @Target({ElementType.FIELD})       // This annotation can only be applied to class methods.
    public @interface Option {
        String opt() default "";
        boolean hasArg() default true;
        boolean required() default false;
        String desc() default "";
    }

    protected final Options options = new Options();

    public void exitWithMessageAndUsage(String msg) {
        System.exit(-1);
    }

    public void printUsage() {

    }

    private void assignValueToField(Field field, String value) throws Exception {
        if ((value == null) || (value.length() == 0)) {
            return;
        }

        Class<?> cls = field.getClass();

        if ((cls == boolean.class) || (cls == Boolean.class))
            field.set(this, Boolean.parseBoolean(value));
        else if ((cls == byte.class) || (cls == Byte.class))
            field.set(this, Byte.parseByte(value));
        else if ((cls == short.class) || (cls == Short.class))
            field.set(this, Short.parseShort(value));
        else if ((cls == int.class) || (cls == Integer.class))
            field.set(this, Integer.parseInt(value));
        else if ((cls == long.class) || (cls == Long.class))
            field.set(this, Long.parseLong(value));
        else if ((cls == float.class) || (cls == Float.class))
            field.set(this, Float.parseFloat(value));
        else if ((cls == double.class) || (cls == Double.class))
            field.set(this, Double.parseDouble(value));
        else if ((cls == char.class) || (cls == Character.class) || (cls == String.class))
            field.set(this, value);
        else
            assert(false);
    }

    public void parse(String[] args) {
        try {
            for (Field field : getClass().getFields()) {
                if (field.isAnnotationPresent(Option.class) == false) {
                    continue;
                }

                Option option = field.getAnnotation(Option.class);

                String opt = option.opt();
                if (opt == null) opt = field.getName();

                options.addOption(opt, option.hasArg(), option.desc());
            }

            CommandLineParser parser = new GnuParser();
            CommandLine cmd = parser.parse(options, args);

            for (Field field : getClass().getFields()) {
                if (field.isAnnotationPresent(Option.class) == false) {
                    continue;
                }

                Option option = field.getAnnotation(Option.class);

                String opt = option.opt();
                if (opt == null) opt = field.getName();

                if (cmd.hasOption(opt)) {
                    if (option.hasArg()) {
                        assignValueToField(field, cmd.getOptionValue(opt));
                    }
                    else {
                        if (field.getClass().equals(boolean.class) || field.getClass().equals(Boolean.class)) {
                            try {
                                field.set(this, true);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        else {
                            printUsage();
                        }
                    }
                }
                else {
                    if (option.required()) {
                        printUsage();
                    }
                }
            }

            // check that the values read are valid
            // this code is specific to your app
            validate();
        }

        catch (Exception e) {
            System.err.println("Parsing failed. Reason: " + e.getMessage());
            printUsage();
        }
    }

    public abstract void validate();
}
