/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package bigjar;

import org.voltdb.CLIConfig;
import org.voltdb.CLIConfig.Option;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Creates either a large file containing random text, or a bunch of Java source
 * files, either or both of which may be used to create very large .jar files,
 * which are used to test loading such files, via @UpdateClasses.
 **/
public class CreateLargeFiles {

    /** The characters from which random text is chosen; includes multiple
     *  spaces, to make them more frequent **/
    private static final String POSSIBLE_CHARACTERS =
            "ABCD EFG HIJ KLM NOP QRS TUV WXYZ abcd efg hij klm nop qrs tuv wxyz ";

    /** The current configuration, containing various option values **/
    private final Config config;


    /** CreateLargeFiles Constuctor **/
    private CreateLargeFiles(Config config) {
        this.config = config;
    }


    /**
     * Inherits from the {@link CLIConfig} class, to declaratively state command
     * line options with defaults and validation.
     */
    private static class Config extends CLIConfig {
        @Option(shortOpt = "f", opt = "filetype", desc = "The type of file(s) "
                + "to be generated: either 'text' or 'java'; default 'text'")
        String generateFileType = "text";

        @Option(shortOpt = "o", opt = "outputfile", desc = "The name of the (large, random) "
                + "text output file; ignored for 'java'; default 'large-random-text.jar'")
        String outputTextFileName = "large-random-text.jar";

        @Option(shortOpt = "l", opt = "numtextlines", desc = "The number of lines "
                + "in the (text) output file; ignored for 'java'; default 800000")
        int numTextLines = 850000;  // results in a .jar file a little under 50Mb

        @Option(shortOpt = "c", opt = "numchars", desc = "The number of characters per "
                + "line, in the (text) output file; ignored for 'java'; default 80")
        int numCharsPerLine = 80;

        @Option(shortOpt = "d", opt = "templatedir", desc = "The directory in "
                + "which to find the Java template file to be copied and modified; "
                + "ignored for 'text'; default './src/bigjar/procedures'")
        String javaTemplateDirectory = "./procedures";

        @Option(shortOpt = "t", opt = "templatefile", desc = "The name of the "
                + "Java template file to be copied and modified; ignored for "
                + "'text'; default 'SelectLEtbd.java'")
        String javaTemplateFilename = "SelectLEtbd.java";

        @Option(shortOpt = "r", opt = "replace", desc = "The substring in the "
                + "Java template file, which is to be replaced in each copy, "
                + "including in the filename; ignored for 'text'; default 'tbd'")
        String javaReplaceSubstring = "tbd";

        @Option(shortOpt = "m", opt = "minfilenum", desc = "The minimum (int) "
                + "value to be used in copies of the Java template file; "
                + "ignored for 'text'; default 1")
        int javaFileMinValue = 1;

        @Option(shortOpt = "M", opt = "maxfilenum", desc = "The maximum (int) "
                + "value to be used in copies of the Java template file; "
                + "ignored for 'text'; default 10 (TODO)")
        int javaFileMaxValue = 10;  // TODO: make this bigger! (how big?)

        @Override
        public void validate() {
            if (!Arrays.asList("text", "java").contains(generateFileType.toLowerCase())) {
                exitWithMessageAndUsage("filetype ("+generateFileType
                        + ") must be 'text' or 'java'");
            }
            if (numTextLines <= 0) exitWithMessageAndUsage("numtextlines ("
                        + numTextLines+") must be > 0");
            if (numCharsPerLine <= 0) exitWithMessageAndUsage("numchars ("
                        + numCharsPerLine+") must be > 0");
            if (!javaTemplateFilename.contains(javaReplaceSubstring)) {
                exitWithMessageAndUsage("templatefile ("+javaTemplateFilename
                        + ") must contain replace substring ("+javaReplaceSubstring+")");
            }
            if (javaFileMaxValue < javaFileMinValue) {
                exitWithMessageAndUsage("maxfilenum ("+javaFileMaxValue
                        + ") must be >= minfilenum ("+javaFileMinValue+")");
            }
        }
    }

    public void createLargeTextFile() throws IOException {
        Path outputFile = Paths.get(config.outputTextFileName);
        Files.write(outputFile, Arrays.asList("Randomly generated text:"),
                StandardCharsets.UTF_8, StandardOpenOption.CREATE);

        Random random = new Random();
        List<String> lines = new ArrayList<String>();
        for (long i=0; i < config.numTextLines; i++) {
            StringBuffer line = new StringBuffer(config.numCharsPerLine);
            for (int j=0; j < config.numCharsPerLine; j++) {
                int index = random.nextInt(POSSIBLE_CHARACTERS.length());
                line.append(POSSIBLE_CHARACTERS.charAt(index));
            }
            lines.add(line.toString());
        }
        Files.write(outputFile, lines, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    }

    public void createJavaClassFiles() throws IOException {
        // Get the template file
        Path templateFile = Paths.get(config.javaTemplateDirectory+"/"+config.javaTemplateFilename);
        List<String> templateFileLines = Files.readAllLines(templateFile);

        // TODO: Debug print!
//        System.out.println("createJavaClassFiles: (config.)...");
//        System.out.println("  generateFileType     :"+config.generateFileType);
//        System.out.println("  javaTemplateDirectory:"+config.javaTemplateDirectory);
//        System.out.println("  javaTemplateFilename :"+config.javaTemplateFilename);
//        System.out.println("  javaReplaceSubstring :"+config.javaReplaceSubstring);
//        System.out.println("  javaFileMinValue     :"+config.javaFileMinValue);
//        System.out.println("  javaFileMaxValue     :"+config.javaFileMaxValue);
//        System.out.println("  templateFile         :"+templateFile);
//        System.out.println("  templateFileLines    :\n"+templateFileLines);

        // Make copies of the template file, slightly modified
        String replace = config.javaReplaceSubstring;
        // TODO: Debug print!
//        System.out.println("  replace              :"+replace);
        for (int i=config.javaFileMinValue; i <= config.javaFileMaxValue; i++) {
            String replacement = String.valueOf(i);
            List<String> outputJavaFileLines = new ArrayList<String>(templateFileLines.size());
            for (int j=0; j < templateFileLines.size(); j++) {
                outputJavaFileLines.add(templateFileLines.get(j)
                                        .replace(replace, replacement));
            }
            // TODO: Debug print!
//            System.out.println("\nreplacement            :"+replacement);
//            System.out.println("  outputJavaFileLines  :"+outputJavaFileLines.size());
            String outputJavaFileName = config.javaTemplateFilename.replace(replace, replacement);
            Path outputJavaFile = Paths.get(config.javaTemplateDirectory+"/"+outputJavaFileName);
            Files.write(outputJavaFile, outputJavaFileLines,
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE);
            // TODO: Debug print!
//            System.out.println("  outputJavaFileName   :"+outputJavaFileName);
//            System.out.println("  outputJavaFile       :"+outputJavaFile);
        }
    }

    public static void main(String[] args) throws IOException {
        Config config = new Config();
        config.parse(CreateLargeFiles.class.getName(), args);

        CreateLargeFiles clf = new CreateLargeFiles(config);
        if ("java".equalsIgnoreCase(config.generateFileType)) {
            clf.createJavaClassFiles();
        } else {
            clf.createLargeTextFile();
        }
    }

}
