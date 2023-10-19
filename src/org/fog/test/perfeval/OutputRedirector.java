package org.fog.test.perfeval;

import org.cloudbus.cloudsim.Log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.IOException;

public class OutputRedirector {

    /**
     * Redirects the standard output to a file. If the file doesn't exist, it will create it.
     *
     * @param filePath The path to the file.
     * @throws FileNotFoundException if the file exists but is a directory rather than a regular file or if the file cannot be opened or created for any other reason.
     */
    public static void redirectOutputToFile(String filePath) throws FileNotFoundException {
        File file = new File(filePath);

        // Check if file exists; if not, try to create it
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new FileNotFoundException("Failed to create the file: " + filePath);
            }
        }

        // Redirect the standard output to the file
        PrintStream fileOut = new PrintStream(file);
        System.setOut(fileOut);
        Log.setOutput(fileOut);
    }

    public static void main(String[] args) {
        try {
            redirectOutputToFile("output.txt");
            System.out.println("This will be written to the file!");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
