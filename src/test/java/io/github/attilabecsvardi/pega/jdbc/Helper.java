package io.github.attilabecsvardi.pega.jdbc;

import java.io.File;
import java.io.InputStream;

public class Helper {

    /**
     * utility to expose property file resource
     *
     * @param path
     * @return
     */
    public static final InputStream loadPropertyFile(String path) {
        File f = new File(".");
        System.out.println(f.getAbsoluteFile());

        return Helper.class.getResourceAsStream(path);
    }
}
