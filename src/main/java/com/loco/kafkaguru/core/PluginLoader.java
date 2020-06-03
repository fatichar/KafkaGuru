package com.loco.kafkaguru.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loco.kafkaguru.MessageFormatter;
import lombok.extern.log4j.Log4j2;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.*;
import java.util.jar.JarFile;

class PluginInfo {
    public Map<String, String[]> classes = new HashMap<>();
}

@Log4j2
public class PluginLoader extends ClassLoader {
    private static final String PLUGIN_DIR = "plugins";
    private static final String PLUGIN_INFO_FILE = "PluginClasses.json";
    public static Map<String, MessageFormatter> formatters = new HashMap<>();
    private static ObjectMapper mapper = new ObjectMapper();
    public static final MessageFormatter defaultFormatter = createJsonFormatter();;

    public static void loadPlugins() {
        loadNativeFormatters();

        var pluginInfoFilePath = Paths.get(PLUGIN_DIR, PLUGIN_INFO_FILE).toString();
        var file = new File(pluginInfoFilePath);
        if (!file.exists()) {
            log.info("No plugins found");
            return;
        }
        PluginInfo pluginInfo = null;
        try {
            pluginInfo = mapper.readValue(file, PluginInfo.class);
            loadPlugins(pluginInfo);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void loadNativeFormatters() {
        formatters.put(defaultFormatter.name(), defaultFormatter);
    }

    private static MessageFormatter createJsonFormatter() {
        return new MessageFormatter() {
            private ObjectMapper mapper = new ObjectMapper();

            @Override
            public String name() {
                return "Json";
            }

            @Override
            public String format(byte[] data) {
                var text = new String(data);
                try {
                    var object = mapper.readValue(text, Object.class);
                    var json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
                    return json;
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                return text;
            }
        };
    }

    public static void loadPlugins(PluginInfo pluginInfo) {

        for (var jarName : pluginInfo.classes.keySet()) {
            loadPlugins(jarName, pluginInfo.classes.get(jarName));
        }
        return;
    }

    private static void loadPlugins(String jarName, String[] classNames) {
        JarFile jarFile = null;
        try {
            var jarFilePath = Paths.get(PLUGIN_DIR, jarName).toString();
            jarFile = new JarFile(jarFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        URL[] urls = new URL[0];
        try {
            urls = new URL[] {new URL("jar:file:" + jarFile.getName() + "!/")};
        } catch (MalformedURLException ex) {
            ex.printStackTrace();
        }
        URLClassLoader cl = URLClassLoader.newInstance(urls);

        for (var className : classNames) {
            // String className = je.getName().substring(0, je.getName().length() - 6);
            try {
                var c = cl.loadClass(className);
                System.out.println("loaded class " + c.getName());
                try {
                    var foreignFormatter = c.getDeclaredConstructor().newInstance();
                    System.out.println("Adding formatter " + c.getName());
                    var localFormatter = new CustomMessageFormatter(foreignFormatter);
                    System.out.println("Added formatter " + c.getName());
                    formatters.put(localFormatter.name(), localFormatter);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
            } catch (Exception ex) {

            }
        }
    }
}
