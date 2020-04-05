package com.loco.kafkaguru;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

import net.rgielen.fxweaver.core.FxWeaver;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;

/**
 * JavaFX App
 */
@SpringBootApplication
public class App extends Application {

    private static Scene scene;
    private static Parent rootNode;
    private static ConfigurableApplicationContext context;

    @Override
    public void init() throws Exception {
        String[] args = getParameters().getRaw().toArray(new String[0]);

        context = new SpringApplicationBuilder()
            .sources(SpringBootExampleApplication.class)
            .run(args);
    }
    @Override
    public void start(Stage stage) throws IOException {
        FxWeaver fxWeaver = context.getBean(FxWeaver.class);
        rootNode = fxWeaver.loadView(MainWindowController.class);
        Scene scene = new Scene(rootNode);
        stage.setScene(scene);
        stage.show();
    }

    @Override
    public void stop() {
        this.context.close();
        Platform.exit();
    }

    public static void main(String[] args) {
        launch();
    }
}