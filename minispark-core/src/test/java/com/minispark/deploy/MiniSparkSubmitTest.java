package com.minispark.deploy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The spark-submit-style argument parser. */
final class MiniSparkSubmitTest {

    @Test
    void parses_class_master_and_executor_options() {
        var s = MiniSparkSubmit.parse(new String[]{
                "--class", "com.example.App",
                "--master", "netty",
                "--num-executors", "2",
                "--executor-cores", "2",
                "--executor-memory", "512",
                "/data/book.txt", "extra"});
        assertThat(s.mainClass).isEqualTo("com.example.App");
        assertThat(s.conf).containsEntry("minispark.master", "netty")
                .containsEntry("minispark.executor.instances", "2")
                .containsEntry("minispark.executor.cores", "2")
                .containsEntry("minispark.executor.memoryMB", "512");
        // "/data/book.txt" is a bare positional (not *.jar), so it begins the
        // app args along with everything after it.
        assertThat(s.jars).isEmpty();
        assertThat(s.appArgs).containsExactly("/data/book.txt", "extra");
    }

    @Test
    void bare_positional_starts_app_args() {
        var s = MiniSparkSubmit.parse(new String[]{
                "--class", "com.example.App", "--master", "local[4]",
                "input.txt", "100"});
        assertThat(s.appArgs).containsExactly("input.txt", "100");
        assertThat(s.jars).isEmpty();
    }

    @Test
    void jar_is_classpath_and_following_tokens_are_app_args() {
        var s = MiniSparkSubmit.parse(new String[]{
                "--class", "com.example.App", "app.jar", "arg1", "arg2"});
        assertThat(s.jars).containsExactly("app.jar");
        assertThat(s.appArgs).containsExactly("arg1", "arg2");
    }

    @Test
    void repeated_conf_flags_accumulate() {
        var s = MiniSparkSubmit.parse(new String[]{
                "--class", "X",
                "--conf", "minispark.shuffle.manager=sort",
                "--conf", "minispark.ui.enabled=true"});
        assertThat(s.conf).containsEntry("minispark.shuffle.manager", "sort")
                .containsEntry("minispark.ui.enabled", "true");
    }

    @Test
    void conf_without_equals_is_rejected() {
        assertThatThrownBy(() -> MiniSparkSubmit.parse(new String[]{
                "--class", "X", "--conf", "bogus"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("key=value");
    }

    @Test
    void unknown_option_is_rejected() {
        assertThatThrownBy(() -> MiniSparkSubmit.parse(new String[]{
                "--class", "X", "--frobnicate", "y"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown option");
    }
}
