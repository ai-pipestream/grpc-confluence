package ai.pipestream.okf;

import java.util.Locale;
import java.util.Objects;

/**
 * Actor strings for {@code generated.by} / {@code verified[].by} (§7).
 */
public final class OkfActor {

    private OkfActor() {
    }

    /**
     * An automated process actor ({@code process:<name>}).
     *
     * @param name process name, for example {@code grpc-microsoft/okf-producer}
     * @return the actor string
     */
    public static String process(String name) {
        return "process:" + Objects.requireNonNull(name, "name").trim();
    }

    /**
     * A human actor ({@code human:<id>}).
     *
     * @param id a person identifier
     * @return the actor string
     */
    public static String human(String id) {
        return "human:" + Objects.requireNonNull(id, "id").trim();
    }

    /**
     * An agent/tool actor ({@code <tool>/<model>}).
     *
     * @param tool tool name
     * @param model model or version name
     * @return the actor string
     */
    public static String agent(String tool, String model) {
        return Objects.requireNonNull(tool, "tool").trim() + "/"
                + Objects.requireNonNull(model, "model").trim();
    }

    /**
     * Whether {@code actor} is a {@code human:} actor (trust tier key, §5.3).
     *
     * @param actor an actor string
     * @return true when the human prefix is present
     */
    public static boolean humanActor(String actor) {
        return actor != null && actor.toLowerCase(Locale.ROOT).startsWith("human:");
    }
}
