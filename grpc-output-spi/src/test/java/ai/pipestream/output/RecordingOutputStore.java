package ai.pipestream.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class RecordingOutputStore implements OutputStore {

    private final String id;
    private final boolean available;
    final List<OutputObject> objects = new ArrayList<>();
    boolean opened;

    RecordingOutputStore(String id, boolean available) {
        this.id = id;
        this.available = available;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public boolean available(Map<String, String> env) {
        return available;
    }

    @Override
    public void open(Map<String, String> env) {
        opened = true;
    }

    @Override
    public void put(OutputObject object) {
        objects.add(object);
    }
}
