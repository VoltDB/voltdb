package exportbenchmark;

public class DummyExporter extends ExportClientBase {
    
    @Override
    public boolean processRow(int rowSize, byte[] rowData) throws RestartBlockException {
        // We don't want to do anything yet
        return true;
    }
}