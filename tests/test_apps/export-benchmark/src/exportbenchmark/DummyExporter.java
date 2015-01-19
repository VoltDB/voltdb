package exportbenchmark;

import org.voltdb.export.AdvertisedDataSource;

public class DummyExporter extends ExportClientBase {

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new PrinterExportDecoder(source);
    }

    @Override
    public boolean processRow(int rowSize, byte[] rowData) throws RestartBlockException {
        // We don't want to do anything yet
        return true;
    }
}