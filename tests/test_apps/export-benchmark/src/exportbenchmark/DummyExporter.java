package exportbenchmark;

import java.util.Properties;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.export.AdvertisedDataSource;

public class DummyExporter extends ExportClientBase {
    
    @Override
    public void configure(Properties config) throws Exception {
        // We have no properties to configure at this point
    }
    
    static class PrinterExportDecoder extends ExportDecoderBase {
        PrinterExportDecoder(AdvertisedDataSource source) {
            super(source);
        }
        
        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            // The AdvertiseDataSource is no longer available. If file descriptors
            // or threads were allocated to handle the source, free them here.
        }
        
        @Override
        public boolean processRow(int rowSize, byte[] rowData) throws RestartBlockException {
            // We don't want to do anything yet
            return true;
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new PrinterExportDecoder(source);
    }
}