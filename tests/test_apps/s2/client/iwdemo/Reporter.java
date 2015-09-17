package iwdemo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;

public class Reporter implements Runnable {

    private final Client m_client;

    Reporter(Client client) {
        m_client = client;
    }

    @Override
    public void run() {
        double rangeInKm = 10.0;
        Map<String, Long> taxisPerCityAllPtns = new HashMap<>();

        try {
            VoltTable keys = m_client.callProcedure("@GetPartitionKeys", "INTEGER")
                    .getResults()[0];
            while (keys.advanceRow()) {
                long key = keys.getLong(1);
                VoltTable taxisPerCity = m_client.callProcedure("TaxisPerCity", key, rangeInKm).getResults()[0];
                while (taxisPerCity.advanceRow()) {
                    String city = taxisPerCity.getString(0);
                    Long val = taxisPerCityAllPtns.get(city);
                    if (val == null) {
                        taxisPerCityAllPtns.put(city, taxisPerCity.getLong(1));
                    }
                    else {
                        val = val + taxisPerCity.getLong(1);
                        taxisPerCityAllPtns.put(city, val);
                    }
                }
            }

        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
            return;
        }

        System.out.println("Places with a taxi within " + rangeInKm + " km "
                + "(" + taxisPerCityAllPtns.size() + ")");
        for (String city : taxisPerCityAllPtns.keySet()) {
            System.out.println("  " + city + ": " + taxisPerCityAllPtns.get(city));
        }
    }
}
