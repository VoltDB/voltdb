package kafkaimporter.client.kafkaimporter;

import org.voltdb.client.Client;

public class FinalCheck {
	static boolean check(Client client) {
		long mirrorRows = MatchChecks.getMirrorTableRowCount(client);
		long importRows = MatchChecks.getImportTableRowCount(client);

		System.out.println("Rows remaining in the Mirror Table: " + mirrorRows);
		System.out.println("Rows remaining in the Import Table: " + importRows);
		if (importRows != 0 || mirrorRows != 0) {
			return false;
		}
		return true;
	}
}
