import java.util.Random;


public class DDLGenerator
{
	String[] datatype = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT",
			"DECIMAL", "VARCHAR", "TIMESTAMP"};

	int numOfCols;
	int idxStep;

	Random r = new Random();

	public DDLGenerator()
	{
	    numOfCols = 50;
	    idxStep = 10;
	}

	public DDLGenerator(int col, double idxPercent)
	{
	    numOfCols = col;
	    idxStep = (int) (1 / idxPercent);
	}

	public String CreateColumn(int colNo)
	{
		int index = r.nextInt(datatype.length);
		if(datatype[index].equals("VARCHAR"))
		{
			int varcharSize = Math.abs(r.nextInt() % 1024) + 1;
			return "C" + colNo + " " + datatype[index] + "(" + varcharSize + ")";
		}
		else
		{
			return "C" + colNo + " " + datatype[index];
		}
	}

	public String CreateTable(int tableNo, String prefix)
	{
	    // number of cols range is [10, numOfCols + 9]
		int cols = Math.abs(r.nextInt() % numOfCols) + 10;
		StringBuffer sb = new StringBuffer();
		sb.append("CREATE TABLE " + prefix + tableNo + " (");

		for(int i = 0; i < cols; i++)
		{
			sb.append(CreateColumn(i) + ", ");
		}

		for(int i = 0; i < cols; i += idxStep)
		{
			sb.append("UNIQUE(C" + i + "), ");
		}
		sb.append("PRIMARY KEY (C0));");

		return sb.toString();
	}
}
