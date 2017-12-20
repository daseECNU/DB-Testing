
import org.voltdb.*;

public class rpstmt4 extends VoltProcedure 
{
	public final SQLStmt select_between = new SQLStmt(
			"select * from item where i_type = ? and i_price between ? and ? limit ?;");
	public VoltTable[] run(int itemkey, double min, double max, int limitnum) throws VoltAbortException 
	{
		voltQueueSQL( select_between, itemkey, min, max, limitnum);
		return voltExecuteSQL();
	}
}