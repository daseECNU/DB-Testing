
import org.voltdb.*;

public class rpstmt5 extends VoltProcedure 
{
	public final SQLStmt select_supplier = new SQLStmt(
			"select * from item where i_suppkey = ? limit ?;");
	public VoltTable[] run(int itemkey, int limitnum) throws VoltAbortException 
	{
		voltQueueSQL( select_supplier, itemkey, limitnum);
		return voltExecuteSQL();
	}
}