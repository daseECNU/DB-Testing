

import org.voltdb.*;

public class rpstmt1 extends VoltProcedure 
{
	public final SQLStmt select_primary = new SQLStmt(
			"select * from item where i_itemkey = ?;");
	public VoltTable[] run(int itemkey) throws VoltAbortException 
	{
		voltQueueSQL( select_primary, itemkey);
		return voltExecuteSQL();
	}
}
