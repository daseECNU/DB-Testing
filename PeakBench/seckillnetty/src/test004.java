

import org.voltdb.*;

public class test004 extends VoltProcedure 
{
	public final SQLStmt select_primary = new SQLStmt(
			"select * from t001 where k = ?;");
	public final SQLStmt select_2 = new SQLStmt(
			"select * from t001 where c2 = ?;");
	public VoltTable[] run(int itemkey, int itemkey2) throws VoltAbortException 
	{
		voltQueueSQL( select_primary, itemkey);
		voltExecuteSQL();
		voltQueueSQL( select_2, itemkey2);
		return voltExecuteSQL();
	}
}
