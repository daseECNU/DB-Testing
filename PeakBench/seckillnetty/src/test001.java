

import org.voltdb.*;

public class test001 extends VoltProcedure 
{
	public final SQLStmt updateT3 = new SQLStmt(
			"update t3 set c1 = c1 + 1 where t3_key = ?;");	
	public final SQLStmt insertT1 = new SQLStmt(
			"insert into t1 values (?, ?);");
	public final SQLStmt insertT2 = new SQLStmt(
			"insert into t2 values (?, ?, ?);");
	public VoltTable[] run(int t1_key, int t2_key, int c1, int c2, int t3_key) 
			throws VoltAbortException {
		voltQueueSQL( updateT3, t3_key);
		VoltTable[] row = voltExecuteSQL();	
		if(row[0].fetchRow(0).getLong(0)>0)
		{
			voltQueueSQL( insertT1, t1_key, c1);
			voltExecuteSQL();
			voltQueueSQL( insertT2, t1_key, t2_key, c2);
			return voltExecuteSQL();	
		}
		else
		{
			return row;	
		}
	}
}
