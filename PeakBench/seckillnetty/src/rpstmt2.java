
import org.voltdb.*;

public class rpstmt2 extends VoltProcedure 
{
	public final SQLStmt select_type = new SQLStmt(
			"select * from item where i_type = ? limit ?;");
	public VoltTable[] run(int typekey, int limtnum) throws VoltAbortException 
	{
		voltQueueSQL( select_type, typekey, limtnum);
		return voltExecuteSQL();
	}
}
