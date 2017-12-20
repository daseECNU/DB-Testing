
import org.voltdb.*;

public class rpstmt3 extends VoltProcedure 
{
	public final SQLStmt select_like = new SQLStmt(
			"select * from item where i_type = ? and i_name like ? limit ?;");
	public VoltTable[] run(int itemkey, String name, int limitnum) throws VoltAbortException 
	{
		voltQueueSQL( select_like, itemkey, name, limitnum);
		return voltExecuteSQL();
	}
}
