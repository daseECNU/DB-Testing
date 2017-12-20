package util;

public class SecKillPlan implements Comparable<SecKillPlan>{
	public int skpkey;
	public int itemkey;
	public int plancount;
	public int popularity;

	public SecKillPlan()
	{
		super();
	}
	public SecKillPlan(int skpkey, int itemkey, int plancount,
			int popularity) {
		super();
		this.skpkey = skpkey;
		this.itemkey = itemkey;
		this.plancount = plancount;
		this.popularity = popularity;
	}

	public int compareTo(SecKillPlan s) {
		if(this.popularity > s.popularity) {
			return -1;
		} else if(this.popularity < s.popularity) {
			return 1;
		} else {
			return 0;
		}
	}
}