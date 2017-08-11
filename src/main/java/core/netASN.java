package core;

public class netASN {

	public String as_path;
	public String as_dst;
	public String as_src;

	public netASN(String as_path, String as_dst, String as_src)
	{
		this.as_path = as_path;
		this.as_dst = as_dst;
		this.as_src = as_src;

	}

	public String getAs_path() {return as_path;}
	public String getAs_dst() {return as_dst;}
	public String getAs_src() {return as_src;}

}
