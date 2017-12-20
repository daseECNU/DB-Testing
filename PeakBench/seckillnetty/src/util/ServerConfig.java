package util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class ServerConfig
{
	private static Map<String, String> propertiesMap;
	private String fileName = null ;
	/**
	 * 加载配置文件
	 * 
	 * @return
	 */
	public ServerConfig(String fileName)
	{
		super();
		this.fileName = fileName;
	}
	public void loadProperties()
	{
		propertiesMap = new HashMap<String, String>();
		String pocHome = System.getProperty("user.dir");
		//"/readServer.properties"
		String realPath = pocHome + fileName;
		System.out.println(realPath);
		try
		{
			
			// 加载server的配置文件
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(realPath)));
			String str = null;
			String[] splitStr = null;
			while ((str = br.readLine()) != null)
			{
				// 过滤注释
				if (str.startsWith("#"))
					continue;
				// 过滤非法属性值对
				if (!str.contains("="))
					continue;
				splitStr = str.split("=", 2);
				propertiesMap.put(splitStr[0].trim(), splitStr[1].trim());
			}
			br.close();
		}
		catch (Exception e)
		{
		}
		finally
		{
		}
	}

	public String getString(String key)
	{
		return propertiesMap.get(key);
	}

	public int getInt(String key, int defaultValue)
	{
		int result = defaultValue;
		try{
			result = Integer.parseInt(propertiesMap.get(key));
		}
		catch (Exception e){
		}
		return result;
	}
	public float getFloat(String key, float defaultValue)
	{
		float result = defaultValue;
		try{
			result = Float.parseFloat(propertiesMap.get(key));
		}
		catch (Exception e){
		}
		return result;
	}

}
