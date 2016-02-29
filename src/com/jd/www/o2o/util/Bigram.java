/**    
* @Title: Bigram.java  
* @Package www.jd.com.o2o.util  
* @Description: TODO(用一句话描述该文件做什么)  
* @author qiuxiangu@gmail.com    
* @date 2016年2月27日 下午8:01:27  
* @version V1.0    
*/

package com.jd.www.o2o.util;
import java.util.ArrayList;
import java.util.List;

/**  
* @ClassName: Bigram  
* @Description: TODO(这里用一句话描述这个类的作用)  
* @author qiuxiangu@jd.com 
* @date 2016年2月27日 下午8:01:27  
*    
*/

public class Bigram {

	/**
	* <p>Title: </p>  
	* <p>Description: </p>    
	*/
	private int n;
	public Bigram(int n) {
		// TODO Auto-generated constructor stub
		this.n = n;
	}

	public List<String> splits(String str) {
		List<String> ram = new ArrayList<String>();
		char[] chars = str.toCharArray();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < chars.length; i++) {
			sb.append(chars[i]);
			if (sb.length() == this.n) {
				ram.add(sb.toString());
				sb.deleteCharAt(0);
			}
		}
		return ram;
	}
	/**  
	* @Title: main  
	* @Description: TODO(这里用一句话描述这个方法的作用)  
	* @param @param args    设定文件  
	* @return void    返回类型  
	* @throws  
	*/

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String str = "浙江温州皮革厂倒闭了";
		Bigram b = new Bigram(3);
		List<String> l = b.splits(str);
		for (int i = 0; i<l.size(); i++){
			System.out.println(l.get(i));
		}
	}
}
