package assess;

/**
 * Convert HashMap to JSONObject 
 */
import java.io.IOException;
import java.util.HashMap;

import org.json.simple.JSONObject;

public class JSOnExample {
	public static void main(String[] args) throws IOException {

		HashMap<Object, Object> obj = new HashMap<Object, Object>();
		obj.put("name", "foo");
		obj.put("num", new Integer(100));
		obj.put("balance", new Double(1000.21));
		obj.put("is_vip", new Boolean(true));
		obj.put("nickname", null);

		JSONObject jsonObj = new JSONObject(obj);
		
		System.out.println(jsonObj.toString());
	}
}
