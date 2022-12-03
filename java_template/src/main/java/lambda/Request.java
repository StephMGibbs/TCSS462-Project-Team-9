package lambda;

/**
 *
 * @author Wes Lloyd
	https://github.com/wlloyduw/SAAF/blob/master/java_template/src/main/java/lambda/Request.java
 */
public class Request {

    String name;

    public String getName() {
        return name;
    }
    
    public String getNameALLCAPS() {
        return name.toUpperCase();
    }

    public void setName(String name) {
        this.name = name;
    }

    public Request(String name) {
        this.name = name;
    }

    public Request() {

    }
}