package se.scalablesolutions.akka.spring.foo;

/**
 * Created by IntelliJ IDEA.
 * User: michaelkober
 * Date: Aug 11, 2010
 * Time: 12:01:00 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IMyPojo {
  public void oneWay(String message);

  public String getFoo();

  public String longRunning();



}
