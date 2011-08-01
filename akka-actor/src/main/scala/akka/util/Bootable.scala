/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.util

trait Bootable {
  def onLoad() {}
  def onUnload() {}
}
