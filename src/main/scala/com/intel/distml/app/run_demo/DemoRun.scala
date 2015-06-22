package com.intel.distml.app.run_demo

import com.intel.distml.model.demo_echo.Demo

/**
 * Created by lq on 6/22/15.
 */
object DemoRun {
  def main(args: Array[String]) {
    val demo: Demo = new Demo()
    demo.run()
  }
}

