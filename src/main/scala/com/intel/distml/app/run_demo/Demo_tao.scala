package com.intel.distml.app.run_demo


import com.intel.distml.model.demo_tao.Demo

/**
 * Created by lq on 6/22/15.
 */
object Demo_tao {
  def main(args: Array[String]) {
    val  demo: Demo = new Demo()
    demo.run()
  }

}
