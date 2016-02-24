## Introduction
The goal of visitante is to calculate various web analytic metric as defined by 
Avinash Kaushik (http://www.kaushik.net/avinash/) on the Hadoop and Storm platform.
However, it has evolved into a general purpose log analytic and mining solution, beyond 
web server logs. 

It also includes customer or marketing  analytic solution. Since customer behavior data 
is mostly captured in logs, there is a close relationship between customer analytics and 
log analytics.

## Philosophy
* Simple and easy to use batch and real time web analytic
* Highly configurable

## Blogs
The following blogs of mine are good source of details of visitante

* http://pkghosh.wordpress.com/2012/06/05/big-web-analytic/
* http://pkghosh.wordpress.com/2012/08/10/big-web-checkout-abandonment/
* https://pkghosh.wordpress.com/2014/10/05/tracking-web-site-bounce-rate-in-real-time/
* https://pkghosh.wordpress.com/2016/01/19/detecting-incidents-with-context-from-log-data/
* https://pkghosh.wordpress.com/2016/02/23/customer-lifetime-value-present-and-future/

## Solutions
* Hadoop based batch analytic for 
    * Num of pages visited
    * Total time spent 
    * Last page visited
    * Flow status (e.g., whether checkout flow was entered, entered but not completed or completed)
    * Incident detection
	* Pattern based event detection with context
    * Customer life time value

* Storm based real time analytic for
    * Bounce rate
    * Visit depth distribution




