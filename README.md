# churn_analysis

Customer attrition othewise known as customer churn is the loss of customers, or clients. A churn analysis is a key insight into how effective customer retetion efforts are.


This post will focus on the technical details of performing a churn analysis using SQL (postgres 9.6). The follwing example will study a sample data set from an e-commerce business.  First some definitions we'll use throughout the example to characterise the customer's status.

* __new customer__ - a customer is new for the 1st month within
* __active customer__ - an active customer is someone who has made a purchase within 3 months
* __churned customer__ - a customer who has not made a purchase for more than 3 months
* __returned customer__ - a customer who makes a purchase after a churn event, a customer will be considered `returned` for the month that the purchase was made and will become an `active` customer the following month

![churn analysis diagram](https://github.com/hannaj06/churn_analysis/blob/master/churn_example.png)
