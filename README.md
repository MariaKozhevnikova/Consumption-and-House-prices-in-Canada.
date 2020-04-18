# Consumption-and-House-prices-in-Canada.
Do House Prices Affect Consumption?


Term Project presentation
Maria Kozhevnikova
SCS 3252 Big Data Management Systems & Tools

Home prices affect consumption in two ways. 
First, if the value of a house increases, this may encourage homeowners to increase consumption because they believe that their wealth has increased. 
Second, homeowners may be more willing to borrow because the house can be used as collateral, reducing credit constraints. In the context of economic cycles, the relationship between housing prices and household spending has policy implications for a country’s financial stability and monetary policy.
 In particular, a large fall in housing prices could explain some of the observed decline in consumption during recessions.



Road map

•	Data Selection and preparation
-	Data source 
-	Data quality and Challenges

•	Data Visualization and Analysis
•	Correlation
•	Linear Regression
•	Predictions
•	Conclusion


Datasets used for Analysis

•	New housing price indexes  
             This table contains 204 series, with data for years 1981 – 2010
             18100073.csv
•	Detailed household final consumption expenditure, provincial and territorial, annual

Annual final consumption expenditure for the household sector, in current and constant 2012 prices, by province and territory for years 1981 - 2017

36100225.csv

New housing price indexes

Statistic	Row data	Transformed data	
Total rows	57978	266	
Total columns	16	3	

Quality – good
 

Dropped Rows – Dropped all housing price indexes except ‘House only’.
Dropped Columns - 13 out of 16 columns. Left: REF_DATE, GEO, VALUE
 

Detailed household final consumption expenditure, provincial and territorial, annual
Statistic	Row data	Transformed data
Total rows	136947	1375
Total columns	16	4

              Quality - good
 

            Dropped Rows – in this dataset column “Estimates” has 96 items.
            Dropped all Estimates except:
-	Household final consumption expenditure
-	Clothing and footwear
-	Education
-	Recreation and culture
-	Food
            Dropped Columns - 13 out of 16 columns. Left: Year, Location, Estimates, Household 

consumption.  
 
After 2 different datasets were transformed into the same structure, I merged them into one.
 
Visualisation

Overall trend in Canada.
Household consumption and house pricing in Canada  1986 - 2010

 	 

Household consumption by estimates in Canada: 

5 largest spending for:
1)	Housing, water, electricity
2)	Transport
3)	Rental fee for housing
4)	Food and beverage
5)	Recreation and culture

Housing price by Location in Canada

Most expensive – Alberta
Least expensive – Prince Edvard Island
 

Percent of growth in Estimates categories

Categories	1986	2010	Percent
Price	956	1842	192%
Education	2113	13169	623%
Recreation and culture	23613	83951	355%
Food	30306	76228	251%
Clothing and footwear	19142	38478	201%

Correlation

Correlation for Canada – Price and Household consumption
Correlation to price for Year 0.85868432131384
Correlation to price for Price 1.0 
Correlation to price for Household_consumption 0.9192634223833661

Perform descriptive analytics
 
Summarize the model over the training set and print out some metrics:
  

Predictions:

In this linear regression example, the label is the house sales price and the feature is the Household consumption. That is, using the feature (house prices) to predict the label (household consumption).
 
 
Root Mean Squared Error (RMSE):  66238.374405 
r2: 0.829512
R squared at 0.82 indicates that in our model, approximate 82% of the variability in “Price” can be explained using the model.
Our model was able to predict the value of Household spending in the test set within $66238 of the real price.

