##
Content

The executed test assignment consists of:

CoordStatusMA       - the initial file with data
ETexclL.py          - python script to parse CoordStatusMA (needs to be ryn via google colabs)
new_CoordstatusMA   - csv file, which is the resut of execution of ETexclL.py
testassignment.pbix - Power BI dashboards to visualise new_CoordstatusMA

##
ETL

Python script reads CSV file CoordStatusMA to make it a "table-like" format: new_CoordstatusMA
The output file - new_CoordstatusMA

In this way the file can be uploaded to a table of any DBMS (Mysql, Postgre, Big Query)
Additional parsing via SQL can be done, but for the means of the current test assignment, the visualisation tool can use csv file as a source
(it is only 10MB)
As well as it can use a connection to any of those DBMS, but storeing the data source in one subnet with the visualisation tool will reduce the latency

For the visualisation, to simplify the case, new_CoordstatusMA is used.

The python code can be run within Airflow, matillion, any other ETL Tool. For this case google colabs is offered.

##
Visual representation of data in new_CoordstatusMA consists of 5 pages:

1. Data validation                       - checks the content overall
2. Covid cases spread                    - Heatmap, working from the manual filters to be chosen; gives an allocation of cases per status in time
3. Covid cases spread - Infographics     - shows the infographics of spreading the covid infection over the time and map
4. Covid cases statistics                - Map, working from the manual filters to be chosen
5. Covid cases statistics - Infographics - shows the infographics of statistics over the time and map

Heatmap in a very good way to show the spread of the number of covid cases within the time (in general); metrics help to describe it better

Map shows the same, but gives numeric description (number of cases - as a running total) per a location(coordinates), status and date
Tooltip shows the essential info on the location: number of cases, status, date of calculation. Pies the circle by the amount of cases per status.

Filters for the visualisations have been derived by DAX formulas out of the [date] field: [years], [months], [weeks], [dates]

Some auxiliary metrics have been created to describe the infographics


In general, the created dashboard helps to see the tendency - the speed of areas to be infected after the central hub has been infected.
And dig dipper with the zoom to see the allocation of the cases per status within time and space.