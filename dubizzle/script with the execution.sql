-- Excersise 1(one)
-- Credentials for the created Amazon Redshift cluster

-- Driver={Amazon Redshift (x64)}
-- ; Server=testcluster.ckahbbybyc9v.eu-west-1.redshift.amazonaws.com; Database=dev
-- ; UID=awsuser; PWD=XXXXX; Port=5439

-- Excersise 1(one)
-- Create table [actions] for data load from s3

-- encoding is used to make encoded columns occupy less space, which speeds up the query operations with them
-- type of encoding has been taken from the Analyze Compression query results

CREATE  TABLE   actions    (
                                userid  varchar(48) sortkey encode zstd,
                                action_type char(1),
                                action_ts date,
                                item_id varchar(48) encode zstd,
                                device varchar(2),
                                b2c boolean
                           )
diststyle key
distkey (action_ts)

-- Excersise 1(one)
-- Load data to the table [actions] from s3


-- Date format has been changed for the 'YYYY-MM-DD' as distkey of the table [actions] to be a date, not a timestamp,
-- since timestamp is always almost a unique value - date can allocate a distribution of a set of rows, which will
-- optimize the operations with the time precision
-- Credentials might have changed

       copy    actions
       from    's3://tradus-bi-recruiting/actions' #s3 bucket
credentials    'aws_access_key_id=XXXX;aws_secret_access_key=XXXX' #AWS S3 bucjet access credentials (available in assignment_task.pdf)
 dateformat    'auto'                              #fixing the dateformat
       gzip

-- Excersise 2(two)
-- A table with the designated segmentation for each user as of 1 July 2018

-- The logic here is to measure the first activity of a user, as well as his/her last activity
-- Secondly, the number of weeks passed from a snapshot time (2018-07-01) till the first/last activity date
-- needs to be calculated
-- Thirdly, to segment a user, comparison in between the numbers of weeks since the first/last activity is made,
-- to entirely describe the whole group of the users
-- Trial, Novice are quite clear to logicly describe, while Repeat, Lost and Dormant have some common factors for
-- last_activity indicator, which needs to be separated in the conditions of CASE WHEN construction

CREATE    TABLE    designated_user_segmentation as
SELECT
          userid,
          CASE
                   when first_act_week<=1 then 'trial'
                   when first_act_week<=8 then 'novice'
                   when first_act_week>8 and last_act_week<=4 then 'repeat'
                   when first_act_week>8 and last_act_week<12 then 'dormant'
                   when first_act_week>8 and last_act_week>=12 then 'lost'
           END
          as customer_base
  FROM
          (
               SELECT
                         userid,
                         round(datediff(day,min(action_ts),'2018-07-01')/7,2) as first_act_week,
                         round(datediff(day,max(action_ts),'2018-07-01')/7,2) as last_act_week
                 FROM    actions
                WHERE    action_ts<=date('2018-07-01')
                GROUP    BY 1
          ) as useractivities

-- Excersise 2(two)
-- Distribution of the segments

SELECT
          CASE
                  when first_act_week<=1 then 'trial'
                  when first_act_week<=8 then 'novice'
                  when first_act_week>8 and last_act_week<=4 then 'repeat'
                  when first_act_week>8 and last_act_week<12 then 'dormant'
                  when first_act_week>8 and last_act_week>=12 then 'lost'
           END
          as customer_base,
          count(userid) as num_of_users,
          round(ratio_to_report(num_of_users) over (),3) dist
  FROM
          (
               SELECT
                         userid,
                         round(datediff(day,min(action_ts),'2018-07-01')/7,2) first_act_week,
                         round(datediff(day,max(action_ts),'2018-07-01')/7,2) last_act_week
                 FROM    actions
                WHERE    action_ts<=date('2018-07-01')
                GROUP    BY 1
          ) as useractivities
 GROUP  BY 1

-- Excersise 3(three)
-- Create redesigned table [actions] as [actions_]
-- Date is needed as a timestamp now, since the day in the excersise is estimated on an hourly precision
-- Encoding has been done as before: Analyze Compression query results
-- Rows are distributed within the node, based on action type, since this is the decision factor in the further
-- data filtering for window function

CREATE    TABLE    actions_    (
                                userid  varchar(48),
                                action_type char(1) encode zstd,
                                action_ts timestamp encode zstd,
                                item_id varchar(48) sortkey encode zstd,
                                device varchar(2),
                                b2c boolean
                               )
diststyle key
distkey (action_type)

-- Excersise 3(three)
-- Load data to [actions_] from s3
-- There is no alteration of the date format

copy actions from 's3://tradus-bi-recruiting/actions'
                        credentials 'aws_access_key_id=XXXX;aws_secret_access_key=XXXX' #AWS S3 bucjet access credentials (available in assignment_task.pdf)
                        gzip

-- Excersise 3(three)
-- CREATE TABLE fact_item_liquidity and populate it with query
-- Distkey for the table has been changed to date as this is the preferred way to keep the data distributed in the node:
-- the calculation happens within a date, so rows need to be distributed per date;
-- then the sortkey is put for replies_received_within_7_days as 2 of 3 further calculation will happen, based on this
-- column
-- The rule is: table nodes have to be 'sorted, indexed' by distkey; and the data within the node is put in accordance
-- to the sortkey

-- Since the data is just a piece of the full snapshot, some rows in it do not have a posting date. To correct it,
-- I have put window function to get the first action type value within the dataset for all the rows
-- for a particular item
-- Then sorted out all the rows, which do not have a posting row in the dataset
-- Thus, further analysis happened on an entire data slice

CREATE  TABLE    fact_item_liquidity    (
                             	             date date encode zstd,
                             	             item_id varchar(48) encode zstd,
                             	             replies_received_within_1_day bigint sortkey,
                                             replies_received_within_7_days bigint sortkey
                             	        )
diststyle key
distkey (date)


INSERT    INTO fact_item_liquidity
SELECT
          MIN(trunc(first_action_ts)) date,
          item_id,
          SUM(CASE WHEN action_type='R' AND datediff(day,first_action_ts,action_ts)<=1 then 1 else 0 END) replies_received_within_1_day,
          SUM(CASE WHEN action_type='R' AND datediff(day,first_action_ts,action_ts)<=7 then 1 else 0 END) replies_received_within_7_days
  FROM
          (
           SELECT
                     item_id,
                     first_value(action_type) over (partition by item_id order by action_ts asc rows between unbounded preceding and unbounded following) first_action_type,
                     min(action_ts) over (partition by item_id) first_action_ts,
                     action_ts,
                     action_type
             FROM    actions_
          )
 WHERE    first_action_type='P'
 GROUP    BY 2


-- Excersise 3(three)
-- Create table fact_liquidity

CREATE    TABLE    fact_liquidity    as
SELECT
          date,
          items_posted_on_date,
          liquid_items_1_reply_within_1_day,
          liquid_items_3_reply_within_7_day,
          liquid_items_5_reply_within_7_day,
          100*ROUND(liquid_items_1_reply_within_1_day/items_posted_on_date,2) "%liquid_items_1_reply_within_1_day",
          100*ROUND(liquid_items_3_reply_within_7_day/items_posted_on_date,2) "%liquid_items_3_reply_within_7_day",
          100*ROUND(liquid_items_5_reply_within_7_day/items_posted_on_date,2) "%liquid_items_5_reply_within_7_day"
  FROM
          (
          SELECT
                    date,
                    CAST(count(item_id) as numeric) items_posted_on_date,
                    CAST(SUM(CASE WHEN replies_received_within_1_day>=1 then 1 else 0 end) as numeric) liquid_items_1_reply_within_1_day,
                    CAST(SUM(CASE WHEN replies_received_within_7_days>=3 then 1 else 0 end) as numeric) liquid_items_3_reply_within_7_day,
                    CAST(SUM(CASE WHEN replies_received_within_7_days>=5 then 1 else 0 end) as numeric) liquid_items_5_reply_within_7_day
            FROM    fact_item_liquidity
           GROUP    BY 1
          )
