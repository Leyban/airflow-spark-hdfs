# Monthly Commission

## Description
Calculates commission for affiliates

## Table of Contents
- [Schedule Interval](#schedule_interval)
- [Overview](#overview)
- [DAG Breakdown](#dag_breakdown)
    - [Daily Data Gathering](#daily_gathering)
        - [Daily Data Gathering Breakdown](#daily_gathering_breakdown)
    - [Scheduled Calculation](#scheduled_calculation)
        - [Scheduled Calculation Breakdown](#scheduled_calculation_breakdown)
- [Extending the DAG](#extending)

## Schedule Interval{#schedule_interval}
Daily at 16:00 UTC

## Overview{#overview}
- This task mainly has two parts. 
    - **daily_data_gathering:** Collects transaction and wager data every day and saves it into specific sqlite files
    - **scheduled_calculation:** Calculates the commission for affiliates if the payout frequency matches the current day of execution

## DAG Breakdown{#dag_breakdown}

### Daily Data Gathering{#daily_gathering}
This task group consist of two parts
- **get_transactions:** 
    - Collects deposit, withdrawal, and adjustment data from `paymentDB` using airflow connection id `payment_conn_id`
    - The collected data is then saved into an sqlite file which can be used by other tasks whenever transaction data is needed
- **get_wagers:** 
    - Collects wager data from `collectorDB` using airflow connection id `wager_conn_id`
    - The collected data is then saved into an sqlite inside a directory named after the product name
    _**note:**_ `digitain_user` is collected using the connection id `vendors_conn_id`

### Daily Data Gathering Breakdown{#daily_gathering_breakdown}
Data Gathering Has 3 major steps
- **Database Querying:** 
    - Queries the database for the data and convert it into a `pandas dataframe`
- **Aggregation:** 
    - The `dataframe` is grouped by each `member_name` and the amounts are summed together
    _**note**_: The wagers are counted at this point
- **Save to SQLite:** 
    - Finally the aggregated `dataframe` is saved to a `SQLite` file for later use

All of these steps are applied in `deposits`, `withdrawals`, `adjustments`, and in every `wager product`

###  Scheduled Calculation{#scheduled_calculation}
There are three instances; one for each of the possible payout frequency: `weekly`, `bi_monthly`, `monthly`
If the current day does not match the requirements then these tasks will not run and raise an `AirflowSkipException`

- **weekly:** 
    - This task group will run based on the execution date's day of the week: `Monday:0 - Sunday:6`
    - The day of the week can be overwritten with an airflow Variable: `COMMISSION_WEEKLY_DAY` where the default value is `0`

- **bi_monthly:** 
    - This task group will run on the `15th` day of the month and use the data from the `1st` to the `14th` day of the month
    - This will also run on the `1st` day of the month and use the data from the `15th` to the `last` day of the month

- **monthly:** 
    - This will only run on the `1st` day of the month and use the data from the previous month
    - This task group will also delete previous data from 3 months ago in the task `delete_old_files`


### Scheduled Calculation Breakdown{#scheduled_calculation_breakdown}
At every run, this will run a task to check if the current date is a valid date to run the tasks. `Eg: 1st day of the month`

The calculation of affiliate commissions happen at this point. Lets break it down into steps:
**Affiliates**
- Take all the affiliates from the postgres database that has the same payout frequency as the one in the current active task.
- Filter all the data on these affiliates

**Transactions**
- Get all the transactions from the sqlite files then aggregate it together. If the payout frequency is `weekly` for example, the task will read all sqlite files from 7 days ago. 
- Save the transactions to `cm_commission_fee` table in `affiliateDB` via the postgres conn id `affiliate_conn_id`
- Clean up the transactions dataframe

**Wagers**
- Get all the wagers from the sqlite files then aggregate it together.
- Save the wagers to `cm_member_wager_product` table
- Clean up the wagers

**Summary**
- At this point, we have all the necessary data for calculating the commission. The transaction and wager dataframes are joined together to make a complete summary then saved to `cm_commission_summary`
- Clean up the dataframe

**Currency Conversion**
- Currency is converted before computing
- If the currency is not any of `VND`, `THB`, and `RMB` the value will be `0` via the `usd_rate`

**Affiliate Adjustment**
- Affiliate adjustment (Different from payment adjustment) is taken from `affiliateDB`
- It is then converted to usd and aggregated before joined to the commission dataframe
_**note:**_ The affiliate adjustment is unique in a way that it doesn't depend on the date. If the affiliate adjustment is not claimed, it can be used in the next payout period

**Total Members**
- The total number of active and inactive members are queried from the postgres database. We do it at this point to limit the number of members that the query has to count.
_**note:**_ This is not used in calculation, same with the total wager bet amount. It's just for display. 

**Previous Settlement**
For each affiliate, if they have valid commissions `(active members >= min active members)` but the commission amount did not reach the minimum amount, the amount will be rolled over to next period.
This amount is the previous settlement which will be added after all the calculation is done
The minimum commission has the default value of `100` and can be overwritten with the airflow Variable `COMMISSION_MIN_NET`

**Net Company Win Loss**
The following formulas are used to compute the Net Company Win Loss for each affiliate
$ net\_company\_win\_loss = comapny\_win\_loss - fee\_total $
$ fee\_total = expenses + other\_fee + temp\_platform\_fee $
$ expenses = \sum\bigg(\big(deposit\_amount + withdrawal\_amount\big) * \big(\frac{payment\_type\_rate}{100}\big)\bigg) $
$ other\_fee = \sum\big(adjustment\big) $

**Grand Total**
Calculating fot the Grand Total can be split in two parts: Calculating the `Total Amount` and `Processing Rollovers`
$grand\_total = total\_amount + previous\_settlement$

**Total Amount**
There are 3 Tiers for every commission. 
The threshold value for `tier 1` and `tier 2` have default values of `10,000` and `100,000`. These values can be overwritten with the airflow Variables `COMMISSION_MAX_NET_TIER1` and `COMMISSION_MAX_NET_TIER2`

total amount will be calculated as:
$net\_company\_win\_loss * commission\_tier1$ 
for the first `10,000`
if it exceeds that value, `commission_tier2`  will be used until it reaches `100,000` after which `commission_tier3` will be used

**Processing Rollovers**
If the number of active members is less than the `min_active_player`, then the commission at this period is not valid and the status for the commission and payment will be `threshold`. The `previous_settlement` can still be claimed next payout period so we set the rollover_next_month value as `previous_claimed next payout period so we set the rollover_next_month value as `previous_claimed next payout period so we set the `rollover_next_month` value as `previous_settlement`

If the number of active members is more than or equal to the `min_active_player`, then the `previous_settlement` is added to the `grand_total`
However, if the `grand_total` is still less than the minimum commission amount then the `grand_total` cannot be claimed to we set it to `rollover_next_month` where the commission can be evaluated at the next payout period.

If the affiliate has enough active members and high enough commission, then the `grand_total` can be processed and the commission status and the payment status will be `processing`


### Extending the DAG{#extending}
When a new product is added to the system, this product has to be added to the DAG. This section covers the steps necessary for this situatuion.

**Constants**
The first step will be adding the constants to the DAG file. 
First add the name of the product code to the list of codes near the top of the file along the others. 
```
PRODUCT_CODE_SABAVIRTUAL = "sabavirtualsport"
PRODUCT_CODE_DIGITAIN = "digitain"
PRODUCT_CODE_WEWORLD = "weworld"
<--------- New Product Code Here
```
Then add it to the `PRODUCT_CODES` list

```
PRODUCT_CODES = [
    PRODUCT_CODE_ALLBET,
    ...
    PRODUCT_CODE_WEWORLD,
    <--------- New Product Code Here
]
```

**Data Fetching**
Define a function that will make a query to where it is saved and return a dataframe. 
Typically we only need the `stake`, `win_loss`, and `login_name` then `get_member_currency` will take the necessary data from `identityDB`
Take the following example:
```
def get_allbet_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id="wager_conn_id")

    rawsql = f"""
        SELECT
            valid_amount AS stake, 
            win_or_loss_amount AS win_loss,
            login_name
        FROM {ALL_BET_WAGER_TABLE}
        WHERE bet_time < '{date_to}'
        AND bet_time >= '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = get_member_currency(df)

    return df
```

**Add Task Group**
Add a new task group for daily data gathering and pass the product code and the data fetching function
```
wager_task_group(PRODUCT_CODE_SABAVIRTUAL, get_saba_virtual)()
wager_task_group(PRODUCT_CODE_WEWORLD, get_weworld_wager)()
wager_task_group(PRODUCT_CODE_DIGITAIN, get_digitain_wager)()
<----------- New Task Group Here

```

And that's it. 
Since the only thing different for every wager is the way we fetch the valid data, the DAG can have an identical process for every wager.
Removing a product would need the same 3 steps. Except instead of adding, we just have to remove it.
