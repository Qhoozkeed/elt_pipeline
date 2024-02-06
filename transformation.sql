
------transformation for agg_public_holiday-----------------------------------------------
SELECT
    current_date ingestion_date,
    SUM(CASE WHEN b.month_of_the_year_num = 1 THEN 1 ELSE 0 END) AS tt_order_hol_jan,
    SUM(CASE WHEN b.month_of_the_year_num = 2 THEN 1 ELSE 0 END) AS tt_order_hol_feb,
    SUM(CASE WHEN b.month_of_the_year_num = 3 THEN 1 ELSE 0 END) AS tt_order_hol_mar,
    SUM(CASE WHEN b.month_of_the_year_num = 4 THEN 1 ELSE 0 END) AS tt_order_hol_apr,
    SUM(CASE WHEN b.month_of_the_year_num = 5 THEN 1 ELSE 0 END) AS tt_order_hol_may,
    SUM(CASE WHEN b.month_of_the_year_num = 6 THEN 1 ELSE 0 END) AS tt_order_hol_jun,
    SUM(CASE WHEN b.month_of_the_year_num = 7 THEN 1 ELSE 0 END) AS tt_order_hol_jul,
    SUM(CASE WHEN b.month_of_the_year_num = 8 THEN 1 ELSE 0 END) AS tt_order_hol_aug,
    SUM(CASE WHEN b.month_of_the_year_num = 9 THEN 1 ELSE 0 END) AS tt_order_hol_sep,
    SUM(CASE WHEN b.month_of_the_year_num = 10 THEN 1 ELSE 0 END) AS tt_order_hol_oct,
    SUM(CASE WHEN b.month_of_the_year_num = 11 THEN 1 ELSE 0 END) AS tt_order_hol_nov,
    SUM(CASE WHEN b.month_of_the_year_num = 12 THEN 1 ELSE 0 END) AS tt_order_hol_dec
FROM
   qudualli9842_staging.orders a
JOIN
    if_common.dim_dates b ON a.order_date = b.calendar_dt
where
	b.year_num =EXTRACT(YEAR FROM NOW()) - 2 
    and b.day_of_the_week_num BETWEEN 1 AND 5
    AND b.working_day = false
    

------------transformation for agg_shipments---------------------------------------------------------------------------------------------------------
select current_date ingestion_date,
    COUNT(CASE WHEN b.delivery_date IS NULL AND b.shipment_date >= a.order_date + INTERVAL '6 days' THEN 1 ELSE  NULL END) AS tt_late_shipments,
    COUNT(CASE WHEN b.delivery_date IS NULL AND b.shipment_date IS NULL AND '2022-09-05' >= a.order_date + INTERVAL '15 days' 
    THEN 1 else  NULL END) AS tt_undelivered_shipments
FROM qudualli9842_staging.shipments_deliveries b
JOIN qudualli9842_staging.orders a ON b.order_id = a.order_id;


------------transformation for best_performing_product-------------------------------------------------------------------------------------------------\
select current_date ingestion_date,rev.product_name, rev.order_date most_ordered_day, rev.is_public_holiday, rev.reviews tt_review_points, perce.percent_1_star pct_one_star_review, perce.percent_2_star pct_two_star_review, 
perce.percent_3_star pct_three_star_review, perce.percent_4_star pct_four_star_review, perce.percent_5_star pct_five_star_review,
rev.percent_early_shipment pct_early_shipment, rev.percent_late_shipment pct_late_shipment

from 
(select	
p.product_id, 
        p.product_name, 
		mod.order_date,
        CASE WHEN d.day_of_the_week_num BETWEEN 1 AND 5 AND d.working_day = false THEN 'True' ELSE 'False' END AS is_public_holiday,
        hr.reviews,
        COUNT(CASE WHEN d.day_of_the_week_num BETWEEN 1 AND 5 THEN 1 END) * 100.0 / COUNT(o.order_id) AS percent_early_shipment,
        COUNT(CASE WHEN d.day_of_the_week_num BETWEEN 6 AND 7 THEN 1 END) * 100.0 / COUNT(o.order_id) AS percent_late_shipment
FROM
        (select product_id,sum(review)reviews
from qudualli9842_staging.reviews group by product_id order by 2 desc limit 1) hr
JOIN
        if_common.dim_products p ON hr.product_id = p.product_id
        
        JOIN
       (select product_id,order_date, count(*) most_ordered from qudualli9842_staging.orders
group by  product_id, order_date order by 3 asc) mod ON hr.product_id =mod.product_id::int
JOIN
        if_common.dim_dates d ON mod.order_date = d.calendar_dt
    JOIN
        qudualli9842_staging.orders o ON hr.product_id = o.product_id::int
   group by p.product_id,p.product_name,mod.order_date,d.day_of_the_week_num ,d.working_day,
   hr.reviews,mod.most_ordered,mod.product_id order by mod.most_ordered desc limit 1) rev

join 

(select r.product_id,
		COUNT(CASE WHEN r.review = 1 THEN 1 END) * 100.0 / COUNT(r.review) AS percent_1_star,
        COUNT(CASE WHEN r.review = 2 THEN 1 END) * 100.0 / COUNT(r.review) AS percent_2_star,
        COUNT(CASE WHEN r.review = 3 THEN 1 END) * 100.0 / COUNT(r.review) AS percent_3_star,
        COUNT(CASE WHEN r.review = 4 THEN 1 END) * 100.0 / COUNT(r.review) AS percent_4_star,
        COUNT(CASE when r.review = 5 THEN 1 END) * 100.0 / COUNT(r.review) AS percent_5_star
 from qudualli9842_staging.reviews r join  qudualli9842_staging.orders o
 on r.product_id = o.order_id
 group by r.product_id) perce on rev.product_id = perce.product_id


----------------------the 3 aggregated tables bases on the tranformation---------------------------
select * from qudualli9842_analytics.agg_public_holiday
select * from qudualli9842_analytics.agg_shipments
select * from qudualli9842_analytics.best_performing_product
