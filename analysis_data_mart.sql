-- BDE A3 part4 
-- Sibo Zhang SID: 25520735
-- views from part 3 stored with schema name public_gold
-- cleaned census tables stored with schema name public_silver

-- community profiles data(g01 and g02) are seperated by sex and other personal characteristics according to 
-- published data dictionary(see report); tables are shown accordingly by characteristics:

-- a
WITH 
source AS (
SELECT host_neighbourhood_lga,
	   SUM(avg_revenue_active_listings) AS annual_revenue_active_listings
FROM public_gold.dm_host_neighbourhood
GROUP BY host_neighbourhood_lga
),

census AS(
SELECT *,a.lga_code AS code FROM public_silver.nsw_census_01 a
JOIN public_silver.nsw_census_02 b ON a.lga_code=b.lga_code
),

combined AS(
SELECT * FROM census c
LEFT JOIN SOURCE s ON c.code = s.host_neighbourhood_lga
WHERE annual_revenue_active_listings IS NOT NULL
),

RESULT AS (
(SELECT *,'highest' AS performance_lga FROM combined
ORDER BY annual_revenue_active_listings DESC
LIMIT 3)
UNION ALL
(SELECT *,'lowest' AS performance_lga FROM combined
ORDER BY annual_revenue_active_listings ASC
LIMIT 3)
)

-- table for key median and averages such as income and age 
SELECT 
	performance_lga, 
	AVG(Median_age_persons)::INT AS median_age,
	AVG(Median_mortgage_repay_monthly)::INT AS median_mortgage, 
	AVG(Median_tot_prsnl_inc_weekly)::INT AS median_weekly_income, 
	AVG(Median_rent_weekly)::INT AS median_rent, 
	AVG(Median_tot_fam_inc_weekly)::INT AS median_family_income,
	AVG(Median_tot_hhd_inc_weekly)::INT AS median_household_income,
	AVG(Average_num_psns_per_bedroom)::INT AS avg_person_per_bedroom, 
	AVG(Average_household_size)::INT AS avg_household_size
FROM RESULT
GROUP BY performance_lga;

-- table for population by gender
SELECT 
	performance_lga,
	ROUND(AVG(Tot_P_M),1) AS total_person_male, 
	ROUND(AVG(Tot_P_F),1) AS total_person_female, 
	ROUND(AVG(Tot_P_P),1) AS total_person_person
FROM RESULT
GROUP BY performance_lga;

-- table for male age group 
SELECT 
	performance_lga,
	ROUND(AVG(Age_0_4_yr_M),1) AS age_0_4,
	ROUND(AVG(Age_5_14_yr_M),1) AS age_5_14, 
	ROUND(AVG(Age_15_19_yr_M),1) AS age_15_19, 
	ROUND(AVG(Age_20_24_yr_M),1) AS age_20_24, 
	ROUND(AVG(Age_25_34_yr_M),1) AS age_25_34, 
	ROUND(AVG(Age_35_44_yr_M),1) AS age_35_44, 
	ROUND(AVG(Age_45_54_yr_M),1) AS age_45_54, 
	ROUND(AVG(Age_55_64_yr_M),1) AS age_55_64, 
	ROUND(AVG(Age_65_74_yr_M),1) AS age_65_74, 
	ROUND(AVG(Age_75_84_yr_M),1) AS age_75_84, 
	ROUND(AVG(Age_85ov_M),1) AS age_85_and_above
FROM RESULT
GROUP BY performance_lga;

-- table for female age group 
SELECT 
	performance_lga,
	ROUND(AVG(Age_0_4_yr_F),0) AS age_0_4,
	ROUND(AVG(Age_5_14_yr_F),0) AS age_5_14, 
	ROUND(AVG(Age_15_19_yr_F),0) AS age_15_19, 
	ROUND(AVG(Age_20_24_yr_F),0) AS age_20_24, 
	ROUND(AVG(Age_25_34_yr_F),0) AS age_25_34, 
	ROUND(AVG(Age_35_44_yr_F),0) AS age_35_44, 
	ROUND(AVG(Age_45_54_yr_F),0) AS age_45_54, 
	ROUND(AVG(Age_55_64_yr_F),0) AS age_55_64, 
	ROUND(AVG(Age_65_74_yr_F),0) AS age_65_74, 
	ROUND(AVG(Age_75_84_yr_F),0) AS age_75_84, 
	ROUND(AVG(Age_85ov_F),0) AS age_85_and_above
FROM RESULT
GROUP BY performance_lga;

-- table for number of person at home by sex
SELECT 
	performance_lga,
	AVG(Counted_Census_Night_home_M)::INT AS male_at_home, 
	AVG(Count_Census_Nt_Ewhere_Aust_M)::INT AS male_elsewhere,
	AVG(Counted_Census_Night_home_F)::INT AS female_at_home,
	AVG(Count_Census_Nt_Ewhere_Aust_F)::INT AS female_elsewhere, 
	AVG(Counted_Census_Night_home_P)::INT AS person_at_home, 
	AVG(Count_Census_Nt_Ewhere_Aust_P)::INT AS person_elsewhere
FROM RESULT
GROUP BY performance_lga;

-- table for Indigenous people by sex
SELECT 
	performance_lga,
	AVG(Indigenous_psns_Aboriginal_M)::INT AS aboriginal_male,
	AVG(Indig_psns_Torres_Strait_Is_M)::INT AS torres_strait_male,
	AVG(Indig_Bth_Abor_Torres_St_Is_M)::INT AS both_male,
	AVG(Indigenous_P_Tot_M)::INT AS total_indigenous_male,
	AVG(Indigenous_psns_Aboriginal_F)::INT AS aboriginal_female,
	AVG(Indig_psns_Torres_Strait_Is_F)::INT AS torres_strait_female, 
	AVG(Indig_Bth_Abor_Torres_St_Is_F)::INT AS both_female,
	AVG(Indigenous_P_Tot_F)::INT AS total_indigenous_female
FROM RESULT
GROUP BY performance_lga;

-- table for birthplace by sex
SELECT 
	performance_lga,
	AVG(Birthplace_Australia_M)::INT AS born_in_au_male, 
	AVG(Birthplace_Elsewhere_M)::INT AS born_elsewhere_male,
	AVG(Birthplace_Australia_F)::INT AS born_in_au_female,
	AVG(Birthplace_Elsewhere_F)::INT AS born_elsewhere_female, 
	AVG(Birthplace_Australia_P)::INT AS born_in_au_total, 
	AVG(Birthplace_Elsewhere_P)::INT AS born_elsewhere_total
FROM RESULT
GROUP BY performance_lga;

-- table for language used by sex
SELECT 
	performance_lga,
	AVG(Lang_spoken_home_Eng_only_M)::INT AS lang_english_male, 
	AVG(Lang_spoken_home_Oth_Lang_M)::INT AS lang_other_male,
	AVG(Lang_spoken_home_Eng_only_F)::INT AS lang_english_female,
	AVG(Lang_spoken_home_Oth_Lang_F)::INT AS lang_other_female, 
	AVG(Lang_spoken_home_Eng_only_P)::INT AS lang_english_total,
	AVG(Lang_spoken_home_Oth_Lang_P)::INT AS lang_other_total
FROM RESULT
GROUP BY performance_lga;

-- table for number of citizens by sex
SELECT 
	performance_lga,
	AVG(Australian_citizen_M)::INT AS au_citizen_male, 
	AVG(Australian_citizen_F)::INT AS au_citizen_female,
	AVG(Australian_citizen_P)::INT AS au_citizen_total
FROM RESULT
GROUP BY performance_lga;

-- table for number of students by sex
SELECT 
	performance_lga, 
	AVG(Age_psns_att_educ_inst_0_4_M)::INT AS edu_male_0_4,
	AVG(Age_psns_att_educ_inst_5_14_M)::INT AS edu_male_5_14,
	AVG(Age_psns_att_edu_inst_15_19_M)::INT AS edu_male_15_19,
	AVG(Age_psns_att_edu_inst_20_24_M)::INT AS edu_male_20_24,
	AVG(Age_psns_att_edu_inst_25_ov_M)::INT AS edu_male_25_over,
	AVG(Age_psns_att_educ_inst_0_4_F)::INT AS edu_female_0_4, 
	AVG(Age_psns_att_educ_inst_5_14_F)::INT AS edu_female_5_14,
	AVG(Age_psns_att_edu_inst_15_19_F)::INT AS edu_female_15_19, 
	AVG(Age_psns_att_edu_inst_20_24_F)::INT AS edu_female_20_24,
	AVG(Age_psns_att_edu_inst_25_ov_F)::INT AS edu_female_25_over, 
	AVG(Age_psns_att_educ_inst_0_4_P)::INT AS edu_0_4,
	AVG(Age_psns_att_educ_inst_5_14_P)::INT AS edu_5_14, 
	AVG(Age_psns_att_edu_inst_15_19_P)::INT AS edu_15_19,
	AVG(Age_psns_att_edu_inst_20_24_P)::INT AS edu_20_24, 
	AVG(Age_psns_att_edu_inst_25_ov_P)::INT AS edu_25_over
FROM RESULT
GROUP BY performance_lga;

-- table for level of education in years
SELECT 
	performance_lga, 
	AVG(High_yr_schl_comp_Yr_12_eq_P)::INT AS year_of_school_12, 
	AVG(High_yr_schl_comp_Yr_11_eq_P)::INT AS year_of_school_11,
	AVG(High_yr_schl_comp_Yr_10_eq_P)::INT AS year_of_school_10, 
	AVG(High_yr_schl_comp_Yr_9_eq_P)::INT AS year_of_school_9,
	AVG(High_yr_schl_comp_Yr_8_belw_P)::INT AS year_of_school_8_below, 
	AVG(High_yr_schl_comp_D_n_g_sch_P)::INT AS no_school
FROM RESULT
GROUP BY performance_lga;

-- table for living status
SELECT 
	performance_lga, 
	AVG(Count_psns_occ_priv_dwgs_P)::INT AS person_in_private_dwellings, 
	AVG(Count_Persons_other_dwgs_P)::INT AS person_in_other_dwellings
FROM RESULT
GROUP BY performance_lga;

-- b

WITH 
source AS (
SELECT host_neighbourhood_lga,
	   SUM(avg_revenue_active_listings) AS annual_revenue_active_listings
FROM public_gold.dm_host_neighbourhood
GROUP BY host_neighbourhood_lga
)
-- correlation table(scatterplot are generated in python)
SELECT lga_code,median_age_persons,annual_revenue_active_listings FROM public_silver.nsw_census_02 c
LEFT JOIN SOURCE s ON c.lga_code = s.host_neighbourhood_lga
WHERE annual_revenue_active_listings IS NOT NULL

--c

WITH SOURCE AS (
SELECT 
a.listing_neighbourhood,
a.YEAR,
a.MONTH,
b.property_type,
b.room_type,
b.accommodates,
b.total_number_of_stays
FROM public_gold.dm_listing_neighbourhood a
LEFT JOIN public_gold.dm_property_type b
ON a.YEAR=b.YEAR AND a.MONTH=b.MONTH

),

rank_by_stays AS (
SELECT *, RANK() OVER (PARTITION BY listing_neighbourhood ORDER BY annual_number_of_stays DESC) AS RANK
FROM(
SELECT listing_neighbourhood,property_type,room_type,accommodates,SUM(total_number_of_stays) AS annual_number_of_stays 
FROM SOURCE GROUP BY listing_neighbourhood,property_type,room_type,accommodates
))

SELECT listing_neighbourhood,property_type,room_type,accommodates FROM rank_by_stays
WHERE RANK=1;

-- d

WITH rich_hosts AS(
SELECT host_id FROM
(SELECT host_id,COUNT(listing_id) AS num_of_listings FROM public_gold.g_dim_host_info
GROUP BY host_id)
WHERE num_of_listings >1
),

RESULT AS (
SELECT host_id,COUNT(DISTINCT listing_neighbourhood ) AS num_distinct_neighbourhood FROM(
SELECT d.host_id, a.listing_id, b.listing_neighbourhood
FROM rich_hosts d
LEFT JOIN public_gold.g_dim_host_info a ON d.host_id=a.host_id
LEFT JOIN public_gold.g_dim_housing_info b ON a.listing_id =b.listing_id
)
GROUP BY host_id
)

SELECT num_distinct_neighbourhood,
	   COUNT(host_id) AS frequency 
FROM RESULT GROUP BY num_distinct_neighbourhood

-- e

WITH normal_hosts AS(
SELECT host_id FROM
(SELECT host_id,COUNT(listing_id) AS num_of_listings FROM public_gold.g_dim_host_info
GROUP BY host_id)
WHERE num_of_listings =1
),

host_revenue AS(
SELECT a.host_id AS host_id,
       f.listing_id AS listing_id,
(CASE WHEN has_availability='t' THEN price*(30-availability_30) ELSE 0 END) AS monthly_estimated_revenue,
       f.date AS date
FROM normal_hosts a
LEFT JOIN public_gold.g_fact f ON a.host_id=f.host_id
LEFT JOIN public_gold.g_dim_availability b ON f.listing_id=b.listing_id
),

host_annual_revenue AS(
SELECT  
host_id, 
listing_id,
sum(monthly_estimated_revenue) AS annual_estimated_revenue
FROM host_revenue
GROUP BY host_id,listing_id,EXTRACT(YEAR FROM date),EXTRACT(MONTH FROM date)
),

RESULT AS(
SELECT 
host_id,
annual_estimated_revenue,
nlc.lga_name AS lga_neighbourhood,
median_mortgage_repay_monthly
FROM host_annual_revenue a
LEFT JOIN public_gold.g_dim_housing_info b ON b.listing_id = a.listing_id
LEFT JOIN public_bronze.nsw_lga_code nlc ON nlc.lga_name=b.listing_neighbourhood 
LEFT JOIN public_silver.nsw_census_02 c ON c.lga_code =nlc.lga_code
)


-- portion of hosts with single listing that can afford their annualized mortgage with their listing revenue
SELECT COUNT(DISTINCT host_id) FILTER(WHERE annual_estimated_revenue >= median_mortgage_repay_monthly)::float/COUNT(DISTINCT host_id)::float
AS mortgage_affordable_ratio
FROM RESULT;


-- lga with most number of hosts that can afford mortgage with their listing revenue 
SELECT lga_neighbourhood,
COUNT(DISTINCT host_id) AS num_hosts_afford_mortgage FROM RESULT
WHERE annual_estimated_revenue >= median_mortgage_repay_monthly
GROUP BY lga_neighbourhood
ORDER BY num_hosts_afford_mortgage DESC





