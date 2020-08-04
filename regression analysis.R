library(blscrapeR)
library(dplyr)
library(tidyr)
library(readr)
library(broom)
library(jtools)
library(sandwich)
library(AER)


require("DBI")
source("../base/connections/coririsi_layer.R")

homes_df <- dbReadTable(coririsi_layer, "second_homes_preiminary_v0_1")

metro <- read.csv("metro.csv")

df <- get_bls_county(c("June 2019", "February 2020", "March 2020", "April 2020", "May 2020", "June 2020"), seasonality = FALSE)
df$fips <- as.numeric(df$fips)
df$period <- as.character(df$period)

df$period[df$period == "2019-06-01"] <- "June2019"
df$period[df$period == "2020-02-01"] <- "Feb2020"
df$period[df$period == "2020-03-01"] <- "Mar2020"
df$period[df$period == "2020-04-01"] <- "Apr2020"
df$period[df$period == "2020-05-01"] <- "May2020"
df$period[df$period == "2020-06-01"] <- "June2020"
df <- df %>% group_by(fips) %>% mutate(min_emp = min(employed))



emp<- df %>% select(fips, area_title, period, employed) %>% 
  pivot_wider(names_from =  period, values_from = employed, names_prefix = "emp.") %>%
  mutate (emp_change = (emp.June2020 - emp.Feb2020)/emp.Feb2020) %>%
  select(fips, area_title, emp.Feb2020, emp.June2020, emp_change)

min_emp <- df %>% group_by(fips) %>% select(fips, area_title, min_emp) %>%
  distinct()

emp_month<- df %>% select(fips, area_title, period, employed) %>% 
  pivot_wider(names_from =  period, values_from = employed, names_prefix = "emp.") %>%
  mutate (emp_change_mo = (emp.June2020 - emp.May2020)/emp.May2020) %>%
  select(fips, area_title, emp_change_mo)

lf<- df %>% select(fips, area_title, period, labor_force) %>% 
  pivot_wider(names_from =  period, values_from = labor_force, names_prefix = "lf.") %>%
  mutate (lf_change = (lf.June2020 - lf.Feb2020)/lf.Feb2020) %>%
  select(fips, area_title, lf.Feb2020, lf.June2020, lf_change)

ue<- emp %>% left_join(lf) %>%
  mutate (ue_change = ((lf.June2020-emp.June2020) - (lf.Feb2020 - emp.Feb2020))/(lf.Feb2020 - emp.Feb2020)) %>%
  select(fips, area_title,ue_change)

yoy_emp <- df %>% select(fips, area_title, period, employed) %>% 
  pivot_wider(names_from =  period, values_from = employed, names_prefix = "emp.") %>%
  mutate (yoy_change = (emp.June2020 - emp.June2019)/emp.June2019) %>%
  select(fips, area_title, emp.June2019, yoy_change)

yoy_lf <-df %>% select(fips, area_title, period, labor_force) %>% 
  pivot_wider(names_from =  period, values_from = labor_force, names_prefix = "lf.") %>%
  mutate (yoy_lf_change = (lf.June2020 - lf.June2019)/lf.June2019) %>%
  select(fips, area_title, lf.June2019, yoy_lf_change)

final <- emp %>% left_join(emp_month, by = c('fips', 'area_title')) %>%
  left_join(min_emp, by = c('fips', 'area_title')) %>%
  left_join(yoy_emp, by = c('fips', 'area_title')) %>%
  left_join(lf, by = c('fips', 'area_title')) %>%
  left_join(ue, by = c('fips', 'area_title')) %>%
  left_join(yoy_lf, by = c('fips', 'area_title'))


homes_df$geoid <- as.numeric(homes_df$geoid)
homes_df <- homes_df %>% rename(fips = geoid) 
homes_df <- homes_df %>%
  mutate(homes_dpop = (potential_pop - total_population_2018)/total_population_2018) %>%
  select(fips, second_home_2018, potential_pop, homes_dpop)

final <- final %>% left_join(homes_df, "fips") %>% left_join(metro, "fips")






#########################################



require(DBI)
require(dbplyr)
require(config)
source("../base/connections/coririsi.R")

source("../base/functions/write_layer.R")

index <- tbl(coririsi, in_schema("sch_layer", "county_resilience_score_v1")) %>% 
  collect()


industry =dbReadTable(coririsi_layer, "lodes_county_2017") %>%
  rename(fips=geoid) %>%
  mutate(rec = (lodes_naics_71_2017)/lodes_all_jobs_2017) %>%
  mutate(accom = (lodes_naics_72_2017)/lodes_all_jobs_2017) %>%
  mutate(manu = lodes_naics_31_33_2017/lodes_all_jobs_2017) %>%
  mutate(admin_waste = lodes_naics_56_2017/lodes_all_jobs_2017) %>%
  mutate(prof_services = lodes_naics_54_2017/lodes_all_jobs_2017) %>%
  select(fips, rec, accom, manu, admin_waste, prof_services)

industry$fips <- as.numeric(industry$fips)
  
prop <- tbl(coririsi, in_schema("sch_layer", "bea_caemp25n_county_01_18")) %>% 
  collect() %>% 
  select(geoid, geoid_st, year, prop_emp, total_emp) %>% 
  filter(year == 2007 | year == 2018) %>%
  gather("var", "emp", prop_emp, total_emp) %>%
  unite(temp, var, year) %>%
  spread(temp, emp) %>%
  mutate(dprop07_18 = (prop_emp_2018 - prop_emp_2007)/prop_emp_2007) %>%
  mutate(prop_share_18 = prop_emp_2018/total_emp_2018) %>%
  mutate(fips = as.numeric(geoid)) %>%
  select(fips, geoid_st, dprop07_18, prop_emp_2007, prop_emp_2018 , prop_share_18) 

pop <- tbl(coririsi, in_schema("sch_layer", "bea_cainc4_county_69_18")) %>% 
  collect() %>% 
  select(geoid,year, pop) %>% 
  filter(year == 2007 | year == 2018) %>%
  gather("var", "pop", pop) %>%
  unite(temp, var, year) %>%
  spread(temp, pop) %>%
  mutate(dpop07_18 = (pop_2018 - pop_2007)/pop_2007) %>%
  mutate(fips = as.numeric(geoid)) %>%
  select(fips, dpop07_18, pop_2007, pop_2018) 

density <- dbGetQuery(coririsi_layer, "SELECT geoid, total_population_2018, land_sqmi from attr_county_full") %>%
  mutate(density = total_population_2018/land_sqmi/1000) %>%
  rename(fips = geoid) %>%
  select(fips, density)

density$fips <- as.numeric(density$fips)
 
broadband <- tbl(coririsi, in_schema("sch_analysis", "la_counties_broadband")) %>% 
  collect() %>% mutate(broadband = f477_maxad_downup_2018dec_25_3_popsum/pop2018_sum) %>%
  mutate(fips = as.numeric(geoid_co)) %>%
  select(fips, broadband,f477_maxad_downup_2018dec_25_3_popsum,pop2018_sum)

young_firm <- tbl(coririsi, in_schema("sch_layer", "lodes_county_2017")) %>% 
  collect() %>%
  mutate(firm_10yr = lodes_firm_age_0_1_2017 + lodes_firm_age_2_3_2017 + lodes_firm_age_4_5_2017 + lodes_firm_age_6_10_2017) %>%
  mutate(fips = as.numeric(geoid)) %>%
  select(fips, firm_10yr, lodes_all_jobs_2017, share_young_firm_10yr_less) 

amenity <- tbl(coririsi, in_schema("sch_layer", "natural_amenity_score_county")) %>% 
  collect() %>%
  mutate(fips = as.numeric(geoid)) %>%
  rename(amenity = natural_amenity_scale)%>%
  select(fips, amenity) 

black2010 <- tbl(coririsi, in_schema("sch_source", "ers_people")) %>% 
  collect() %>%
  mutate(fips = as.numeric(FIPS)) %>%
  rename(black2010 = BlackNonHispanicPct2010) %>%
  select(fips, black2010) 

hispanic2010 <- tbl(coririsi, in_schema("sch_source", "ers_people")) %>% 
  collect() %>%
  mutate(fips = as.numeric(FIPS)) %>%
  rename(hispanic2010 = HispanicPct2010) %>%
  select(fips, hispanic2010) 


covid_cases <- read.csv("us-counties_covid.csv")
emp.2007 <- read.csv("2007 emp.csv")
metro <- read.csv("metro.csv")
broadband_sub <- read.csv("broadband_sub.csv")
bach2010 <- read.csv("bach2010.csv")
bach2018 <- read.csv("bach2018.csv")
emp2019 <- read.csv("emp2019.csv")
amenity_full <- read.csv("amenity_full.csv")

covid_cases <- covid_cases %>% filter(date == "2020-04-12") %>% select(fips,cases,deaths)


df_emp <-broadband_sub %>% 
  left_join(bach2010, "fips") %>% 
  left_join (bach2018, "fips") %>% 
  mutate(dbach = (bach2018 - bach2010)/bach2010) %>% 
  left_join(emp2019,"fips")%>% 
  left_join(broadband, "fips") %>%
  left_join(prop, "fips") %>%
  left_join(young_firm, "fips") %>%
  left_join(amenity, "fips") %>%
  left_join(black2010, "fips") %>%
  left_join(hispanic2010, "fips") %>%
  left_join(pop, "fips") %>%
  left_join(covid_cases, "fips") %>%
  left_join(industry, "fips") %>%
  left_join(amenity_full, "fips") %>%
  left_join(density, "fips")

df_emp <- df_emp %>% left_join(emp.2007, "fips") %>% 
  mutate(demp.2007.2019 = (emp.2019 - emp.2007)/emp.2007)
df_emp <- df_emp %>%
  mutate(demp.2007.Apr20 = (emp.Apr2020 - emp.2007)/emp.2007)
df_emp <- df_emp %>% mutate(covid_per_100k = cases/(pop_2018/100000))
df_emp$covid_per_100k[is.na(df_emp$covid_per_100k)] <- 0

################################

final <- final %>% left_join(df_emp, "fips")


#################################

fit <- lm(yoy_change ~ homes_dpop + covid_per_100k + demp.2007.2019 + log(pop_2018) + dpop07_18 +   
            prop_share_18 + bach2018_share + metro + share_young_firm_10yr_less + 
            broadband_sub + black2010 + density + density*homes_dpop +
            homes_dpop*land_surface + homes_dpop*log_water + homes_dpop*july_temp + 
            july_temp + land_surface + log_water +
            rec + accom + manu + admin_waste + prof_services + geoid_st, data = final)

summ(fit,  digit =5,  cluster = geoid_st)





#########################

fit <- lm(yoy_change ~ homes_dpop + covid_per_100k + demp.2007.2019 + log(pop_2018) + dpop07_18 +   
            prop_share_18 + bach2018_share + metro + share_young_firm_10yr_less + 
            broadband_sub + black2010 + density +
            july_temp + july_humidity  + land_surface + log_water +
            rec + accom + manu + admin_waste + prof_services + geoid_st, data = final)

summ(fit,  digit = 5,  cluster = geoid_st)

######################################

fit <- lm(emp_change ~ homes_dpop + covid_per_100k + demp.2007.2019 + log(pop_2018) + dpop07_18 +   
            prop_share_18 + bach2018_share + metro + share_young_firm_10yr_less + 
            broadband_sub + black2010 + density + density*homes_dpop +
            homes_dpop*land_surface + homes_dpop*log_water + homes_dpop*july_temp + 
            july_temp + land_surface + log_water +
            rec + accom + manu + admin_waste + prof_services + geoid_st, data = final)

summ(fit,  digit = 5,  cluster = geoid_st)

######################################

fit <- lm(emp_change ~ homes_dpop + covid_per_100k + demp.2007.2019 + log(pop_2018) + dpop07_18 +   
            prop_share_18 + bach2018_share + metro + share_young_firm_10yr_less + 
            broadband_sub + black2010 + density + 
            july_temp + july_humidity  + land_surface + log_water +
            rec + accom + manu + admin_waste + prof_services + geoid_st, data = final)

summ(fit,  digit = 5,  cluster = geoid_st)

###################################
