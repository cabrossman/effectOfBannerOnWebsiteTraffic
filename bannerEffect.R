##############################################
# This script is run all sequentially. 
#############################################



#############################################################
# Functions & imports
#############################################################
  library(tidyverse)
  library(broom)
  library(sqldf)
  library(sandwich)
  library(lmtest)
  #function to do fuzzy match
  fmatch <- function(source1, source2){
    source1$name<-as.character(source1$name)
    source2$name<-as.character(source2$name)
    
    # It creates a matrix with the Standard Levenshtein distance between the name fields of both sources
    dist.name<-adist(source1$name,source2$name, partial = TRUE, ignore.case = TRUE)
    
    # We now take the pairs with the minimum distance
    min.name<-apply(dist.name, 1, min)
    
    
    df = NULL
    for(i in 1:NROW(source1$name)){
      for(j in 1:NROW(source2$name)){
        if(dist.name[i,j] == min.name[i]){
          temp <- cbind.data.frame(thing = source1$name[i], closestMatch = source2$name[j], dist = min.name[i])
          df <- rbind(df,temp)
        }
      }
    }
    
    return(df)
    
  }
  
  
  
  
  #data
  #AX billing -- needed to tell if customer (SF) has both website and banner
  path <- paste0(getwd(),"/raw_data/")

#############################################################
# Data sources not actually needed
#############################################################

  
  # SF to AX customer mapping - may not need this
    #AX_customerList <- read_csv(paste0(path,'AX Customer List.csv'))
  # Rev Data -- may not need this
    # revData <- read_csv(paste0(path,'revData.csv'))
    
  # website traffic 
  # this data is used to get the response variable, website traffic as we are investigating the conditional variable, banner traffic
    
  
#############################################################
# Pull in website traffic
#############################################################
    websiteTraffic <- read_csv(paste0(path,'in.c-keboola-ex-google-analytics-v4-228443150.Daily-Traffic.csv'))
    websiteProfiles <- read_csv(paste0(path,'in.c-keboola-ex-google-analytics-v4-228443150.profiles.csv'))
    
    
    #join traffic data with dimensional data, filter out development sites
    websiteData <- inner_join(websiteTraffic, websiteProfiles, by=c("idProfile" = "id")) %>% 
      filter(!grepl('dmmwebsites.com|local|vagrant|boatwizardwebsolutions',webPropertyName)) %>% 
      filter(!grepl('dmmwebsites.com|local|vagrant|boatwizardwebsolutions',hostname)) %>%
      filter(!grepl('^[1-9]', hostname)) %>% filter(!grepl('^abcdefg', hostname)) %>%
      select(webPropertyName, hostname, date, sessions, pageviews, users)
    
    #remove old data
    rm(websiteTraffic,websiteProfiles)
    
###########################################################################################################
#join websiteData with SF_to_Website Data ---- This allows us to map customer website traffic with SF id
###########################################################################################################
  
    
    #read in SF_to_website data
    SF_to_Website <- read_csv(paste0(path,'SF ID with Website.csv'))
    SF_to_Website$SF_ID <- SF_to_Website$`18AccountID`
    
    #get matches on Account name
    MatchOnAcctName <- websiteData %>% inner_join(SF_to_Website, by=c("webPropertyName"="Account Name")) %>% select(webPropertyName, SF_ID) %>% distinct()
    #get matches on AKA
    MatchOnAKA <- websiteData %>% inner_join(SF_to_Website, by=c("webPropertyName"="AKA")) %>% select(webPropertyName, SF_ID) %>% distinct()
    #get matches on web url
    MatchOnURL <- websiteData %>% inner_join(SF_to_Website, by=c("hostname"="Website")) %>% select(webPropertyName, SF_ID) %>% distinct()
    #merge together flows
    websiteToSF <- rbind(MatchOnAcctName, MatchOnAKA, MatchOnURL) %>% distinct()
    
    #how many matched out of total?
    print(paste0('Matched ',length(unique(websiteToSF$webPropertyName)),' of ',length(unique(websiteData$webPropertyName)), ' web properties'))
  
    #remove old files
    rm(SF_to_Website, MatchOnAcctName, MatchOnAKA, MatchOnURL)
  

###########################################################################################################
#join websiteToSF data to operative  -- this allows us to map customer to sales order/banner data
###########################################################################################################

    # this data links the Operative data to SF
    Operative_to_SF <- read_csv(paste0(path,'OperativeSF_OAS_Map.csv')) 
    
    #join  the operative data to website/Sf data
    sfID_to_salesOrderId <- Operative_to_SF %>% inner_join(websiteToSF, by=c("COMBO_EXTERNAL_ID"="SF_ID")) %>%
      select(SF_ID = COMBO_EXTERNAL_ID, Sales_Order_ID) %>% distinct()
    
    #this will link website - SF - SalesOrder
    website_to_SF_to_banner <- websiteToSF %>% left_join(sfID_to_salesOrderId, by="SF_ID")
    
    rm(Operative_to_SF,sfID_to_salesOrderId,websiteToSF)
    
    
###########################################################################################################
#Bring in banner traffic data  --  This is the conditional variable of interest in model
###########################################################################################################
    
    #pull in dimensional and fact details from banner traffic
    campaign_dim <- read_csv(paste0(path,'out.c-AdOps-rs.campaign_dim.csv'))
    campaign_fact <- read_csv(paste0(path,'out.c-AdOps-rs.page_pos_fact_alldata.csv'))
    
    #create a temp table for only those order Id's which matched in prior matching (SF-website-operative)
    # this will be used to filter down to only those sales order Id's in the billing data below
    salesOrdersToFilter <- website_to_SF_to_banner %>% filter(!is.na(Sales_Order_ID)) %>% distinct(Sales_Order_ID) %>% as.list() %>% unlist()
    
    
    #join dimesnion and fact banner data and filter on marine only salesgroup. Get SalesOrderID from insertion_order. Filter for campaigns outside of scope.
    bannerData <- campaign_dim %>% 
      filter(salesgroup == 'MARINE') %>% inner_join(campaign_fact, campaign_dim, by=c("campaign_id")) %>% 
      mutate(Sales_Order_ID = as.integer(substr(insertion_order,1,5))) %>%
      filter(Sales_Order_ID %in% salesOrdersToFilter) %>%
      filter(as.Date(end_date) > as.Date('2016-01-01'), as.Date(start_date) < as.Date('2017-01-01')) 
    
    
    #Bring in WebPropertyName to Banner data. This links the banner data to the web data
    bannerDataWebProp <- website_to_SF_to_banner %>% filter(!is.na(Sales_Order_ID)) %>% distinct(webPropertyName, Sales_Order_ID) %>%
      inner_join(bannerData, by="Sales_Order_ID")
    
####The next 3 are actually not used because they are fixed effects. But I left these in.###########################################  
    
    
    #get top Pos by impression per webPropertyName, event_date
    webPropDateTopPos <- bannerDataWebProp %>% group_by(webPropertyName,event_date,pos) %>% summarise(impressions = sum(impressions)) %>% 
      group_by(webPropertyName,event_date) %>% mutate(n = row_number(desc(impressions))) %>% ungroup() %>% 
      filter(n == 1) %>% select(webPropertyName,event_date,pos)
    
    
    #get section by impression per webPropertyName, event_date
    webPropDateTopSection <- bannerDataWebProp %>% group_by(webPropertyName,event_date,site_section) %>% summarise(impressions = sum(impressions)) %>% 
      group_by(webPropertyName,event_date) %>% mutate(n = row_number(desc(impressions))) %>% ungroup() %>% 
      filter(n == 1) %>% select(webPropertyName,event_date,site_section)
  
    ##et top portal by impression per webPropertyName, event_date
    webPropDateTopPortal <- bannerDataWebProp %>% group_by(webPropertyName,event_date,portal) %>% summarise(impressions = sum(impressions)) %>% 
      group_by(webPropertyName,event_date) %>% mutate(n = row_number(desc(impressions))) %>% ungroup() %>% 
      filter(n == 1) %>% select(webPropertyName,event_date,portal)  
    
    
#####################################################################################################################################      
      
    #aggregate banner data to WebpropertyName & event date with dimensions aggregated from facts above
    AggBannerData <- bannerDataWebProp %>% select(webPropertyName, event_date, impressions, clicks) %>%
      inner_join(webPropDateTopPos, by=c("webPropertyName"="webPropertyName","event_date"="event_date")) %>% 
      inner_join(webPropDateTopSection, by=c("webPropertyName"="webPropertyName","event_date"="event_date")) %>% 
      inner_join(webPropDateTopPortal, by=c("webPropertyName"="webPropertyName","event_date"="event_date")) %>% 
      group_by(webPropertyName, pos, site_section, portal, event_date) %>% 
      summarise(impressions = sum(impressions), clicks = sum(clicks)) %>%
      ungroup()
    
    #remove old data
    rm(campaign_dim, campaign_fact, bannerData, bannerDataWebProp, webPropDateTopPos, webPropDateTopSection, webPropDateTopPortal,salesOrdersToFilter)
    
   
    
###########################################################################################################
#join website and banner data into One Files
###########################################################################################################

    #aggregate website Data to webPropertyName and Date
    websiteDataAgg <- websiteData %>% group_by(webPropertyName, date) %>% summarise(sessions = sum(sessions), pageviews = sum(pageviews), users = sum(users)) %>%
      filter(as.Date(date) < as.Date('2017-01-01'), as.Date(date) >= as.Date('2016-01-01'))
      
    rm(websiteData)
    
    #Join  banner data and website data on webproperty name and date. Make nulls = 0 for impressions and clicks. Create new column for month too
    combo_data <- left_join(websiteDataAgg,AggBannerData, by=c("webPropertyName"="webPropertyName","date"="event_date")) %>% 
      mutate(impressions = ifelse(is.na(impressions),0,impressions), clicks= ifelse(is.na(clicks),0,clicks)) %>%
      mutate(month = substr(as.character(date),6,7)) %>% mutate(bannerRun = ifelse(impressions > 0, 'TRUE', 'FALSE'))
    
    rm(AggBannerData, website_to_SF_to_banner, websiteDataAgg)
  
    
###########################################################################################################
#pull in economic variables and join with combo_date & remove fixed effects I created ealier
###########################################################################################################
    
    
    #unemployment
    UNEMPLOY <- read_csv(paste0(path,'UNEMPLOY.csv')) %>% 
      mutate(year = substr(as.character(DATE),1,4), month = substr(as.character(DATE),6,7)) %>% 
      filter(year == 2016) %>% select(month, unemploy = UNEMPLOY)
    
    #CPI
    CPI <- read_csv(paste0(path,'CPIAUCSL.csv')) %>% 
      mutate(year = substr(as.character(DATE),1,4), month = substr(as.character(DATE),6,7)) %>% 
      filter(year == 2016) %>% select(month,cpi = CPIAUCSL)
    
    
    #take out some explanatory vars as they are fixed effects
    combo <- combo_data %>% left_join(UNEMPLOY,by="month") %>% left_join(CPI,by="month") %>%
      select(-site_section,-portal,-pos)
    
    rm(combo_data, CPI, UNEMPLOY)
    
################################################################################
# Summary Statsssss
################################################################################
    
    #histogram on page views per day webPropertyName
    combo %>% group_by(webPropertyName) %>% summarise(avg_pv = mean(pageviews)) %>% qplot(avg_pv,data=.,geom="histogram", bins=50) + ggtitle("Avg Page Views Per Day Per Website in Sample")
    
    #histogram on page views
    combo %>% group_by(webPropertyName) %>% summarise(avg_sessions = mean(sessions)) %>% qplot(avg_sessions,data=.,geom="histogram", bins=50) + ggtitle("Avg Sessions Per Day Per Website in Sample")
    
    #histogram on page views
    combo %>% group_by(webPropertyName) %>% summarise(avg_users = mean(users)) %>% qplot(avg_users,data=.,geom="histogram", bins=50) + ggtitle("Avg Users Per Day Per Website in Sample")
################################################################################
# Modeling
################################################################################
    #ensure proper data types for modeling
    combo$webPropertyName <- factor(combo$webPropertyName)
    combo$date  <- factor(combo$date)
    combo$month <- factor(combo$month)
    combo$sessions <- as.numeric(combo$sessions)
    combo$pageviews <- as.numeric(combo$pageviews)
    combo$users <- as.numeric(combo$users)
    combo$impressions <- as.numeric(combo$impressions)
    combo$clicks <- as.numeric(combo$clicks)
    combo$unemploy <- as.numeric(combo$unemploy)
    combo$cpi <- as.numeric(combo$cpi)
    combo$bannerRun <- factor(combo$bannerRun)
    
    #cluster standard errors around webproperty name
    coefSTDerr <- function(dataframe,model){
      G <- length(unique(dataframe$webPropertyName))
      N <- length(dataframe$webPropertyName)
      dfa <- (G/(G - 1)) * (N - 1)/ model$df.residual
      c_vcov <- dfa*vcovHC(model, type = "HC0", cluster = "group", adjust = T)
      return(coeftest(model, vcov = c_vcov))
    }
    
    
####################################################################################################################
# Notes on model
# what is the effect of banner (had any banner running, impressions, clicks) on traffic (pageviews, users, sessions)
# holding constant:
#   individual website - this fixes things within a company. Any fixed effects are captured here
#   month - this fixes things similar to all websites at the month level such as economic changes and seasonal fluctuations
#   day - this fixes things similar to all websites at the day level such as holidays
####################################################################################################################
    
    
    
####################################################################################################################
# run 27 regressions and extract term, estimate, std.error, statistic, and p.value for a single specification
####################################################################################################################
    
    #create DF with combination lookup files
    
    #dependent var combos
    d_var <- cbind.data.frame(dindex = c(1:3), dvar = c('pageviews','users','sessions'))
    #indep var combos
    i_var <- cbind.data.frame(iindex = c(1:3), ivar = c('bannerRun','impressions','clicks'))
    #specification
    spec <- cbind.data.frame(sindex = c(1:3), svar = c('level-level','log-level','log-log'))
    
    #cross_join 
    lookup <- sqldf("select * from d_var, i_var, spec") %>% mutate(index = paste0(dindex,'-',iindex,'-',sindex)) %>% select(index, dvar, ivar, svar)
    
    #make sure everything is string in lookup
    for(q in 1:length(lookup)){
      lookup[,q] <- as.character(lookup[,q])
    }
    
    rm(d_var, i_var, spec)
    
    
    allRegs <- NULL
    cnt <- 1
    #pageviews, users, sessions
    for(i in 1:3){
      
      #bannerRun, impressions/1000, clicks
      for(j in 1:3){
        
        #level-level, log-level, log-log
        for(k in 1:3){
          
          
          
          #create index to find lookup
          regToRunIndex <- paste0(i,'-',j,'-',k)
          
          #get reg setup params
          regToRun <- lookup %>% filter(index == regToRunIndex)
          
          #setup functional form
          d.var <- ifelse(grepl('^log',regToRun$svar),paste0('log(',regToRun$dvar,' + 1)'),regToRun$dvar)
          i.var <- ifelse(grepl('log$',regToRun$svar),paste0('log(',regToRun$ivar,' + 1)'),regToRun$ivar)
          
          fstring <- paste0(d.var,' ~ ',i.var,' + month + webPropertyName + date')
          f <- as.formula(fstring)
          
          print(paste0(cnt,' of 27 : ', regToRunIndex,' : ',fstring))
          
          #'log' not meaningful for factors 
          if(!(regToRun$ivar == 'bannerRun' & regToRun$svar == 'log-log')){
            #run regression
            reg <- lm(f, data = combo)
            #cluster std errors around webproperty id
            adjreg <- coefSTDerr(combo, reg)
            
            regVarOfInterest <- tidy(adjreg) %>% filter(grepl(regToRun$ivar,term)) %>% 
              mutate(dvar = regToRun$dvar, ivar = regToRun$ivar, form = regToRun$svar, indexVar = regToRun[,1], count = cnt,
                     interpret = 
                       ifelse(regToRun$svar == 'level-level', paste0('a one unit increase in ',regToRun$ivar,' increases ',regToRun$dvar,' by ',estimate), 
                        ifelse(regToRun$svar == 'log-level', paste0('a one unit increase in ',regToRun$ivar,' increases ',regToRun$dvar,' by ',100*estimate,'%'),
                         ifelse(regToRun$svar == 'log-log', paste0('a one pct increase in ',regToRun$ivar,' increases ',regToRun$dvar,' by ',estimate,'%'),'NA'))))
            
            allRegs <- rbind(allRegs, regVarOfInterest)
          }

          #increment count
          cnt <- cnt + 1
        }
        
      }
    }
    
#write out findings:
    write_csv(allRegs, 'allRegs.csv')