require(dplyr)
require(lubridate)
require(gdata)
require(xlsx)


my_db <- src_postgres(dbname = 'cxtd', host = 'localhost', port = 5432, user = 'cxtd', password = 'xintiandi')

member <- data.frame(tbl(my_db, 'member'))
wechat_member <- data.frame(tbl(my_db, 'wechat_member'))
coupon <- data.frame(tbl(my_db, 'coupon'))

unique(member$member_source)

member <- tbl_df(data.frame(tbl(my_db, 'member'))) %>% mutate(created_datetime = ymd_hms(created_datetime))
#####
# current_date <- today()
# duration <- ddays(7)
# end_time <- current_date
# start_time <- end_time - duration



test_func <- function(member_df, end_time, rpt_dur=7){
    
    # calculate report duration
    rpt_dur <- ddays(rpt_dur)
    
    # if end_time is missing use "today":00:00:00 as end time
    end_time <- ifelse(missing(end_time), floor_date(ymd_hms(Sys.time()), unit = 'day'), ymd(end_time))
    
    # calculate start of time
    start_time <- end_time - rpt_dur
    
    # taking subset of current 
    # member_df_sub <- member_df %>% filter(created_datetime < end_time & created_datetime >= start_time) 

    # get member_report
    hq_df <- get_member_growth(member_df, start_time = start_time, end_time = end_time, m_source = "08") %>% 
        #rename_col            
                select(mydate,
                       hq_member = member_count, 
                       hq_cum = cum_sum)
    
    rh_df <- get_member_growth(member_df, start_time = start_time, end_time = end_time, m_source = "03") %>%
                    select(mydate,
                           rh_member = member_count, 
                           rh_cum = cum_sum)
    
    itiandi_df <- get_member_growth(member_df, start_time = start_time, end_time = end_time, m_source = "09") %>%
                     select(mydate,
                           itiandi_member = member_count, 
                           itiandi_cum = cum_sum)
    
    tpq_df <- get_member_growth(member_df, start_time = start_time, end_time = end_time, m_source = "02") %>%
                 select(mydate,
                       tpq_member = member_count, 
                       tpq_cum = cum_sum)

    # combine DFs and output
    output <- hq_df %>% 
                    inner_join(rh_df, by="mydate") %>%
                    inner_join(itiandi_df, by='mydate') %>%
                    inner_join(tpq_df, by = 'mydate')
    return(output)
    
}

get_member_growth <- function(member_subset, start_time, end_time, m_source) {
    # calculate member growth by day
    member_by_date <- member_subset %>% filter(member_source == m_source) %>% 
        mutate(mydate=floor_date(created_datetime, unit = "day")) %>% 
        group_by(mydate) %>% 
        summarise(member_count = n())
    
    # adding "fake date"
    filler_date <- member_subset %>% 
        filter(member_source == m_source) %>% 
        mutate(mydate=floor_date(created_datetime, unit = "day")) %>%
        # summarise(max = max(mydate, (end_time-ddays(1))),
        summarise(max = max(mydate, end_time),
                  min = min(mydate, start_time))
    
    filler <- data.frame(mydate=filler_date$min + days(seq(from=0, 
                                                           to=as.integer(filler_date$max - filler_date$min))),
                         member_count2 = 0)         
    
    # join and create data
    member_by_date <- member_by_date %>% 
        full_join(filler, by = "mydate") %>% 
        mutate(member_count = ifelse(is.na(member_count), 
                                     member_count2, 
                                     member_count)) %>%
        select(-member_count2) %>%
        arrange(mydate) %>% 
        mutate(cum_sum = cumsum(member_count))
    
    # take subset of the report period
    member_by_date <- member_by_date %>% 
                        filter(mydate >= start_time & mydate < end_time)
    return(member_by_date)
}

# aaa <- test_func(member, '2015-10-26')



spending_func <- function(db_con, end_time=today(), rpt_dur=7, non_shop_list = c(161, 160, 49, 58)){
    
    # calculate report duration
    rpt_dur <- ddays(rpt_dur)
    end_time <- ymd(end_time)
    # if end_time is missing use "today":00:00:00 as end time
    # end_time <- ifelse(missing(end_time), floor_date(ymd_hms(Sys.time()), unit = 'day'), ymd(end_time))
    
    # calculate start of time
    start_time <- end_time - rpt_dur
    end_time <- as.character(end_time)
    start_time <- as.character(start_time)
    
    ##
    ## HARDCODED non-shop list
    
    # frozen member
    frozen_user <- tbl(db_con, 'member') %>% 
        filter(status != 0) %>%
        select(member_id)
    
    # mall_data 
    mall <- tbl(db_con, 'mall') %>% 
        select(mall_id, 
               mall_name = name_sc) #%>%
#         filter(is_mall == 1) 
    # shop_data
    shop <- tbl(db_con, 'shop') %>% 
        select(shop_id, shop_code, name_sc, mall_id) %>% 
        filter(!shop_id %in% non_shop_list) %>% 
        inner_join(mall, by = "mall_id")
    # sales data
    sales_sub <- tbl(db_con, 'sales') %>% 
        filter(transaction_datetime >= start_time & 
                   transaction_datetime < end_time &
                   invoice_original_amount > 0) %>% 
        inner_join(shop, by="shop_id") %>%
        anti_join(frozen_user, by = "member_id")
    
    cum_unique_sales <- tbl(db_con, 'sales') %>% 
        filter(invoice_original_amount > 0 & 
                   transaction_datetime < end_time) %>%
        select(shop_id, member_id) %>%
        inner_join(shop, by = 'shop_id') %>%
        anti_join(frozen_user, by = "member_id") %>%
        group_by(mall_name) %>% 
        summarize(cum_unique_purchaser = count(distinct(member_id))) %>%
        collect()
    
    # sales point issue
    sales_point_issue <- tbl(db_con, 'sales_point_issue') %>% 
        filter(created_datetime >= start_time & 
                   created_datetime < end_time) %>% 
        anti_join(frozen_user, by = "member_id") %>%
        transmute(sales_id, 
                  point_issue = point)
    
        
    # check
    if(any(duplicated(sales_point_issue$sales_id))){
        warning('DUPLICATED SALES_ID in sales_point_issue')
    }
    
    # pt1
    sales_data <- sales_sub %>% 
        filter(invoice_original_amount > 0) %>%
        group_by(mall_name) %>% 
        summarize(n_uniq_member = count(distinct(member_id)),
                  total_purchase = sum(sales_settlement_amount),
                  n_transaction = n()) %>%
        mutate(avg_transaction_amount = total_purchase/n_transaction) %>% 
        collect()
    
    # pt2 
    point_data <- sales_sub %>% 
        inner_join(sales_point_issue, by = 'sales_id') %>% 
        group_by(mall_name) %>% 
        summarize(total_point_issue = sum(point_issue)) %>% 
        collect()
    
    # knitting 2 parts together
    ##### if nrow == 0 create fake sales_data & point_data DF 
    if (nrow(sales_data) == 0) {
        print('no sales data this week')
        final_member_sales <- tbl_df(data.frame(mall_name = character(), 
                                                n_uniq_member = integer(),
                                                total_purchase = integer(),
                                                n_transaction = integer(),
                                                avg_transaction_amount = integer(),
                                                total_point_issue = integer(),
                                                cum_unique_purchaser = integer()
                                                ))
        
    }else if (nrow(point_data) == 0) {
        
        point_data <- tbl_df(data.frame(mall_name = character(), 
                                       total_point_issue = integer()))
        
        final_member_sales <- sales_data %>% 
            full_join(point_data, by = 'mall_name') %>%
            full_join(cum_unique_sales, by = 'mall_name') %>%
            collect() %>%
            mutate_each(funs(ifelse(is.na(.), 0, .)))    
        
    }else {
        final_member_sales <- sales_data %>% 
            full_join(point_data, by = 'mall_name') %>%
            full_join(cum_unique_sales, by = 'mall_name') %>%
            collect() %>%
            mutate_each(funs(ifelse(is.na(.), 0, .)))    
    }
    
        
    
    return(final_member_sales)
}

#
tenant_func <- function(db_con, end_time=today(), rpt_dur=7, non_shop_list = c(161, 160, 49, 58)){
    # HARDCODED contract_type 
    # contract_type <- tbl_df(data.frame(contract_type = c(0, 1, 2, 3, 4),
                                       # contract_type_sc = c('积分', '优惠券', '折扣', '礼品', '非合作')))
    contract_type_mapping <- c('积分', '优惠券', '折扣', '礼品', '非合作')
    names(contract_type_mapping) <- c(0, 1, 2, 3, 4)
    # calculate report duration
    rpt_dur <- ddays(rpt_dur)
    end_time <- ymd(end_time)
    start_time <- end_time - rpt_dur
    # if end_time is missing use "today":00:00:00 as end time
    # end_time <- ifelse(missing(end_time), floor_date(ymd_hms(Sys.time()), unit = 'day'), ymd(end_time))
        # calculate start of time
    if (month(end_time) != month(start_time)) {
        last_month_end <- start_time
        last_month_start <- floor_date(last_month_end, unit = "month")
        
    }else {
        last_month_end <- floor_date(start_time, unit = "month")
        last_month_start <- floor_date(last_month_end - ddays(7), unit = 'month')    
    }
    # print(last_month_end)
    # print(last_month_start)
    
    
    
    # characterize
    current_month_start <- as.character(last_month_end)
    current_month_end <- as.character(end_time)
    end_time <- as.character(end_time)
    start_time <- as.character(start_time)
    last_month_end <- as.character(last_month_end)
    last_month_start <- as.character(last_month_start)
    ##
    ## HARDCODED non-shop list
    
    # frozen member
    frozen_user <- tbl(db_con, 'member') %>% 
        filter(status != 0) %>%
        select(member_id)
    
    # mall_data 
    mall <- tbl(db_con, 'mall') %>% 
        select(mall_id, 
               mall_name = name_sc) %>%
        filter(is_mall == 1) 
    
    # category 
    category <- tbl(db_con, 'category') %>% 
        select(category_id, category_sc = name_sc)
    
    # contract_shop_mapping 
    contract_shop_mapping <- tbl(db_con, 'contract_shop_mapping') %>% 
        select(contract_id, shop_id)
    
    # contract 
    contract <- tbl(db_con, 'contract') %>% 
        select(contract_id, contract_type) %>% 
        filter(status == 0) %>% 
#         collect() %>% 
#         mutate(contract_type = plyr::mapvalues(contract_type, from = names(contract_type_mapping), to = contract_type_mapping))
        inner_join(contract_shop_mapping, by = 'contract_id')
        
        # shop_data
    shop <- tbl(db_con, 'shop') %>% 
        select(shop_id, shop_code, 
               shop_name = name_sc,
               mall_id, category_id) %>% 
        filter(!shop_id %in% non_shop_list) %>% 
        inner_join(category, by = 'category_id') %>%
        inner_join(mall, by = "mall_id") %>% 
        inner_join(contract, by = 'shop_id') %>%
        collect() %>% 
        mutate(contract_type = plyr::mapvalues(contract_type, from = names(contract_type_mapping), to = contract_type_mapping)) %>%
        select(-mall_id, -category_id, -contract_id)
    
    ############ 
    # sales data
    ### supposed fucked data ###
    fucked_sales <- tbl(db_con, 'sales') %>% 
        filter(transaction_datetime >= '2015-12-07' & 
                   transaction_datetime < '2015-12-12'  &
                   invoice_original_amount > 0 &
                   shop_id == 144) %>%
        inner_join(frozen_user, by = 'member_id')
    ### end here ###
    
    ## CURRENT WEEK
    sales_sub <- tbl(db_con, 'sales') %>% 
        filter(transaction_datetime >= start_time & 
                   transaction_datetime < end_time &
                   invoice_original_amount > 0) %>%
        anti_join(frozen_user, by = 'member_id')
    
    ## LAST MONTH
    sales_last_month <- tbl(db_con, 'sales') %>% 
        filter(transaction_datetime < start_time & 
                   invoice_original_amount > 0) %>%
        anti_join(frozen_user, by = 'member_id') %>%
        dplyr::union(fucked_sales)
#         filter(transaction_datetime >= last_month_start & 
#                    transaction_datetime < last_month_end &
#                    invoice_original_amount > 0) %>%
    
    ## CURRENT MONTH CUM SUM
    sales_current_cum <- tbl(db_con, 'sales') %>% 
        filter(transaction_datetime < end_time & 
                   invoice_original_amount > 0) %>%
        anti_join(frozen_user, by = 'member_id') %>%
        dplyr::union(fucked_sales)
#         filter(transaction_datetime >= current_month_start& 
#                    transaction_datetime < current_month_end &
#                    invoice_original_amount > 0) %>%

    ###############
    ### AGGREGATION
    # current week
    current <- sales_sub %>% 
        group_by(shop_id) %>% 
        summarize(current_unique_purchaser = count(distinct(member_id)),
                  current_amount = sum(sales_settlement_amount),
                  current_transactions = n()) %>% 
        collect()
    
    if (nrow(current) == 0){
        current <- tbl_df(data.frame(shop_id = integer(), 
                                     current_unique_purchaser = integer(),
                                     current_amount = integer(),
                                     current_transactions = integer()))
    }
    # last month
    last_mon <- sales_last_month %>% 
        group_by(shop_id) %>% 
        summarize(last_unique_purchaser = count(distinct(member_id)),
                  last_amount = sum(sales_settlement_amount),
                  last_transactions = n()) %>% 
        collect()
    
    if (nrow(last_mon) == 0){
        last_mon <- tbl_df(data.frame(shop_id = integer(), 
                                     last_unique_purchaser = integer(),
                                     last_amount = integer(),
                                     last_transactions = integer()))
    }
    # current month cum sum 
    cum_sum <- sales_current_cum %>% 
        group_by(shop_id) %>% 
        summarize(cum_unique_purchaser = count(distinct(member_id)),
                  cum_amount = sum(sales_settlement_amount),
                  cum_transactions = n()) %>% 
        collect()
    
    if (nrow(cum_sum) == 0){
        cum_sum <- tbl_df(data.frame(shop_id = integer(), 
                                     cum_unique_purchaser = integer(),
                                     cum_amount = integer(),
                                     cum_transactions = integer()))
    }
    
    ### COMBINE
    final <- current %>% 
        full_join(last_mon, by = "shop_id") %>%
        full_join(cum_sum, by = 'shop_id') %>% 
        # inner_join(shop, by = 'shop_id') %>%
        right_join(shop, by = 'shop_id') %>%
        arrange(mall_name, shop_id) %>%
        mutate(filler1 = "", filler2 = "", filler3 = "",
               last_avg_amount = last_amount/last_transactions,
               cum_avg_amount = cum_amount/cum_transactions) %>%
        mutate_each(funs(ifelse(is.na(.), 0, .))) %>%
        select(mall_name, shop_code, shop_id, shop_name, contract_type, category_sc,
               last_unique_purchaser, current_unique_purchaser, cum_unique_purchaser,filler1,
               last_amount, current_amount, cum_amount, filler2,
               last_transactions, current_transactions, cum_transactions, filler3, 
               last_avg_amount, cum_avg_amount) %>% 
        mutate(current_month_start = current_month_start,
               current_month_end = current_month_end,
               current_week_start = start_time,
               current_week_end = end_time,
               last_month_start = last_month_start, 
               last_month_end = last_month_end)
    
    return(final)
}


getting_file_dir <- function(base_dir=getwd(), output_file_name='member_growth.xlsx') {
    new_dir <- file.path(base_dir)
    
    if (!file.exists(file.path(paste(base_dir, 'member_growth', today(), sep = '/')))) {
        if (!file.exists(file.path(paste(base_dir, 'member_growth',sep = '/')))) {
            dir.create(file.path(paste(base_dir, 'member_growth',sep = '/')))
        }    
        new_dir <- paste(new_dir, 'member_growth', sep = '/')
        print(new_dir)
        dir.create(file.path(paste(new_dir, today(), sep = '/')))
        new_dir <- paste(new_dir, today(), sep = '/')
        print(new_dir)
    }
    if(base_dir == new_dir){
        ### do something
        new_dir <- file.path(paste(base_dir, 'member_growth', today(), sep = '/'))
    }
    #create file names
    sales_output_dir <- normalizePath(paste(new_dir, output_file_name, sep = '/'))
    return(sales_output_dir)
}



output_xlsx <- function(df_list, report_output_dir){
    wb <- createWorkbook()    
    # each mall
    for (df_nm in names(df_list)){
        current_sheet <- createSheet(wb, sheetName=df_nm)
        current_df <- df_list[[df_nm]]
        addDataFrame(current_df, current_sheet)
    }
    
    
    #save xlsx
    saveWorkbook(wb, report_output_dir)
}


#########################
# acutal FUNCTION CALLS #
#########################
# member_report <- test_func(member_df = member, end_time = '2016-2-13')
# sales_report <- spending_func(my_db, end_time = "2016-2-13")
# tenant_report <- tenant_func(my_db, end_time = "2016-2-13")

member_report <- test_func(member_df = member)
sales_report <- spending_func(my_db)
tenant_report <- tenant_func(my_db)

df_list <- list("member_report" = member_report, 
                "sales_report" = sales_report,
                "tenant_report" = tenant_report)

report_output_dir <- getting_file_dir()
# report_output_dir <- getting_file_dir('~/src/all_reports')
# prep excel 

output_xlsx(df_list, report_output_dir) 

print(paste(now(), 'Success!', sep = " "))












# 
# 
# 
# 
# #get base directory
# base_dir <- getwd()
# dir.create(file.path(paste(base_dir, 'reports', today(), sep = '/')))
# 
# #create file names
# member_file_name <- paste('member_growth', '.xlsx', sep = '')
# member_output_dir <- paste(base_dir, 'reports', today(), member_file_name, sep = '/')
# 
# sales_file_name <- paste('sales_n_points', '.xlsx', sep = '')
# sales_output_dir <- paste(base_dir, 'reports', today(), sales_file_name, sep = '/')
# 
# # create dir 
# write.xlsx2(x=member_report, file = member_output_dir)
# write.xlsx2(x=sales_report, file = sales_output_dir)



# 
# 
# test <- function(end_time=today(), rpt_dur=7){
#      # calculate report duration
#     rpt_dur <- ddays(rpt_dur)
#     end_time <- ymd(end_time)
#     # if end_time is missing use "today":00:00:00 as end time
#     # end_time <- ifelse(missing(end_time), floor_date(ymd_hms(Sys.time()), unit = 'day'), ymd(end_time))
#     
#     # calculate start of time
#     start_time <- end_time - rpt_dur
#     day <- today()
#     print(start_time)
#     print(day)
#     day2 <- day - ddays(2)
#     print(end_time)
#     print(as.character(end_time))
# }
# 
# ymd(today())
# test()
# 
# 
# 
# end_time <- floor_date(ymd_hms(Sys.time()))
# rpt_dur=ddays(20)
# start_time <- end_time - rpt_dur


                       


# coupon_sub <- coupon %>% 
#     filter(coupon_redeem_datetime >= start_time & 
#                coupon_redeem_datetime < end_time) %>% 
#     select(redeem_sales_id)



# calculate point issue and point redeem




# real_sales <- sales_sub %>% anti_join(coupon_sub, by=c('sales_id' = 'redeem_sales_id'))

### join first and check total_sales == 0 & has coupon_redemption












# 
# #####################################
# #pt1 summary member growth by date
# member_by_date <- member %>% filter(member_source == m_source) %>% 
#     mutate(mydate=floor_date(created_datetime, unit = "day")) %>% 
#     group_by(mydate) %>% 
#     summarise(member_count = n())
# 
# #pt2 adding fake 0 to date that has 0 member
# filler_date <- member %>% filter(member_source == m_source) %>% 
#     mutate(mydate=floor_date(created_datetime, unit = "day")) %>%
#     summarise(max = max(mydate),
#               min = min(mydate))
# #adding 0 to DFs
# filler <- data.frame(mydate=filler_date$min + days(seq(from=0, 
#                                                        to=as.integer(filler_date$max - filler_date$min))),
#                      member_count2 = 0)         
# #pt3 join and create data
# member_by_date <- member_by_date %>% 
#     full_join(filler, by = "mydate") %>% 
#     mutate(member_count = ifelse(is.na(member_count), 
#                                  member_count3, 
#                                  member_count)) %>%
#     select(-member_count2) %>%
#     arrange(mydate) %>% 
#     mutate(cum_sum = cumsum(member_count))
# 
# 
# View(member_by_date)
# 
