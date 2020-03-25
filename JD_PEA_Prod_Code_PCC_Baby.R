#************************************************************************************************************#
#************************************************************************************************************#
#************************************************************************************************************#
##############################################################################################################
# ------- Load the library and RHIVE #########################################################################
##############################################################################################################
library(data.table)
library(coda)        #prepare for MCMC package-HDInterval
library(HDInterval)  #Package to calculate HPDI interval
library(zoo)
library(lubridate)   #Day Month Year package
library(pea)         #PEA package created by EU PEA team
library(devtools)
library(dplyr)
library(rJava)
library(dbscan)
#install.packages("RJDBC")
#install.packages("/home/dm5578/RHive_2.0-0.2.tar.gz")
library("RJDBC")
library(RHive)
library(DMwR)
#************************************************************************************************************#
#************************************************************************************************************#
#************************************************************************************************************#
hive.class.path = list.files(path=c("/data/pea/HiveJDBC4.jar"),pattern="jar", full.names=T);

cp = c(hive.class.path)
.jinit(classpath=cp,parameters="-Djavax.security.auth.useSubjectCredsOnly=false")
drvhive <- JDBC("com.cloudera.hive.jdbc4.HS2Driver","/data/pea/HiveJDBC4.jar",identifier.quote="`")
conn <- dbConnect(drvhive, "jdbc:hive2://10.127.203.7:10000/cdl", "ni_w", "a123456")
show_databases <- dbGetQuery(conn, "show databases")
#************************************************************************************************************#
#************************************************************************************************************#
#************************************************************************************************************#
Sys.time()
JD_POS_USER <- dbGetQuery(conn, "SELECT tran_date, rpc, user_login_acct, province_name, city_name, parent_order_id, son_order_id, city_level_flag, chanel_id, sale_qty, sale_amount, coupon_amount FROM   edl.fdt_jd_epos_byuser_hot WHERE tran_date BETWEEN '2017-07-01' AND '2019-06-30'")
JD_POS_USER <- data.table(JD_POS_USER)
Sys.time()

setnames(JD_POS_USER, 'tran_date', 'Day')
setnames(JD_POS_USER, 'user_login_acct', 'User_ID')
setnames(JD_POS_USER, 'sale_qty', 'Offtake_Qty')
setnames(JD_POS_USER, 'sale_amount', 'Offtake_Value')
setnames(JD_POS_USER, 'coupon_amount', 'Coupon_Amount')
setnames(JD_POS_USER, 'province_name', 'Province')
setnames(JD_POS_USER, 'city_name', 'City')
setnames(JD_POS_USER, 'parent_order_id', 'Parent_Order')
setnames(JD_POS_USER, 'son_order_id', 'Son_Order')
setnames(JD_POS_USER, 'city_level_flag', 'City_Level')
setnames(JD_POS_USER, 'chanel_id', 'Chanel')

JD_POS_USER[is.Date(Day)               == FALSE, Day           := as.Date(Day)]
JD_POS_USER[is.character(User_ID)      == FALSE, User_ID       := as.character(User_ID)]
JD_POS_USER[is.numeric(Offtake_Qty)    == FALSE, Offtake_Qty   := as.numeric(Offtake_Qty)]
JD_POS_USER[is.numeric(Offtake_Value)  == FALSE, Offtake_Value := as.numeric(Offtake_Value)]
JD_POS_USER[is.numeric(Coupon_Amount)  == FALSE, Coupon_Amount := as.numeric(Coupon_Amount)]
JD_POS_USER[is.character(Province)     == FALSE, Province      := as.character(Province)]
JD_POS_USER[is.character(City)         == FALSE, City          := as.character(City)]
JD_POS_USER[is.character(Parent_Order) == FALSE, Parent_Order  := as.character(Parent_Order)]
JD_POS_USER[is.character(Son_Order)    == FALSE, Son_Order     := as.character(Son_Order)]
JD_POS_USER[is.character(City_Level)   == FALSE, City_Level    := as.character(City_Level)]


SINGLE_MASTER_DATA <- dbGetQuery(conn, "SELECT jd_rpc, pg_bar_code FROM edl.fdt_hb_all_single_product_hot")
SINGLE_MASTER_DATA <- data.table(SINGLE_MASTER_DATA)
setnames(SINGLE_MASTER_DATA, 'jd_rpc'     , 'rpc')
setnames(SINGLE_MASTER_DATA, 'pg_bar_code', 'Barcode')

BUNDLE_MASTER_DATA <- dbGetQuery(conn, "SELECT rpc, pg_bar_code FROM edl.fdt_hb_all_bundle_product_hot")
BUNDLE_MASTER_DATA <- data.table(BUNDLE_MASTER_DATA)
setnames(BUNDLE_MASTER_DATA, 'pg_bar_code', 'Barcode')

MASTER_DATA <- rbindlist(list(SINGLE_MASTER_DATA, BUNDLE_MASTER_DATA), use.names = FALSE, fill = FALSE, idcol = NULL)     
MASTER_DATA = MASTER_DATA[is.na(rpc) == FALSE & rpc != '']

MASTER_DATA[is.character(rpc)     == FALSE, rpc     := as.character(rpc)]
MASTER_DATA[is.numeric(Barcode)   == FALSE, Barcode := as.numeric(Barcode)]
MASTER_DATA[, Barcode := substr(Barcode  + 10^14, 2, 100)]
MASTER_DATA[is.character(Barcode) == FALSE, Barcode := is.character(Barcode)]


Prod_Info  <- dbGetQuery(conn, "SELECT gtin, category_en, brand_en FROM cdl.prod_cmd_product_item_h_new_hot")
Prod_Info  <- data.table(Prod_Info)
Prod_Info  <- Prod_Info[, .(Barcode  = gtin, Category = category_en, Brand = brand_en)]

Prod_Info[is.character(Barcode)          == FALSE, Barcode   := as.character(Barcode)]
Prod_Info[is.character(Category)         == FALSE, Category  := as.character(Category)]
Prod_Info[is.character(Brand)            == FALSE, Brand     := as.character(Brand)]

Prod_Info = Prod_Info[Category != '']
Prod_Info = Prod_Info[!duplicated(Barcode)]


MASTER_DATA = merge(MASTER_DATA, Prod_Info, by = c('Barcode'), all.x = TRUE)
MASTER_DATA = MASTER_DATA[!duplicated(MASTER_DATA[, .(Barcode, rpc, Category, Brand)])]

MASTER_DATA[, RPC_Count_Category := length(unique(Category)), by = 'rpc']
MASTER_DATA[RPC_Count_Category == 2 & is.na(unique(Category)[1]) == TRUE  & is.na(unique(Category)[2]) == FALSE, Category := unique(Category)[2], by = 'rpc']
MASTER_DATA[RPC_Count_Category == 2 & is.na(unique(Category)[1]) == FALSE & is.na(unique(Category)[2]) == TRUE , Category := unique(Category)[1], by = 'rpc']

MASTER_DATA[, RPC_Count_Brand := length(unique(Brand)), by = 'rpc']
MASTER_DATA[RPC_Count_Brand == 2 & is.na(unique(Brand)[1]) == TRUE  & is.na(unique(Brand)[2]) == FALSE, Brand := unique(Brand)[2], by = 'rpc']
MASTER_DATA[RPC_Count_Brand == 2 & is.na(unique(Brand)[1]) == FALSE & is.na(unique(Brand)[2]) == TRUE , Brand := unique(Brand)[1], by = 'rpc']

MASTER_DATA[, RPC_Count_Barcode := length(unique(Barcode)), by = 'rpc']
MASTER_DATA[RPC_Count_Barcode == 2 & is.na(unique(Barcode)[1]) == TRUE  & is.na(unique(Barcode)[2]) == FALSE, Barcode := unique(Barcode)[2], by = 'rpc']
MASTER_DATA[RPC_Count_Barcode == 2 & is.na(unique(Barcode)[1]) == FALSE & is.na(unique(Barcode)[2]) == TRUE , Barcode := unique(Barcode)[1], by = 'rpc']


MASTER_DATA = MASTER_DATA[is.na(Category) == FALSE | (is.na(Category) == TRUE & is.na(Barcode) == FALSE)]
MASTER_DATA[, Category := lr(Category, 'NoCat_InMDM'), by = 'rpc']
MASTER_DATA[, Brand    := lr(Brand   , 'NoBrd_InMDM'), by = 'rpc']
MASTER_DATA[, Barcode  := lr(Barcode , 'NoBcd_InMDM'), by = 'rpc']

MASTER_DATA[, RPC_Count_Category := length(unique(Category)), by = 'rpc']
MASTER_DATA[, RPC_Count_Brand    := length(unique(Brand))   , by = 'rpc']
MASTER_DATA[, RPC_Count_Barcode  := length(unique(Barcode)) , by = 'rpc']

length(MASTER_DATA[RPC_Count_Category == 1]$rpc)/length(MASTER_DATA$rpc)
length(MASTER_DATA[RPC_Count_Category == 2]$rpc)/length(MASTER_DATA$rpc)
length(MASTER_DATA[RPC_Count_Brand == 1   ]$rpc)/length(MASTER_DATA$rpc)
length(MASTER_DATA[RPC_Count_Brand == 2   ]$rpc)/length(MASTER_DATA$rpc)
length(MASTER_DATA[RPC_Count_Barcode == 1 ]$rpc)/length(MASTER_DATA$rpc)
length(MASTER_DATA[RPC_Count_Barcode == 2 ]$rpc)/length(MASTER_DATA$rpc)

MASTER_DATA = MASTER_DATA[!duplicated(rpc)]

JD_POS_USER = merge(JD_POS_USER, MASTER_DATA, by = 'rpc', all.x = TRUE)
JD_POS_USER[is.na(Category) == TRUE, Category := 'NoCat_InMap']
JD_POS_USER[is.na(Brand)    == TRUE, Brand    := 'NoBrd_InMap']


#################################################################
#################################################################
#################################################################
#################################################################
#################################################################
#################################################################
#################################################################
JD_POS_USER = JD_POS_USER[Category == 'PCC'| Category == 'Baby']
#################################################################
#################################################################
#################################################################
#################################################################
#################################################################
#################################################################

# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION ------- Rolling Ave. For Category Qty in 13 Weeks
# FUNCTION ------- Rolling Sum For Category Qty in 53 Weeks
# FUNCTION ------- Rolling Sum For Count Buy in 53 Weeks
Rolling_Func <- function(Data, Select_Week_Data, Select_Week){
  setkey(Data,             'User_ID')
  setkey(Select_Week_Data, 'User_ID')
  # Select One Unique Week and Filter Users (in the week) in Other Weeks
  Week_Sample = Select_Week_Data[Week == Select_Week]
  Exist_User  = unique(Week_Sample$User_ID)
  User_inSelectWeek = as.data.table(Data %>% filter(User_ID %in% Exist_User))
  # Fill 0 Buy For Appearanced Users in All Weeks
  User_Count        = length(Exist_User)  
  Min_Week          = min(Data$Week)
  Max_Week          = max(Data$Week)
  Exist_Week        = seq(from = Min_Week, to = Max_Week, by = 1)
  Length_Exist_Week = length(Exist_Week)
  Week_Set          = data.table(Week = rep(Exist_Week, User_Count), User_ID = rep(Exist_User, each = Length_Exist_Week))
  Week_Set          = merge(Week_Set, User_inSelectWeek, by = c('Week', 'User_ID'), all.x = TRUE)
  # Give 0 Sales/Buy For Filled Users in Each Week
  Week_Set[is.na(Qty_Cat), Qty_Cat := 0]
  Week_Set[is.na(Val_Cat), Val_Cat := 0]
  Week_Set[is.na(Cnt_Buy), Cnt_Buy := 0]  
  # Rolling Function For The Rest Three Featues
  Week_Set[, Rol_Qty_Cat := roll(Qty_Cat, width = 13, FUN = mean, align = 'center', fill = 'complete'), by = 'User_ID']
  Week_Set[, Sum_Qty_Cat := roll(Qty_Cat, width = 53, FUN = sum, align = 'center', fill = 'complete', na.rm = FALSE), by = 'User_ID']
  Week_Set[, Sum_Cnt_Buy := roll(Cnt_Buy, width = 53, FUN = sum, align = 'center', fill = 'complete', na.rm = FALSE), by = 'User_ID']
  # Delete Other Filled Weeks
  Week_Set = Week_Set[Qty_Cat != 0 & Week == Select_Week]
  # Return Columns
  return(Week_Set[, .(Rol_Qty_Cat, Sum_Qty_Cat, Sum_Cnt_Buy)])
}
# FUNCTION ------- Optics Qty & Optics 
# FUNCTION ------- Qty:    Max SKU Qty X Ave Cat Qty X Sum Cat Qty
# FUNCTION ------- QtyCnt: Sun Cnt Buy X Ave Cat Qty X Sum Cat Qty
Optics_Fun <- function(Data, WhichWeek, ColName){
  # Target Which Week
  WeekData = Data[Week == WhichWeek]
  # Max SKU Qty X Ave Cat Qty X Sum Cat Qty
  if(ColName == 'Qty'){
    # EPS + Min Pts + OPTICS
    eps_TH    = floor(max(WeekData$Scale_Rol_Qty_Cat, WeekData$Scale_Sum_Qty_Cat, WeekData$Scale_Max_Qty_SKU)) * 5
    min_Pts_P = max(max(table(WeekData[Max_Qty_SKU >= 48]$Max_Qty_SKU)), 
                    max(table(WeekData[Rol_Qty_Cat >= 1]$Rol_Qty_Cat)), 
                    max(table(WeekData[Sum_Qty_Cat >= 48]$Sum_Qty_Cat)))
    min_Pts   = floor(min(max(min_Pts_P + 1, length(WeekData$User_ID) * 0.001), length(WeekData$User_ID) * 0.05))
    OPTICS    = optics(WeekData[, .(Scale_Max_Qty_SKU, Scale_Rol_Qty_Cat, Scale_Sum_Qty_Cat)], eps = eps_TH, minPts = min_Pts)
    # Reach Dist
    Reachdist = OPTICS$reachdist
    MaxReach  = max(Reachdist[is.infinite(Reachdist) == FALSE])
    # Do Not Want To Wrongly-detect Non-WS Users
    # 600- 600-1200 1200+
    if(length(WeekData[Sum_Qty_Cat       >= 1200]$User_ID) != 0){
      Slope = 1.025        
    }else if(length(WeekData[Sum_Qty_Cat >= 600 ]$User_ID) != 0){
      Slope = 1.050             
    }else{
      Slope = 1.075
    }
    # Make Sure Detected - Do Not Have Like Abnormal Less Sale Stores ;)
    if(MaxReach != 0 ){
      Reachdist[is.infinite(Reachdist)] = MaxReach
      
      Reachdist_TransF = Reachdist
      Reachdist_TransF = Reachdist_TransF/max(Reachdist)
      Reachdist_TransF = Reachdist_TransF*length(Reachdist)
      CutReachdist     = sort(seq(1:(length(Reachdist_TransF) * Slope)), decreasing = T)
      
      CutReachdist     = CutReachdist[1:length(Reachdist)]
      Dist             = CutReachdist-sort(Reachdist_TransF)
      Location         = which.min(abs(Dist))
      Reachdist_TH     = sort(Reachdist_TransF)[Location]/length(Reachdist)*max(Reachdist) # + max(Reachdist)/max(eps_TH, 500)
      # Nonapplied/Tested Code
      # min(CutReachdist)
      # plot(CutReachdist)
      # plot(sort(Reachdist_TransF))
      # lines(CutReachdist)
      res              = extractDBSCAN(OPTICS, eps_cl = Reachdist_TH)
      WeekData[, Column_Diag := res$cluster]
      # Nonapplied/Tested Code      
      # length(WeekData[Sum_Qty_Cat >= 200 & Column_Diag == 1]$User_ID)
      # length(WeekData[Column_Diag == 0])
    }else{
      WeekData[, Column_Diag := 9999]    
    }
    # ############################
    # Do Not Want To Miss WS Users
    # 96-120 120+
    # ############################
    if(length(WeekData[Sum_Qty_Cat   >= 96  & Column_Diag != 0]$User_ID) != 0){
      Slope   = 1.010
      if(length(WeekData[Sum_Qty_Cat >= 120 & Column_Diag != 0]$User_ID) != 0){
        Slope = 1.005        
      }
      Reachdist[is.infinite(Reachdist)] = MaxReach
      
      Reachdist_TransF = Reachdist
      Reachdist_TransF = Reachdist_TransF/max(Reachdist)
      Reachdist_TransF = Reachdist_TransF*length(Reachdist)
      CutReachdist     = sort(seq(1:(length(Reachdist_TransF) * Slope)), decreasing = T)
      
      CutReachdist     = CutReachdist[1:length(Reachdist)]
      Dist             = CutReachdist-sort(Reachdist_TransF)
      Location         = which.min(abs(Dist))
      Reachdist_TH     = sort(Reachdist_TransF)[Location]/length(Reachdist)*max(Reachdist) # + max(Reachdist)/max(eps_TH, 500)
      # Nonapplied/Tested Code
      # min(CutReachdist)
      # plot(CutReachdist)
      # plot(sort(Reachdist_TransF))
      # lines(CutReachdist)
      res              = extractDBSCAN(OPTICS, eps_cl = Reachdist_TH)
      WeekData[, Column_Diag := res$cluster]
    }
    # ####################################
    # Do Not Want To Miss WS Users AGAIN
    # 120-192 192-240 240-288 288-300 300+
    # ####################################
    if(length(WeekData[Sum_Qty_Cat   >= 120 & Column_Diag != 0]$User_ID) != 0){
      Slope   = 1.000
      if(length(WeekData[Sum_Qty_Cat >= 192 & Column_Diag != 0]$User_ID) != 0){
        Slope = 0.9975        
      }
      if(length(WeekData[Sum_Qty_Cat >= 240 & Column_Diag != 0]$User_ID) != 0){
        Slope = 0.995        
      }
      if(length(WeekData[Sum_Qty_Cat >= 288 & Column_Diag != 0]$User_ID) != 0){
        Slope = 0.9925        
      }
      if(length(WeekData[Sum_Qty_Cat >= 300 & Column_Diag != 0]$User_ID) != 0){
        Slope = 0.9900
      }
      Reachdist[is.infinite(Reachdist)] = MaxReach
      
      Reachdist_TransF = Reachdist
      Reachdist_TransF = Reachdist_TransF/max(Reachdist)
      Reachdist_TransF = Reachdist_TransF*length(Reachdist)
      CutReachdist     = sort(seq(1:(length(Reachdist_TransF) * Slope)), decreasing = T)
      
      CutReachdist     = CutReachdist[1:length(Reachdist)]
      Dist             = CutReachdist-sort(Reachdist_TransF)
      Location         = which.min(abs(Dist))
      Reachdist_TH     = sort(Reachdist_TransF)[Location]/length(Reachdist)*max(Reachdist) # + max(Reachdist)/max(eps_TH, 500)
      # Nonapplied/Tested Code
      # min(CutReachdist)
      # plot(CutReachdist)
      # plot(sort(Reachdist_TransF))
      # lines(CutReachdist)
      res              = extractDBSCAN(OPTICS, eps_cl = Reachdist_TH)
      WeekData[, Column_Diag := res$cluster]
    }
    # ########################################
    # Do Not Want To Miss WS Users AGAIN AGAIN
    # 240-300 300+
    # ########################################
    if(length(WeekData[Sum_Qty_Cat   >= 240 & Column_Diag != 0]$User_ID) != 0){
      Slope   = 0.9875
      if(length(WeekData[Sum_Qty_Cat >= 300 & Column_Diag != 0]$User_ID) != 0){
        Slope = 0.9850
      }
      Reachdist[is.infinite(Reachdist)] = MaxReach
      
      Reachdist_TransF = Reachdist
      Reachdist_TransF = Reachdist_TransF/max(Reachdist)
      Reachdist_TransF = Reachdist_TransF*length(Reachdist)
      CutReachdist     = sort(seq(1:(length(Reachdist_TransF) * Slope)), decreasing = T)
      
      CutReachdist     = CutReachdist[1:length(Reachdist)]
      Dist             = CutReachdist-sort(Reachdist_TransF)
      Location         = which.min(abs(Dist))
      Reachdist_TH     = sort(Reachdist_TransF)[Location]/length(Reachdist)*max(Reachdist) # + max(Reachdist)/max(eps_TH, 500)
      # Nonapplied/Tested Code
      # min(CutReachdist)
      # plot(CutReachdist)
      # plot(sort(Reachdist_TransF))
      # lines(CutReachdist)
      res              = extractDBSCAN(OPTICS, eps_cl = Reachdist_TH)
      WeekData[, Column_Diag := res$cluster]
    }
    # Sum Cnt Buy X Ave Cat Qty X Sum Cat Qty
  }else if(ColName == 'QtyCnt'){
    eps_TH    = floor(max(WeekData$Scale_Rol_Qty_Cat, WeekData$Scale_Sum_Qty_Cat, WeekData$Scale_Sum_Cnt_Buy)) * 5 # WeekData$Scale_Max_Qty_SKU, 
    min_Pts_P = max(max(table(WeekData[Sum_Cnt_Buy >= 12]$Sum_Cnt_Buy)), 
                    max(table(WeekData[Rol_Qty_Cat >= 1 ]$Rol_Qty_Cat)), 
                    max(table(WeekData[Sum_Qty_Cat >= 48]$Sum_Qty_Cat))) # , max(table(WeekData[Max_Qty_SKU >= 48]$Max_Qty_SKU))
    min_Pts   = floor(min(max(min_Pts_P + 1, length(WeekData$User_ID) * 0.001), length(WeekData$User_ID) * 0.05))
    OPTICS    = optics(WeekData[, .(Scale_Sum_Cnt_Buy, Scale_Rol_Qty_Cat, Scale_Sum_Qty_Cat)], eps = eps_TH, minPts = min_Pts) #, Scale_Max_Qty_SKU
    
    Reachdist = OPTICS$reachdist
    MaxReach = max(Reachdist[is.infinite(Reachdist) == FALSE])
    # Do Not Want To Wrongly-detect Non-WS Users
    # 600- 600-1200 1200+
    if(length(WeekData[Sum_Qty_Cat       >= 1200]$User_ID) != 0){
      Slope = 1.025        
    }else if(length(WeekData[Sum_Qty_Cat >= 600 ]$User_ID) != 0){
      Slope = 1.050             
    }else{
      Slope = 1.075
    }
    if(MaxReach != 0 ){
      Reachdist[is.infinite(Reachdist)] = MaxReach
      
      Reachdist_TransF = Reachdist
      Reachdist_TransF = Reachdist_TransF/max(Reachdist)
      Reachdist_TransF = Reachdist_TransF*length(Reachdist)
      CutReachdist     = sort(seq(1:(length(Reachdist_TransF) * Slope)), decreasing = T)
      
      CutReachdist     = CutReachdist[1:length(Reachdist)]
      Dist             = CutReachdist-sort(Reachdist_TransF)
      Location         = which.min(abs(Dist))
      Reachdist_TH     = sort(Reachdist_TransF)[Location]/length(Reachdist)*max(Reachdist) # + max(Reachdist)/max(eps_TH, 500)
      
      res              = extractDBSCAN(OPTICS, eps_cl = Reachdist_TH)
      WeekData[, Column_Diag := res$cluster]
    }else{
      WeekData[, Column_Diag := 9999]    
    }
    # ############################
    # Do Not Want To Miss WS Users
    # 96-120 120+
    # ############################
    if(length(WeekData[Sum_Qty_Cat >= 96 & Column_Diag != 0]$User_ID) != 0){
      Slope = 1.010
      if(length(WeekData[Sum_Qty_Cat >= 120 & Column_Diag != 0]$User_ID) != 0){
        Slope = 1.005        
      }
      Reachdist[is.infinite(Reachdist)] = MaxReach
      
      Reachdist_TransF = Reachdist
      Reachdist_TransF = Reachdist_TransF/max(Reachdist)
      Reachdist_TransF = Reachdist_TransF*length(Reachdist)
      CutReachdist     = sort(seq(1:(length(Reachdist_TransF) * Slope)), decreasing = T)
      
      CutReachdist     = CutReachdist[1:length(Reachdist)]
      Dist             = CutReachdist-sort(Reachdist_TransF)
      Location         = which.min(abs(Dist))
      Reachdist_TH     = sort(Reachdist_TransF)[Location]/length(Reachdist)*max(Reachdist) # + max(Reachdist)/max(eps_TH, 500)
      
      res              = extractDBSCAN(OPTICS, eps_cl = Reachdist_TH)
      WeekData[, Column_Diag := res$cluster]
    }
    # ####################################
    # Do Not Want To Miss WS Users AGAIN
    # 120-192 192-240 240-288 288-300 300+
    # ####################################
    if(length(WeekData[Sum_Qty_Cat   >= 120 & Column_Diag != 0]$User_ID) != 0){
      Slope   = 1.000
      if(length(WeekData[Sum_Qty_Cat >= 192 & Column_Diag != 0]$User_ID) != 0){
        Slope = 0.9975        
      }
      if(length(WeekData[Sum_Qty_Cat >= 240 & Column_Diag != 0]$User_ID) != 0){
        Slope = 0.995        
      }
      if(length(WeekData[Sum_Qty_Cat >= 288 & Column_Diag != 0]$User_ID) != 0){
        Slope = 0.9925        
      }
      if(length(WeekData[Sum_Qty_Cat >= 300 & Column_Diag != 0]$User_ID) != 0){
        Slope = 0.9900
      }
      Reachdist[is.infinite(Reachdist)] = MaxReach
      
      Reachdist_TransF = Reachdist
      Reachdist_TransF = Reachdist_TransF/max(Reachdist)
      Reachdist_TransF = Reachdist_TransF*length(Reachdist)
      CutReachdist     = sort(seq(1:(length(Reachdist_TransF) * Slope)), decreasing = T)
      
      CutReachdist     = CutReachdist[1:length(Reachdist)]
      Dist             = CutReachdist-sort(Reachdist_TransF)
      Location         = which.min(abs(Dist))
      Reachdist_TH     = sort(Reachdist_TransF)[Location]/length(Reachdist)*max(Reachdist) # + max(Reachdist)/max(eps_TH, 500)
      
      res = extractDBSCAN(OPTICS, eps_cl = Reachdist_TH)
      WeekData[, Column_Diag := res$cluster]
    }
    # ########################################
    # Do Not Want To Miss WS Users AGAIN AGAIN
    # 240-300 300+
    # ########################################
    if(length(WeekData[Sum_Qty_Cat   >= 240 & Column_Diag != 0]$User_ID) != 0){
      Slope   = 0.9875
      if(length(WeekData[Sum_Qty_Cat >= 300 & Column_Diag != 0]$User_ID) != 0){
        Slope = 0.9850
      }
      Reachdist[is.infinite(Reachdist)] = MaxReach
      
      Reachdist_TransF = Reachdist
      Reachdist_TransF = Reachdist_TransF/max(Reachdist)
      Reachdist_TransF = Reachdist_TransF*length(Reachdist)
      CutReachdist     = sort(seq(1:(length(Reachdist_TransF) * Slope)), decreasing = T)
      
      CutReachdist     = CutReachdist[1:length(Reachdist)]
      Dist             = CutReachdist-sort(Reachdist_TransF)
      Location         = which.min(abs(Dist))
      Reachdist_TH     = sort(Reachdist_TransF)[Location]/length(Reachdist)*max(Reachdist) # + max(Reachdist)/max(eps_TH, 500)
      
      res              = extractDBSCAN(OPTICS, eps_cl = Reachdist_TH)
      WeekData[, Column_Diag := res$cluster]
    }
    # Sum_Cnt_Buy <= 24 不可以 因为有的人就是买的很多次买很少的XX啊...
    WeekData[Sum_Qty_Cat/Sum_Cnt_Buy <= 12 & Sum_Qty_Cat <= 96 & Column_Diag == 0, Column_Diag := 2333]
  }
  # Set Column Name
  Column_New = paste('DiagRes_', ColName, sep = '')
  setnames(WeekData, 'Column_Diag', Column_New)
  # Return OPTICS Results
  return(WeekData)
}
# FUNCTION ------- Apply OPTICS Functions
Week_OPTICS <- function(Data, Week_Selected){
  setkey(Data, 'User_ID')
  WS_Week    = data.table()
  See        = Data[Week == Week_Selected]
  Which_Week = unique(See$Week)
  # Split Users If Counts > 30000
  if(length(See$User_ID) > 30000){
    See     = See[order(Sum_Qty_Cat, decreasing = TRUE)]
    RepTime = floor(length(See$User_ID)/30000)+1  
    See[, Sample := rep(1 : RepTime, floor(length(See$User_ID)/RepTime + 1))[1:length(See$User_ID)]]
    # Execute OPTICS Funtions
    for(i in 1:RepTime){
      See_Sample = See[Sample == i]
      See_Sample = See_Sample[, -c('Sample'), with = FALSE]
      if(max(See_Sample$Sum_Qty_Cat) <= 36){
        See_Sample[, DiagRes_Qty     := 6666]
        See_Sample[, DiagRes_QtyCnt  := 6666]
      }else{
        See_Sample = Optics_Fun(See_Sample, ColName = 'Qty'   , WhichWeek = Which_Week)
        See_Sample = Optics_Fun(See_Sample, ColName = 'QtyCnt', WhichWeek = Which_Week)
      }
      See_Sample$DiagRes_Qty    = as.numeric(See_Sample$DiagRes_Qty)
      See_Sample$DiagRes_QtyCnt = as.numeric(See_Sample$DiagRes_QtyCnt)      
      WS_Week      = rbindlist(list(WS_Week, See_Sample), use.names = FALSE, fill = FALSE, idcol = NULL)
      setkey(WS_Week, 'User_ID')
    }
  }else{
    # Do Not Split Users If Counts <= 30000
    See_Sample = See
    if(max(See_Sample$Sum_Qty_Cat) <= 36){
      See_Sample[, DiagRes_Qty    := 6666]
      See_Sample[, DiagRes_QtyCnt := 6666]
    }else{
      See_Sample = Optics_Fun(See_Sample, ColName = 'Qty'   , WhichWeek = Which_Week)
      See_Sample = Optics_Fun(See_Sample, ColName = 'QtyCnt', WhichWeek = Which_Week)
    }
    See_Sample$DiagRes_Qty    = as.numeric(See_Sample$DiagRes_Qty)
    See_Sample$DiagRes_QtyCnt = as.numeric(See_Sample$DiagRes_QtyCnt)    
    WS_Week      = rbindlist(list(WS_Week, See_Sample), use.names = FALSE, fill = FALSE, idcol = NULL)
    setkey(WS_Week, 'User_ID')  
  }
  # Return Golden Columns
  return(WS_Week[, c('DiagRes_Qty', 'DiagRes_QtyCnt')])
}
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION ------- Rolling Sum_Val_Cat_Year Calculation
Rolling_Func_Val <- function(Data, Select_Week_Data, Select_Week){
  setkey(Data, 'User_ID')
  setkey(Select_Week_Data, 'User_ID')
  # Select One Unique Week And Filter Users (in the week) in Other Weeks
  Week_Sample       = Select_Week_Data[Week == Select_Week]
  Exist_User        = unique(Week_Sample$User_ID)
  User_inSelectWeek = as.data.table(Data %>% filter(User_ID %in% Exist_User))
  # Fill 0 Buy For Appearanced Users in All Weeks
  User_Count        = length(Exist_User)
  Min_Week          = min(Data$Week)
  Max_Week          = max(Data$Week)
  Exist_Week        = seq(from = Min_Week, to = Max_Week, by = 1)
  Length_Exist_Week = length(Exist_Week)
  Week_Set          = data.table(Week = rep(Exist_Week, User_Count), User_ID = rep(Exist_User, each = Length_Exist_Week))
  Week_Set          = merge(Week_Set, User_inSelectWeek, by = c('Week', 'User_ID'), all.x = TRUE)
  # Give 0 Sales Value For Filled Users in Each Week
  Week_Set[is.na(Val_Cat), Val_Cat := 0]
  # Rolling Function For Value Feature
  Week_Set[, Sum_Val_Cat := roll(Val_Cat, width = 53, FUN = sum, align = 'center', fill = 'complete', na.rm = FALSE), by = 'User_ID']
  # Delete Other Filled Weeks
  Week_Set          = Week_Set[Val_Cat != 0 & Week == Select_Week]
  # Return The Column
  return(Week_Set[, .(Sum_Val_Cat)])
}
# FUNCTION ------- LOF
LOF_Func <- function(Data, AB, Cat, Price_Normal, Price_Abnormal){
  # Select Detected/Non-Detected Group
  Group = Data[abnormal == AB]
  # Apply LOF To Group
  Rep   = ceiling(length(Group$User_ID)/10000) 
  setkey(Group, 'Max_Sum_Qty_Cat_Year')
  Group[, Sample_Q := rep(1:Rep, ceiling(length(Group$User_ID)/Rep))[1: length(Group$User_ID)]]
  Group = Group[Sample_Q == 1]
  K     = median(c(max(table(Group$Max_Sum_Qty_Cat_Year))* 2, max(table(Group$Max_Sum_Qty_Cat_Year)), floor(length(Group$User_ID) * 0.1)))
  Group[, Res_Qty := lofactor(Max_Sum_Qty_Cat_Year, K)]
  # Abnormal Or Normal
  if(AB == 0){
    Q_TH = median(c(max(Group[Res_Qty <= 1]$Max_Sum_Qty_Cat_Year), 71, 83))
    Threshold_Coef = ifelse((Cat == 'Fem' | Cat == 'Baby'), 2, 1)
    Q_TH           = Q_TH * Threshold_Coef
    Threshold_Coef = ifelse((Cat == 'Skin' | Cat == 'Baby'), 4, 1)
    V_TH           = median(c(Price_Normal * Q_TH, 4500 * Threshold_Coef, 9000 * Threshold_Coef))
    return(c(Q_TH, V_TH))
  }else if(AB == 1){
    Q_TH_De        = median(c(min(Group[Res_Qty <= 1]$Max_Sum_Qty_Cat_Year), 47, 59))
    Threshold_Coef = ifelse((Cat == 'Fem' | Cat == 'Baby'), 2, 1)
    Q_TH_De        = Q_TH_De * Threshold_Coef
    Threshold_Coef = ifelse((Cat == 'Skin' | Cat == 'Baby'), 4, 1)
    V_TH_De        = median(c(Price_Abnormal * Q_TH_De, 1500 * Threshold_Coef, 3000 * Threshold_Coef))
    return(c(Q_TH_De, V_TH_De))
  }
}
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION

# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION ------- Most Frequent Price
Freq_Price_Func <- function(Num_of_Price){
  if(length(unique(round(Num_of_Price, 2))) == 1){
    Freq_Price = unique(round(Num_of_Price, 2))
  }else{
    Freq_Price = round(as.numeric(data.table(sort(table(round(Num_of_Price, 2)), decreasing = TRUE))[1,1]), 2)
  }  
  return(as.numeric(Freq_Price))
}
# FUNCTION ------- Max Price with Appearance Time > 3
Sec_Max_Price_Func <- function(Num_of_Price){
  if(length(unique(round(Num_of_Price, 2))) == 1){
    Sec_Max_Price = unique(round(Num_of_Price, 2))
  }else{
    Candidate = data.table(sort(table(round(Num_of_Price, 2)), decreasing = TRUE))
    C = data.table(Num_of_Price = as.numeric(as.vector(unlist(Candidate[, 1]))), N = as.numeric(as.vector(unlist(Candidate[, 2]))))
    if(max(C$N) <3){
      Sec_Max_Price = round(max(C$Num_of_Price), 2)
    }else{
      Sec_Max_Price = round(max(C[N >= 3]$Num_of_Price), 2)
    }
  }
  return(as.numeric(Sec_Max_Price))
}
# FUNCTION ------- Fill 0 Sales on Non-sales Days
Fill_Missing_Day_Func <- function(Price_Table, RPC){
  Daily_Sale       = Price_Table[rpc == RPC]
  Min_Day          = min(Daily_Sale$Day)
  Max_Day          = max(Daily_Sale$Day)
  Exist_Day        = seq(from = Min_Day, to = Max_Day, by = 1)
  Length_Exist_Day = length(Exist_Day)
  Day_Set          = data.table(rpc = rep(RPC, Length_Exist_Day), Day = Exist_Day)  
  return(Day_Set)
}
# FUNCTION ------- Fill 0 Sales on Non-Sales Days For All SKU
Fill_Price_Day_Func <- function(NonFill_Table){
  Price_Day_Fill = data.table()
  for(i in 1 : length(unique(NonFill_Table$rpc))){
    # Un-used Code
    # print(paste(i, '/',length(unique(NonFill_Table$Barcode)), sep = ''))
    RPC_In         = sort(unique(NonFill_Table$rpc))[i]
    Table          = Fill_Missing_Day_Func(Price_Table = NonFill_Table, RPC = RPC_In)  
    Price_Day_Fill = rbindlist(list(Price_Day_Fill, Table), use.names = FALSE, fill = FALSE, idcol = NULL)
  }
  Price_Day_Fill = merge(Price_Day_Fill, NonFill_Table, by = c('rpc', 'Day'), all.x = TRUE)
  # Left/Right Fill Highest Price
  Price_Day_Fill[, Highest_Price_Day := lr(Highest_Price_Day, -9999), by ='rpc']
  # Return Filled Table
  return (Price_Day_Fill)
}
# FUNCTION ------- Define Base Incr Function
Base_Incr_Func <- function(Data){
  # Calculate Daily Shelf Sales & Daily Promo Sales
  Data[Discount <= 0.05, Daily_Shelf_Value := Offtake_Value_AfterCoupon]
  Data[Discount <= 0.05, Daily_Shelf_Qty   := Offtake_Qty]
  Data[Discount <= 0.05, Daily_Promo_Value := 0]
  Data[Discount <= 0.05, Daily_Promo_Qty   := 0]
  Data[Discount >  0.05, Daily_Shelf_Value := 0]
  Data[Discount >  0.05, Daily_Shelf_Qty   := 0]
  Data[Discount >  0.05, Daily_Promo_Value := Offtake_Value_AfterCoupon]
  Data[Discount >  0.05, Daily_Promo_Qty   := as.numeric(Offtake_Qty)]
  # Create WS Columns
  Data[abnormal == 1, Daily_Shelf_Value := 0]
  Data[abnormal == 1, Daily_Shelf_Qty   := 0]
  Data[abnormal == 1, Daily_Promo_Value := 0]
  Data[abnormal == 1, Daily_Promo_Qty   := 0]
  Data[abnormal == 0, Daily_WS_Value    := 0]
  Data[abnormal == 0, Daily_WS_Qty      := 0]
  Data[abnormal == 1, Daily_WS_Value    := Offtake_Value_AfterCoupon]
  Data[abnormal == 1, Daily_WS_Qty      := as.numeric(Offtake_Qty)]
  # Calculate Shelf and Promotion in SKU(RPC) & Day Level
  Data_Base_Incr = Data[, .(Day, rpc, Offtake_Qty, Offtake_Value_AfterCoupon, 
                            Daily_Shelf_Value, Daily_Shelf_Qty, Daily_Promo_Value, Daily_Promo_Qty, Daily_WS_Value, Daily_WS_Qty)]
  Data_Base_Incr[, Sum_Offtake_Qty               := sum(Offtake_Qty),               by = c('rpc', 'Day')]
  Data_Base_Incr[, Sum_Offtake_Value_AfterCoupon := sum(Offtake_Value_AfterCoupon), by = c('rpc', 'Day')]
  Data_Base_Incr[, Sum_Daily_Shelf_Value         := sum(Daily_Shelf_Value),         by = c('rpc', 'Day')]
  Data_Base_Incr[, Sum_Daily_Shelf_Qty           := sum(Daily_Shelf_Qty),           by = c('rpc', 'Day')]
  Data_Base_Incr[, Sum_Daily_Promo_Value         := sum(Daily_Promo_Value),         by = c('rpc', 'Day')]
  Data_Base_Incr[, Sum_Daily_Promo_Qty           := sum(Daily_Promo_Qty),           by = c('rpc', 'Day')]
  Data_Base_Incr[, Sum_Daily_WS_Value            := sum(Daily_WS_Value),            by = c('rpc', 'Day')]
  Data_Base_Incr[, Sum_Daily_WS_Qty              := sum(Daily_WS_Qty),              by = c('rpc', 'Day')]
  Data_Base_Incr = Data_Base_Incr[, -c('Offtake_Qty', 'Offtake_Value_AfterCoupon', 
                                       'Daily_Shelf_Value', 'Daily_Shelf_Qty', 
                                       'Daily_Promo_Value', 'Daily_Promo_Qty', 'Daily_WS_Value', 'Daily_WS_Qty'), with = FALSE]
  Data_Base_Incr = Data_Base_Incr[!duplicated(Data_Base_Incr[, .(Day, rpc)])]
  # Calculate Base & Incremental
  Data_Base_Incr[, Weekday := format(Day, '%a')]
  setkey(Data_Base_Incr, Day)
  Data_Base      = Data_Base_Incr[Sum_Daily_Shelf_Qty != 0, .(Day, rpc, Sum_Daily_Shelf_Value, Sum_Daily_Shelf_Qty, Weekday)]
  Data_Base[, Base_Value      := round(roll(Sum_Daily_Shelf_Value, width = 13, align = 'center', fill = 'complete'), 4), by = c('rpc', 'Weekday')]
  Data_Base[, Base_Qty        := round(roll(Sum_Daily_Shelf_Qty, width = 13, align = 'center', fill = 'complete'), 4), by = c('rpc', 'Weekday')]
  Data_Base                    = Data_Base[, .(Day, rpc, Base_Value, Base_Qty)]
  Data_Base_Incr               = merge(Data_Base_Incr, Data_Base, by = c('rpc', 'Day'), all.x = TRUE)
  Data_Base_Incr[, Base_Value := lr(Base_Value, 0), by = c('rpc', 'Weekday')]
  Data_Base_Incr[, Base_Qty   := lr(Base_Qty, 0)  , by = c('rpc', 'Weekday')]
  Data_Base_Incr[, Incr_Value := Sum_Offtake_Value_AfterCoupon - Sum_Daily_WS_Value - Base_Value]
  Data_Base_Incr[, Incr_Qty   := Sum_Offtake_Qty               - Sum_Daily_WS_Qty   - Base_Qty]
  Data_Base_Incr[, WS_Value   := Sum_Daily_WS_Value]
  Data_Base_Incr[, WS_Qty     := Sum_Daily_WS_Qty]
  # Return
  return(Data_Base_Incr)
}
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
# FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION FUNCTION
FCT <- function(Catee){
  Data             = JD_POS_USER[Category == Catee]
  Data_ForBaseIncr = Data
  rm(JD_POS_USER)
  # Define Ongoing Or Initial Time Period
  Data             = Data[, .(Barcode, Day, User_ID, Offtake_Qty, Offtake_Value_AfterCoupon = Offtake_Value - Coupon_Amount, Category)]
  Data[is.character(Barcode)                  == FALSE, Barcode                   := as.character(Barcode)]
  Data[is.Date(Day)                           == FALSE, Day                       := as.Date(Day)]
  Data[is.character(User_ID)                  == FALSE, User_ID                   := as.character(User_ID)]
  Data[is.numeric(Offtake_Qty)                == FALSE, Offtake_Qty               := as.numeric(Offtake_Qty)]
  Data[is.numeric(Offtake_Value_AfterCoupon)  == FALSE, Offtake_Value_AfterCoupon := as.numeric(Offtake_Value_AfterCoupon)]
  Data[is.character(Category)                 == FALSE, Category                  := as.character(Category)]
  setkey(Data, Day)
  setnames(Data, 'Offtake_Value_AfterCoupon', 'Offtake_Value')
  # Give Initial Data
  Data_I = Data
  # Data Quality Check and Fix
  Data[Offtake_Value == 0 & Offtake_Qty != 0, Offtake_Qty     := 0]
  Data[Offtake_Value != 0 & Offtake_Qty == 0, Offtake_Value   := 0] 
  Data = Data[Offtake_Value > 0 & Offtake_Qty >= 0]
  # Slice the Week
  MinDay = min(Data$Day)
  Data[, Week := as.numeric(floor((Day - MinDay)/7) + 1)]
  setkey(Data, 'Week')
  # FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING
  # FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING
  # FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING
  # Feature Prepare ------- Max SKU Offtake Qty/Value in One Year (Ave.)
  TTL_Week = max(Data$Week)
  Data[, Sum_Qty_SKU := round(53 * sum(Offtake_Qty)/TTL_Week),      by = c('User_ID', 'Barcode') ]
  Data[, Sum_Val_SKU := round(53 * sum(Offtake_Value)/TTL_Week, 2), by =c('User_ID', 'Barcode') ]
  Data[, Max_Qty_SKU := max(Sum_Qty_SKU),                           by = 'User_ID']
  Data[, Max_Val_SKU := max(Sum_Val_SKU),                           by = 'User_ID']
  # Feature Prepare ------- Frequency Buy/Category Offtake Qty/Value in One Week
  Data[, Cnt_Buy     := length(unique(Day)), by = c('User_ID', 'Week')]
  Data[, Qty_Cat     := sum(Offtake_Qty),    by =c('User_ID', 'Week') ]
  Data[, Val_Cat     := sum(Offtake_Value),  by =c('User_ID', 'Week') ]
  # Prepare for Feature Engineering - Frequency Buy and Category Feature
  Data = Data[!duplicated(Data[, c('User_ID', 'Week')])]
  Data = Data[, -c('Day', 'Offtake_Qty', 'Offtake_Value', 'Sum_Qty_SKU', 'Sum_Val_SKU', 'Barcode'), with = FALSE]
  setkey(Data, 'Week')
  # FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING
  # FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING
  # FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING FEATURE ENGINEERING
  # MUST
  setkey(Data, 'User_ID')
  Data[, c('Rol_Qty_Cat', 'Sum_Qty_Cat', 'Sum_Cnt_Buy') := Rolling_Func(Data = Data, Select_Week_Data = Data, Select_Week = Week), by = c('Week')]
  # Set Scaled Features
  Data[, Scale_Max_Qty_SKU := round(scale(Max_Qty_SKU, center = FALSE), 5), by = c('Week')]
  Data[, Scale_Rol_Qty_Cat := round(scale(Rol_Qty_Cat, center = FALSE), 5), by = c('Week')]
  Data[, Scale_Sum_Qty_Cat := round(scale(Sum_Qty_Cat, center = FALSE), 5), by = c('Week')]
  Data[, Scale_Sum_Cnt_Buy := round(scale(Sum_Cnt_Buy, center = FALSE), 5), by = c('Week')]
  # MUST
  setkey(Data, 'User_ID')
  Data[, c('DiagQty', 'DiagQtyCnt') := Week_OPTICS(Data = Data, Week_Selected = Week), by = 'Week']
  #Multi_PATH1 = c('/data/tmp/fact_pos_sale_daily/')
  #Multi_PATH2 = c('_Multi.csv')
  #Multi_PATH  = paste(Multi_PATH1, Catee, Multi_PATH2, sep = "", collapse = "")
  #fwrite(Data, Multi_PATH) 
  ##################################################################################################################
  
  ##################################################################################################################
  Data = Data[, .(User_ID, Category, Qty_Cat, Val_Cat, Sum_Qty_Cat, DiagQty, DiagQtyCnt)]
  # Define Abnormal Users
  Abnormal_Users = data.table(User_ID = unique(Data[DiagQty == 0 | DiagQtyCnt == 0]$User_ID), abnormal = 1)
  Data = merge(Data, Abnormal_Users, by ='User_ID', all.x = TRUE)
  Data[is.na(abnormal) == TRUE, abnormal := 0]
  # Define Max Sum Qty Bought in One Year
  Data[, Max_Sum_Qty_Cat_Year := max(Sum_Qty_Cat), by = 'User_ID']
  # Whole Time Period Buy
  Data[, Whole_Cat_Qty := sum(Qty_Cat), by = 'User_ID']
  Data[, Whole_Cat_Val := sum(Val_Cat), by = 'User_ID']
  Data = Data[!duplicated(User_ID)]
  Data = Data[, -c('Qty_Cat', 'Val_Cat', 'Sum_Qty_Cat', 'DiagQty', 'DiagQtyCnt'), with = FALSE]
  Data[, Price := Whole_Cat_Val/Whole_Cat_Qty]
  # Filter Candidate - Abnormal Users & Quantile 0.99 Users
  # And Also, Only Calculate Max_Sum_Val_Cat_Year For Them
  Threshold_Coef = ifelse((Catee == 'Skin' | Catee == 'Baby'), 2, 1)
  Threshold      = min(max(quantile(Data$Whole_Cat_Val, 0.99), 500 * Threshold_Coef), 3000 * Threshold_Coef)
  Candidate      = Data[abnormal == 1 | Whole_Cat_Val >= Threshold, .(User_ID, abnormal)]
  # 这一步是为了得到一年里每个用户的总购买量（Rolling一年内的总合最大）Max_Sum_Val_Cat_Year
  # 计算Max_Sum_Val_Cat_Year的目的是算Max_Sum_Qty_Cat_Year * Price
  # Load Data and Filter The Date
  Data_Initial = Data_I
  Data_Initial = merge(Data_Initial, Candidate, by = 'User_ID')
  # Data Quality Check and Fix
  Data_Initial[Offtake_Value == 0 & Offtake_Qty != 0, Offtake_Qty     := 0]
  Data_Initial[Offtake_Value != 0 & Offtake_Qty == 0, Offtake_Value   := 0] 
  Data_Initial = Data_Initial[Offtake_Value > 0 & Offtake_Qty >= 0]
  # Slice the Week
  MinDay = min(Data_Initial$Day)
  Data_Initial[, Week := as.numeric(floor((Day - MinDay)/7) + 1)]
  Data_Initial[, Val_Cat := sum(Offtake_Value), by =c('User_ID', 'Week') ]
  Data_Initial = Data_Initial[!duplicated(Data_Initial[, c('User_ID', 'Week')])]
  Data_Initial = Data_Initial[, -c('Day', 'Offtake_Value', 'Offtake_Qty', 'abnormal'), with = FALSE]
  setkey(Data_Initial, 'Week')
  # MUST
  setkey(Data_Initial, 'User_ID')
  Data_Initial[, c('Sum_Val_Cat') := Rolling_Func_Val(Data = Data_Initial, Select_Week_Data = Data_Initial, Select_Week = Week), by = c('Week')]
  # Define Max Sum Value Bought in One Year For Candidates
  Data_Initial[, Max_Sum_Val_Cat_Year := max(Sum_Val_Cat), by = 'User_ID']
  Data_Initial = Data_Initial[!duplicated(User_ID)]
  # The Length Of This Candidate Set Is The Same As Previous Candidate Length - Just Give Max_Sum_Val_Cat_Year
  Candidate = Data_Initial[, .(User_ID, Max_Sum_Val_Cat_Year)]
  # Calculate Price For Normal And Abnormal
  Price_Normal_Candidate   = sum(Data[abnormal == 0 & Whole_Cat_Val >= Threshold]$Whole_Cat_Val)/sum(Data[abnormal == 0 & Whole_Cat_Val >= Threshold]$Whole_Cat_Qty)
  Price_Abnormal_Candidate = sum(Data[abnormal == 0 & Whole_Cat_Val <  Threshold]$Whole_Cat_Val)/sum(Data[abnormal == 0 & Whole_Cat_Val <  Threshold]$Whole_Cat_Qty)
  Price_Abnormal           = min(Price_Normal_Candidate, Price_Abnormal_Candidate)
  Price_Normal             = max(Price_Normal_Candidate, Price_Abnormal_Candidate)
  # Filter Candidate Data - Why Here? Because We Need To Calculate Price Based on All Data
  Data                     = Data[abnormal == 1 | Whole_Cat_Val >= Threshold]
  Missed_Abnormal          = LOF_Func(Data = Data, AB = 0, Cat = Catee, Price_Normal   = Price_Normal  )
  Wrongly_Detected_Normal  = LOF_Func(Data = Data, AB = 1, Cat = Catee, Price_Abnormal = Price_Abnormal)
  # Give Data Max_Sum_Val_Year - Why Here? Because We Have Filtered Abnormal & Q 99 Already
  Data = merge(Data, Candidate, by = 'User_ID', all.x = TRUE)
  User = Data[, .(User_ID, abnormal, Max_Sum_Qty_Cat_Year, Max_Sum_Val_Cat_Year)]
  ##################################################################################################################
  
  ##################################################################################################################
  Data = Data_I
  # Map With User (Abnormal Or Q 99) With Max_Sum_Qty_Cat_Year & Max_Sum_Val_Cat_Year
  Data = merge(Data, User, by = 'User_ID', all.x = TRUE)
  # Equipped Data With Abnormal & Max_Sum_Qty_Cat_Year & Max_Sum_Val_Cat_Year - Abnormal & Q 99
  # Other Data Needs To Be Filled With These Three Columns
  Data[is.na(abnormal)            , abnormal := 0]
  Data[is.na(Max_Sum_Qty_Cat_Year), Max_Sum_Qty_Cat_Year := 0]
  Data[is.na(Max_Sum_Val_Cat_Year), Max_Sum_Val_Cat_Year := 0]
  # Newly Defined Abnormal & Normal Based on LOF
  Data[abnormal == 0 & Max_Sum_Qty_Cat_Year >= Missed_Abnormal[1]         & Max_Sum_Val_Cat_Year >= Missed_Abnormal[2]        , abnormal := 1]
  Data[abnormal == 1 & Max_Sum_Qty_Cat_Year <= Wrongly_Detected_Normal[1] & Max_Sum_Val_Cat_Year <= Wrongly_Detected_Normal[2], abnormal := 0]
  # Newly Defined Abnormal & Normal Based on Rule
  Threshold_Coef = ifelse((Catee == 'Skin' | Catee == 'Baby'), 4, 1)
  TH_SK2         = ifelse(Catee == 'Prestige', 15, 1)
  Data[abnormal == 0 & Max_Sum_Val_Cat_Year >= 10000 * Threshold_Coef & Max_Sum_Qty_Cat_Year >= 60, abnormal := 1]
  Threshold_Coef = ifelse((Catee == 'Skin' | Catee == 'Baby'), 2, 1)
  TH_SK2         = ifelse(Catee == 'Prestige', 15, 1)
  Data[abnormal == 1 & Max_Sum_Val_Cat_Year <= 4000 * Threshold_Coef  & Max_Sum_Qty_Cat_Year <= 48, abnormal := 0]
  # Abnormal User List
  Abnormal_User = data.table(User_ID = unique(Data[abnormal == 1]$User_ID), abnormal = 1)
  AB_User_PATH1 = c('/data/tmp/fact_pos_sale_daily/')
  AB_User_PATH2 = c('_Abnormal_Users.csv')
  AB_User_PATH  = paste(AB_User_PATH1, Catee, AB_User_PATH2, sep = "", collapse = "")
  fwrite(Abnormal_User, AB_User_PATH)    
  
  rm(Data)
  rm(Data_I)
  rm(Data_Initial)
  
  # Base Incremental WS
  Data = Data_ForBaseIncr
  rm(Data_ForBaseIncr)
  setkey(Data, Day)
  Data[is.character(rpc)                == FALSE, rpc                := as.character(rpc)]
  Data[is.character(Barcode)            == FALSE, Barcode            := as.character(Barcode)]
  Data[is.Date(Day)                     == FALSE, Day                := as.Date(Day)]
  Data[is.character(User_ID)            == FALSE, User_ID            := as.character(User_ID)]
  Data[is.numeric(Offtake_Qty)          == FALSE, Offtake_Qty        := as.numeric(Offtake_Qty)]
  Data[is.numeric(Offtake_Value)        == FALSE, Offtake_Value      := as.numeric(Offtake_Value)]
  Data[is.numeric(Coupon_Amount)        == FALSE, Coupon_Amount      := as.numeric(Coupon_Amount)]
  Data[is.character(Category)           == FALSE, Category           := as.character(Category)]
  Data[is.character(Brand)              == FALSE, Brand              := as.character(Brand)]
  Data[is.character(Province)           == FALSE, Province           := as.character(Province)]
  Data[is.character(City)               == FALSE, City               := as.character(City)]
  Data[is.character(Parent_Order)       == FALSE, Parent_Order       := as.character(Parent_Order)]
  Data[is.character(Son_Order)          == FALSE, Son_Order          := as.character(Son_Order)]
  Data[is.character(City_Level)         == FALSE, City_Level         := as.character(City_Level)]
  Data[is.character(Chanel)             == FALSE, Chanel             := as.character(Chanel)]
  Data[is.character(RPC_Count_Category) == FALSE, RPC_Count_Category := as.character(RPC_Count_Category)]
  Data[is.character(RPC_Count_Brand)    == FALSE, RPC_Count_Brand    := as.character(RPC_Count_Brand)]
  Data[is.character(RPC_Count_Barcode)  == FALSE, RPC_Count_Barcode  := as.character(RPC_Count_Barcode)]
  
  # Read Abnormal Users
  Abnormal_User_List = Abnormal_User
  Abnormal_User_List[is.character(User_ID) == FALSE, User_ID  := as.character(User_ID)]
  Abnormal_User_List[is.numeric(abnormal)  == FALSE, abnormal := as.numeric(abnormal)]
  # Fix Data Quality
  Data = Data[Offtake_Value > 0 & Offtake_Qty > 0]
  # Calculate Price
  Data[, Price := round(Offtake_Value/Offtake_Qty, 2)]   
  # Take Highest Price on The Same Day Amoung Diffrent Users/Orders
  Data[, Highest_Price_Day := floor(max(Price)), by = c('rpc', 'Day')]
  # Fill 0 Sales on Non-sales Days
  Price_Day_NonFill = Data[, .(rpc, Day, Highest_Price_Day)]
  Price_Day_NonFill = Price_Day_NonFill[!duplicated(Price_Day_NonFill[, .(Day, rpc)])]
  setkey(Price_Day_NonFill, Day)
  Price_Day_Fill = Fill_Price_Day_Func(NonFill_Table = Price_Day_NonFill)
  # Three Different Kinds of Rolling Functions for Selecting Shelf Price
  # Back Up Code
  # Price_Day_Fill[, Max_Price_Test := roll(Highest_Price_Day, 90, FUN = max, align = 'right', fill = 'partial'), by = 'rpc']
  # Price_Day_Fill[, Freq_Price_Test := lmRoll(Highest_Price_Day, 90, FUN = Freq_Price_Func, align = 'right', fill = 'partial'), by = 'rpc']
  Period = 360/2
  if(Catee == 'Hair'){
    Period = floor(365/1.8)
  }
  if(Catee == 'Oral'){
    Period = floor(365/1.7)
  }
  if(Catee == 'PCC'){
    Period = floor(365/1.8)
  }
  if(Catee == 'Fabric' | Catee == 'Home'){
    Period = floor(365/1.6)
  }
  if(Catee == 'Fem'){
    Period = floor(365/2.5)
  }
  if(Catee == 'Baby'){
    Period = floor(365/2.6)
  }
  if(Catee == 'Shave'){
    Period = floor(365/1.4)
  }
  if(Catee == 'Skin' | Catee == 'Prestige'){
    Period = floor(365/2.3)
  }
  Price_Day_Fill[, Estimated_Shelf_Price := lmRoll(Highest_Price_Day, Period, FUN = Sec_Max_Price_Func, align = 'right', fill = 'partial'), by = 'rpc']
  # Give Shelf Price
  Price_Day_Fill = Price_Day_Fill[, -c('Highest_Price_Day'), with = FALSE]
  Data           = Data[          , -c('Highest_Price_Day'), with = FALSE]
  Data           = merge(Data, Price_Day_Fill, by = c('Day', 'rpc'), all.y = TRUE)
  # Give After Coupon Value
  Data[, Offtake_Value_AfterCoupon := Offtake_Value - Coupon_Amount]
  Data = Data[, -c('Offtake_Value'), with = FALSE]
  # Clean Up The Data
  Data[Offtake_Value_AfterCoupon == 0 & Offtake_Qty != 0, Offtake_Qty               := 0]
  Data[Offtake_Value_AfterCoupon != 0 & Offtake_Qty == 0, Offtake_Value_AfterCoupon := 0] 
  # Because Of NA - Cannot Use - Data = Data[Offtake_Value_AfterCoupon > 0 & Offtake_Qty >0] Only
  # Data[Barcode == '6953581600067' & Day == '2018-11-11'] After Coupon Value < 0
  Data[Offtake_Value_AfterCoupon < 0, Offtake_Qty               := 0]
  Data[Offtake_Value_AfterCoupon < 0, Offtake_Value_AfterCoupon := 0]
  # Give After Coupon Price
  Data[                      , Price                     := round(Offtake_Value_AfterCoupon/Offtake_Qty, 2)]
  Data[is.nan(Price)  == TRUE, Price                     := Estimated_Shelf_Price] # Negative Money = No Need Money => Discount = 0
  Data[is.na(User_ID) == TRUE, Price                     := Estimated_Shelf_Price]
  Data[is.na(User_ID) == TRUE, Offtake_Qty               := 0]
  Data[is.na(User_ID) == TRUE, Coupon_Amount             := 0]
  Data[is.na(User_ID) == TRUE, Offtake_Value_AfterCoupon := 0]
  # Map Abnormal Users
  Data = merge(Data, Abnormal_User_List, by = c('User_ID'), all.x = TRUE)
  Data[is.na(abnormal) == TRUE, abnormal := 0]
  # Calculate Discount and Base & Incr & WS
  Data[             , Discount := round((Estimated_Shelf_Price - Price)/Estimated_Shelf_Price, 4)]
  Data[Discount <= 0, Discount := 0]
  Data_Base_Incr = Base_Incr_Func(Data = Data)
  Data = merge(Data, Data_Base_Incr[, .(Day, rpc, Weekday, Base_Value, Base_Qty, Incr_Value, Incr_Qty, WS_Value, WS_Qty)],
               by = c('Day', 'rpc'), all.x = TRUE)
  
  Data_PATH1 = c('/data/tmp/fact_pos_sale_daily/')
  Data_PATH2 = c('_BPW_JD.csv')
  BPW_PATH   = paste(Data_PATH1, Catee, Data_PATH2, sep = "", collapse = "")
  fwrite(Data, BPW_PATH)        
}

############################################ gc 释放部分内存 ##################################################
gc()
############################################ gc 释放部分内存 ##################################################

library(parallel)
detectCores()

cl <- makeCluster(getOption('cl.cores', 2)) # 初始化3核心集群

clusterEvalQ(cl,library(data.table)) #添加并行计算中用到的包
clusterEvalQ(cl,library(coda))
clusterEvalQ(cl,library(HDInterval))
clusterEvalQ(cl,library(zoo))
clusterEvalQ(cl,library(lubridate))
clusterEvalQ(cl,library(pea))
clusterEvalQ(cl,library(devtools))
clusterEvalQ(cl,library(dplyr))
clusterEvalQ(cl,library(rJava))
clusterEvalQ(cl,library(dbscan))
clusterEvalQ(cl,library(DMwR))
# clusterExport(cl,'Category_Name_Flag') #添加并行计算中用到的环境变量（如当前上下文中定义的方法）
clusterExport(cl,'JD_POS_USER')
clusterExport(cl,'Rolling_Func')
clusterExport(cl,'Optics_Fun')
clusterExport(cl,'Week_OPTICS')
clusterExport(cl,'Rolling_Func_Val')
clusterExport(cl,'LOF_Func')
clusterExport(cl,'Freq_Price_Func')
clusterExport(cl,'Sec_Max_Price_Func')
clusterExport(cl,'Fill_Missing_Day_Func')
clusterExport(cl,'Fill_Price_Day_Func')
clusterExport(cl,'Base_Incr_Func')
clusterExport(cl,'FCT')
rm(JD_POS_USER)
system.time({parLapply(cl, c('PCC', 'Baby'), FCT)})
stopCluster(cl)