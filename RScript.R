#this is the Rscript code that gets run in the algorithm
#all mysql code will be included here, as that is where it is run in the algo

library(healthcareai)
library(RMySQL)

mydb = dbConnect(MySQL(), user='root', password='', dbname='stocks', host='localhost')

dbSendQuery(mydb, "drop table stocks.outputpreds;
"
)

dbSendQuery(mydb, "drop table staging.Pycharmcombine
"
)

dbSendQuery(mydb, "create table staging.Pycharmcombine as 
select staging.pycharm_output.*, fearandgreedindexv1.fearandgreed
from staging.pycharm_output join staging.fearandgreedindexv1 on fearandgreedindexv1.MyDate=staging.pycharm_output.mydate
where staging.pycharm_output.mydate > '2023-01-01';"
)

dbSendQuery(mydb, "drop table staging.finalbeforetrain;
"
)

#futureclose represents whichever column I chose to calculate from
#x represents the percent change value I am planning to predict. Ex. twodayclose, fivedayclose
# there is typically many of these case statements
# I typically run multiple models with different targets

dbSendQuery(mydb, "create table staging.finalbeforetrain as
Select 
fearandgreed, tech1, tech2, tech3, etc, 
ticker, MyDate, (futureclose-c)/c*100 percentchange,
case 
when (futureclose-c)/c*100 > x then 'Y' else 'N' 
end yesnotomorrowBuyYNv1,
 c, l, h, o from staging.Pycharmcombine
 where mydate > '2023-01-01';
"
)

#I also run the two previous mysql queries when getting training data, but in mysql and without the mydate parameter

quick_models <-load_models(file = "C:/Users/wend0/OneDrive/Documents/Stocks/Models/Examplemodel.RDS")




rs = dbSendQuery(mydb, "select
ticker
, MyDate
, tech1
, tech2
, tech3
, etc
, PercentChange
, yesno
from staging.Modeltraindata
where mydate >= '2023-01-01'
and ticker is not null
and yesno is not null"
)

data = fetch(rs, n=-1)

predictions <- predict(quick_models, data)
filename <- paste0("C:\\Users\\wend0\\OneDrive\\Documents\\Stocks\\DataFiles\\file", Sys.Date(), ".csv", row.names = FALSE)
#write the file out as a log
write.csv(predictions,filename, row.names = FALSE)
#get the file back as a clean dataframe
predsclean <- read.csv(filename)

#take subset for loading into a table
predscleansub<-predsclean[c("MyDate","predicted_yesno", "yesno", "ticker")]
predscleansub$ETL_DATE<-Sys.Date()
dbWriteTable(mydb, value = predscleansub, name ="outputpreds" , row.names = F , append = T)





#here would be a bunch of sql code for data processing/manipulation
#removed for intellectual property concerns
#parameters excluded for the same reason


#This outputs the predictions as a csv file to my drive so I can access the predictions anywhere
holder1 = dbGetQuery(mydb, "select table1.column, table2.*
from table1 left join table2 on table1.MyDate=table2.Mydate
where [parameters go here];"
)

holder2 = dbGetQuery(mydb, "select *, ROW_NUMBER() over (order by Mydate) rownum from table2
where [parameters go here]
;"
)


write.csv(holder1, "H:\\My Drive\\holder1.csv")

write.csv(holder2, "H:\\My Drive\\holder2.csv")









# this is what training a model would look like

library(healthcareai)
library(RMySQL)

mydb = dbConnect(MySQL(), user='root', password='', dbname='stocks', host='localhost')




rs = dbSendQuery(mydb, "select
ticker
, MyDate
, tech1
, tech2
, tech3
, etc
, PercentChange
, yesno
from staging.Modeltraindata
where mydate >= '2018-01-11'
and mydate < '2023-03-01'
and ticker is not null
and yesno is not null"
)

data = fetch(rs, n=-1)
quick_models <- machine_learn(data, MyDate, ticker, PercentChange, outcome = yesno, n_folds =4, tune_depth=4, models = 'xgb')



save_models(quick_models, file = "C:/Users/wend0/OneDrive/Documents/Stocks/Models/ModelName.RDS")


rs = dbSendQuery(mydb, "select
ticker
ticker
, MyDate
, tech1
, tech2
, tech3
, etc
, PercentChange
, yesno
from staging.Modeltraindata
where mydate >= '2018-01-11'
and ticker is not null
and yesno is not null"
)

data = fetch(rs, n=-1)

predictions <- predict(quick_models, data)
filename <- paste0("C:\\Users\\wend0\\OneDrive\\Documents\\Stocks\\DataFiles\\file", Sys.Date(), ".csv", row.names = FALSE)
#write the file out as a log
write.csv(predictions,filename, row.names = FALSE)
#get the file back as a clean dataframe
predsclean <- read.csv(filename)

#take subset for loading into a table
predscleansub<-predsclean[c("MyDate","predicted_yesno", "yesno", "ticker")]
predscleansub$ETL_DATE<-Sys.Date()


dbWriteTable(mydb, value = predscleansub, name ="ModelOutputPreds" , row.names = F , append = T)



# these are the queries I use to plot the density and variable importance graphs
quick_models %>%
  predict(outcome_groups = 2) %>%
  plot()


get_variable_importance(quick_models) %>%
  plot()

