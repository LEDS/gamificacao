library(sqldf)
library(tableHTML)
library(sys)


con <- dbConnect(SQLite(), dbname = "C:/Users/felip_kja6gpn/Desktop/ETL_LEDS/sqlite/database_temp")
horas <-dbGetQuery(con,"select * from horas_trabalhadas, particiapcoes")
participacoes <-dbGetQuery(con,"select * from particiapcoes")
listaEmails <-dbGetQuery(con,"select DISTINCT(EMAIL) from particiapcoes")


for(i in 1:nrow(listaEmails)) {
  row <- participacoes[i,]
  print(listaEmails[i,1])
  condicao = paste(listaEmails[i,1], "' ", sep="")
  
#  select (100-sum(Performance)) as indiceGeral from (select (((1-(sum(QUANTIDADE_PARTICIPACAO))/META_TOTAL))*PESO_META) as Performance, ATIVIDADE_META from particiapcoes where email = 'rodrigocalhau@gmail.com' GROUP by ATIVIDADE_META)

  #select ((SUM(QUANTIDADE_PARTICIPACAO) / ROUND(sum(MICRO_META)))*100 || "%") as Performance, ATIVIDADE_META from particiapcoes where  EMAIL = 'felipefo@gmail.com'  group by  ATIVIDADE_META   

  #select SUM(QUANTIDADE_PARTICIPACAO) || "/" || ROUND(sum(MICRO_META)) as Performance, ATIVIDADE_META from particiapcoes where  EMAIL = 'felipefo@gmail.com'  group by  ATIVIDADE_META     


  sqlQuery <- "select ATIVIDADE_META, SUM(QUANTIDADE_PARTICIPACAO) || '/' || ROUND(sum(MICRO_META)) as Performance, ((SUM(QUANTIDADE_PARTICIPACAO)/ROUND(sum(MICRO_META)))*100 || '%') as 'Performance(%)' from particiapcoes where  EMAIL = '"
  sqlQuery  <- paste(sqlQuery, condicao, sep="")
  sqlQuery  = paste(sqlQuery, " group by  ATIVIDADE_META", sep="")
  analiseCadaParticipanteGeral <-dbGetQuery(con,  sqlQuery )
  htmlCodeNotaGeral  <-tableHTML(analiseCadaParticipanteGeral) 
  
  
  
  sqlQueryPorMes <- "select ATIVIDADE_META, SUM(QUANTIDADE_PARTICIPACAO) || '/' || ROUND(sum(MICRO_META)) as Performance, ((SUM(QUANTIDADE_PARTICIPACAO)/ROUND(sum(MICRO_META)))*100 || '%') as 'Performance(%)' from particiapcoes where strftime('%Y', date('now')) = Year_1  and  EMAIL = '"
  sqlQueryPorMes  <- paste(sqlQueryPorMes, condicao, sep="")
  sqlQueryPorMes  <- paste(sqlQueryPorMes, " group by  ATIVIDADE_META", sep="")
  analiseCadaParticipantePorMes <-dbGetQuery(con,  sqlQueryPorMes )
  htmlCodeGeralNoAno  <-tableHTML(analiseCadaParticipantePorMes)
  
  
  
  sqlQueryIndiceGeral <- "select ROUND(avg(Performance)) as 'Desempenho Geral(%)' from (select ((SUM(QUANTIDADE_PARTICIPACAO)/ROUND(sum(MICRO_META)))*100) as Performance, ATIVIDADE_META, Monht_1 || '/' || Year_1 from particiapcoes where  EMAIL = '"
  sqlQueryIndiceGeral  <- paste(sqlQueryIndiceGeral, condicao, sep="")
  sqlQueryIndiceGeral  <- paste(sqlQueryIndiceGeral, " group by  ATIVIDADE_META,Monht_1 || '/' || Year_1 )", sep="")
  analiseIndiceGeral <-dbGetQuery(con,  sqlQueryIndiceGeral )
  print(sqlQueryIndiceGeral)
  htmlIndiceGeral <-tableHTML(analiseIndiceGeral) 
  
  
  
  if(i==1){
    htmlPerformance<-data.frame(htmlCodeGeralNoAno,htmlIndiceGeral, htmlCodeNotaGeral,listaEmails[i,1])
    names(htmlPerformance)[1]<-paste("PERFORMANCE_DO_ANO")
    names(htmlPerformance)[2]<-paste("INDICE_GERAL")
    names(htmlPerformance)[3]<-paste("PERFORMANCE_GERAL")
    names(htmlPerformance)[4]<-paste("EMAIL")
  }else{
    newRow <- data.frame(PERFORMANCE_DO_ANO=htmlCodeGeralNoAno,INDICE_GERAL=htmlIndiceGeral,PERFORMANCE_GERAL=htmlCodeNotaGeral,EMAIL=listaEmails[i,1]) 
    htmlPerformance<-rbind(htmlPerformance,newRow)
  }
  dbWriteTable(con, name = "analise_participacoes", value = htmlPerformance, row.names = TRUE,  overwrite=TRUE)
  
}







