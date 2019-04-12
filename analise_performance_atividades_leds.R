library(sqldf)

library(tableHTML)

con <- dbConnect(SQLite(), dbname = "C:/Users/felip_kja6gpn/Desktop/ETL_LEDS/sqlite/database_temp")
horas <-dbGetQuery(con,"select * from horas_trabalhadas, particiapcoes")
participacoes <-dbGetQuery(con,"select * from particiapcoes")
listaEmails <-dbGetQuery(con,"select DISTINCT(EMAIL) from particiapcoes")

for(i in 1:nrow(listaEmails)) {
  row <- participacoes[i,]
  print(i)
  condicao = paste(listaEmails[i,1], "' group by  ATIVIDADE_META ", sep="")
  sqlQuery  = paste("select  ATIVIDADE_META as Atividade , (sum(QUANTIDADE_PARTICIPACAO) || '/'|| META_DE_PARTICIPACAO) as Performance, ((sum(QUANTIDADE_PARTICIPACAO)*100/META_DE_PARTICIPACAO) || '%') AS 'Performance(%)' from particiapcoes where EMAIL = '", condicao, sep="")
  analiseCadaParticipante <-dbGetQuery(con,  sqlQuery )
  htmlCode  <-tableHTML(analiseCadaParticipante) 
  print(condicao)
  if(i==1){
    htmlPerformance<-data.frame(htmlCode, listaEmails[i,1])
    names(htmlPerformance)[1]<-paste("TABELA_PERFORMANCE")
    names(htmlPerformance)[2]<-paste("EMAIL")
  }else{
    newRow <- data.frame(TABELA_PERFORMANCE=htmlCode,EMAIL=listaEmails[i,1]) 
    htmlPerformance<-rbind(htmlPerformance,newRow)
  }
  dbWriteTable(con, name = "analise_participacoes", value = htmlPerformance, row.names = TRUE,  overwrite=TRUE)
}







