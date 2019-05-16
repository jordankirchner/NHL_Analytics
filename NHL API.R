#-----------------------------
# environment
#-----------------------------
# utility file contains all custom functions needed for this script
source("C:\\Users\\jordankirchner\\Desktop\\NHL\\Code\\Utilities.R")
libs <- c('RMySQL', 'jsonlite', 'tidyr', 'stringr', 'RCurl', 'httr')
install_all_packages(libs)
rm(libs)

# db credentials
credentials <- c('teamgk_jordank', 'teamgk_NHL', 'lchs2060', 'gkperformance.ca')

#-----------------------------
# build a vector of games to search for
#-----------------------------
gameids <- full_season_ids(2018, 2018)
current_games <- get_current_games(creds[1], creds[2], creds[3], creds[4])
gameids <- gameids[which(!gameids %in% current_games)]
validated <- validate(gameids, base, 'replace')

#-----------------------------
# iterate through each game, parse json, append to file
#-----------------------------

# create containers
tbl_game <- tibble()
tbl_play <- tibble()
tbl_team <- tibble()


for(game in 1:nrow(validated)){
  #-----------------------------
  # setup
  #-----------------------------
  gameid <- as.character(validated[game, 3])
  url <- as.character(validated[game, 1])
  jsonresult <- read_json(url, simplifyVector = T)
  
  #-----------------------------
  # game table
  #-----------------------------
  # only continue if there are plays to capture
  if(length(jsonresult$liveData$plays$allPlays) > 0){
    result <- flatten(jsonresult$liveData$plays$allPlays$result)
    about <- flatten(jsonresult$liveData$plays$allPlays$about)
    coordinates <- flatten(jsonresult$liveData$plays$allPlays$coordinates)
    temp_game <- cbind(about, result, coordinates)
    temp_game <- cbind(temp_game, data.frame(play_number = as.integer(row.names(temp_game))-1))
    temp_game$game_id <- gameid
    
    #-----------------------------
    # play
    #-----------------------------
    events <- temp_game$eventIdx
    tbl_sub_play <- tibble()
    collect_names <- c("player.id", "player.fullName", "player.link", "playerType", "seasonTotal")
    for(event in events){
      eventidx <- event
      play <- event + 1
      temp_play <- jsonresult$liveData$plays$allPlays$players[[play]]
      if(length(temp_play) > 0){
        temp_play <- flatten(temp_play)
        temp_play <- temp_play[, which(names(temp_play) %in% collect_names)]
        temp_play$eventIdx <- eventidx
        temp_play$game_id <- gameid
        tbl_sub_play <- rbind.fill(tbl_sub_play, temp_play)
      }
    }
    
    #-----------------------------
    # team
    #-----------------------------
    temp.team.away <- data.frame(jsonresult$liveData$boxscore$teams$away$teamStats$teamSkaterStats)
    temp.team.away$game_id <- gameid
    temp.team.away$team_name <- jsonresult$liveData$boxscore$teams$away$team$name
    temp.team.away$team_id <- jsonresult$liveData$boxscore$teams$away$team$id
    
    temp.team.home <- data.frame(jsonresult$liveData$boxscore$teams$home$teamStats$teamSkaterStats)
    temp.team.home$game_id <- gameid
    temp.team.home$team_name <- jsonresult$liveData$boxscore$teams$home$team$name
    temp.team.home$team_id <- jsonresult$liveData$boxscore$teams$home$team$id
    
    temp_team <- rbind(temp.team.away, temp.team.home)
    #temp_team$'Unnamed: 0' <- NA
    
    #-----------------------------
    # merge all results
    #-----------------------------
    tbl_game <- rbind(tbl_game, temp_game)
    tbl_play <- rbind(tbl_play, tbl_sub_play)
    tbl_team <- rbind(tbl_team, temp_team)
  }
}

#-----------------------------
# clean up
#-----------------------------
exceptions <- c('tbl_play', 'tbl_game', 'tbl_team')
  remove <- ls()[which(!ls() %in% exceptions)]
  for(r in remove){rm(list = r)}

#-----------------------------
# data validation
#-----------------------------
#   sample data to compare against
#-----------------------------  

tbls <- dbListTables(mydb)
query_base <- 'select * from tbl_name limit 10;'

for(tbl in tbls){
  RemoveAllConnections()
  mydb = dbConnect(MySQL(), user=creds[1], password=creds[3], dbname=creds[2], host=creds[4])
  temp_query <- str_replace(query_base, 'tbl_name', tbl)
  temp_sample_df <- GetSampleData(mydb, temp_query)
  obj_name <- paste0('sample_', tbl)
  assign(obj_name, temp_sample_df)
}

RemoveAllConnections()

#-----------------------------
# push to database
#-----------------------------

dataframes <- c('tbl_game', 'tbl_play', 'tbl_team')
tables <- dbListTables(mydb)

# verify that both vectors are in the correct order

WriteAllTables(dataframes, tables, credentials, T)

# get all data
for(i in 1:length(tables)){
  connection = dbConnect(MySQL(), user=credentials[1], password=credentials[3], dbname=credentials[2], host=credentials[4])
  query <- paste0('select * from ', tables[i])
  tempdf <- dbGetQuery(connection, query)
  write.csv(tempdf, paste0(tables[i], '.csv'), row.names = F)
  RemoveAllConnections()
}

