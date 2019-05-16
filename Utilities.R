#-----------------------------
# functions
#-----------------------------
#   install all packages: requires a list of packages that you wish to load or install
#-----------------------------

install_all_packages <- function(package_names){
  for(package in package_names){
    if (!package %in% installed.packages()[, 1]){
      install.packages(package, dependencies = T, character.only = T)
    }
    require(package, character.only = T)
  }
}

#-----------------------------
#   build gameids
#-----------------------------
#     post season
#-----------------------------

post_season_ids <- function(season_start, season_end){
  
  # params
  seasons <- season_start:season_end
  season_type <- "03"
  playoff_rounds <- 1:4
  matchups <- 1:8
  games <- 1:7
  
  # build vector
  gameids <- c()
  for (season in seasons){
    for (round in playoff_rounds){
      if(round==1){
        matchups <- 1:8
      } else if (round ==2) {
        matchups <- 1:4 
      } else if (round==3) {
        matchups <- 1:2
      } else {
        matchups <- 1
      }
      for (matchup in matchups){
        for (game in games){
          tempgameid <- paste0(as.character(season),
                               season_type, 
                               str_pad(as.character(round), 2, side = 'left', pad = '0'), 
                               as.character(matchup), 
                               as.character(game))
          gameids <- c(gameids, tempgameid)
        }
      }
    }
  }
  return(gameids)
}

#-----------------------------
#     regular season
#-----------------------------

reg_season_ids <- function(season_start, season_end){
  
  # params
  seasons <- season_start:season_end
  season_type <- "02"
  games <- 1:1231
  
  # build vector
  gameids <- c()
  for(season in seasons){
    for(game in games){
      tempgameid <- paste0(as.character(season), as.character(season_type), str_pad(as.character(game), 4, side = 'left', pad = '0'))
      gameids <- c(gameids, tempgameid)
    }
  }
  return(gameids)
}

#-----------------------------
#     full season
#-----------------------------

full_season_ids <- function(season_start, season_end){
  reg <- reg_season_ids(season_start, season_end)
  pos <- post_season_ids(season_start, season_end)
  full <- c(reg, pos)
  return(full)
}

#-----------------------------
#   current games
#-----------------------------
get_current_games <- function(db_user, db_name, db_password, db_hostname){
  mdb <- dbConnect(MySQL(), user=db_user, password=db_password, dbname=db_name, host=db_hostname)
  current_gameids <- dbSendQuery(mdb, "SELECT distinct game_id FROM GAME;")
  current_gameids <- fetch(current_gameids, n = -1)
  current_gameids <- as.list(current_gameids$game_id)
  current_gameids <- unlist(current_gameids)
  current_gameids <- as.character(current_gameids)
  return(current_gameids)
}

#-----------------------------
# requires a vector of game ids, a base url to create each url to be test, the string to be replaced
#-----------------------------
base <- "https://statsapi.web.nhl.com/api/v1/game/replace/feed/live"

validate <- function(gameids, base_url, replace_string){
  urls <- tibble()
  iter <- 1
  for(gameid in gameids){
    url <- str_replace(base_url, replace_string, gameid)
    exists <- GET(url)$status_code
    urls[iter, 1] <- url
    urls[iter, 2] <- exists
    urls[iter, 3] <- gameid
    iter <- iter + 1
  }
  names(urls) <- c("url", "page.status", "gameid")
  urls <- urls[urls$page.status==200, ]
  return(urls)
}

#-----------------------------
# data validation
#-----------------------------
# 		db utils
#-----------------------------
RemoveAllConnections <- function(){
 connections <- dbListConnections(MySQL())
  for(connection in connections)
  {
    dbDisconnect(connection)
  }
  return(dbListConnections(MySQL()))
}

GetSampleData <- function(connection, query){
	  queryResult <- dbSendQuery(connection, query)
	  queryResult <- fetch(queryResult)
	  return(queryResult)
}

WriteAllTables(dataframes, tables, credentials, append){
	for(i in length(dataframes)){
	  connection = dbConnect(MySQL(), user=credentials[1], password=credentials[3], dbname=credentials[2], host=credentials[4])
	  tempdata <- get(dataframes[i])
	  temptbl <- tables[i]
	  dbWriteTable(connection, name = temptbl, value = tempdata, row.names = F)
	  RemoveAllConnections()
	}
}

GetAllTables <- function(tbl_names, credentials	){
		connection = dbConnect(MySQL(), user=credentials[1], password=credentials[3], dbname=credentials[2], host=credentials[4])
		db_tables <- dbListTables(connection)
		for(i in 1:length(db_tbls){
			query <- paste0('select * from ', db_tbls[i], ';')
			tempdata <- dbGetQuery(connection, query)
			assign()
		}
		query <- paste0('SELECT * FROM ', )
		tempdata <- dbGetQuery(connection, )
}