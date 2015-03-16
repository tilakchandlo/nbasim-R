# dependencies: data.table
#library(data.table)
#library(plyr)
#library(doParallel)
#registerDoParallel(cores=2)

sim.nba.positions.parallel = function (iterations, per_mode=c("total","pergame","per36"), min.played=1000, num.teams=10, num.players=10) {
  sim = foreach(i=1:iterations) %dopar% sim.nba.positions(i, per_mode, min.played, num.teams, num.players)
  results = rbindlist(sim)
  results[c(20,1:19)]
  players = ddply(results, .(Player), numcolwise(mean))
  #players$nSim = ddply(results, ~Player, summarize, N=length(Player))
  #players$VORP = players$TOT_rank - players$TOT_rank[1.2 * num.teams * num.players]
  #players$Rank = rank(-players$VORP, ties.method="min")
  #players[order(-players$VORP),]
  players[order(-players$TOT_rank),]
}


sim.nba.positions = function (sim.id, per_mode, min.played, num.teams, num.players) {
  
  # load data if you have not already done so
  if(!exists("nba.dt")) {
    nba.dt = fread("doug.csv", stringsAsFactors=TRUE, headers=TRUE)  
  }

  # make 1/2 players util  
  nba.dt[sample(1:nrow(nba.dt), nrow(nba.dt)/2),"PS"] = "UTIL"
  
  # fread doesn't recognize factors in my file, so add them
  nba.dt$PS = as.factor(nba.dt$PS)
  nba.dt$Team = as.factor(nba.dt$Team)
  nba.dt$Player = as.factor(nba.dt$Player)
  
  # could make this flexible in future editions; TO should be last to drop off
  cats.initial = c("FGM", "FGA", "FTM", "FTA", "X3M", "TR", "AS", "ST", "BK", "PTS", "TO")
  cats.totaling = c("FGP", "FTP", "X3M", "TR", "AS", "ST", "BK", "PTS", "TO")
  cats.ranking = c("FGP_rank", "FTP_rank", "X3M_rank", "TR_rank", "AS_rank", "ST_rank", "BK_rank", "PTS_rank", "TO_rank")
  
  # filter by minutes, let the user decide
  nba.dt = nba.dt[nba.dt$Min >= min.played,]
  
  # per_mode - adjust stats accordingly (no need if totals)
  if (per_mode == 'pergame') {
    nba.dt[,cats.initial] = lapply(nba.dt[,cats.initial], FUN = function(x) x / nba.dt$GP)  
  } else if (per_mode == 'per36') {
    nba.dt[,cats.initial] = lapply(nba.dt[,cats.initial], FUN = function(x) x / nba.dt$Min * 36)    
  }
  
  # sample equal number of positions
  positional.group.size = num.teams * num.players / 10
  utility.size = num.teams * num.players / 2
  
  # now do the sampling
  # need to work in test for adequate number of samples
  positional.groups = split(nba.dt, list(nba.dt$PS))
  positional.group.sample = lapply(positional.groups[c("PG","SG","SF","PF","C")], function(x) x[sample(1:nrow(x), num.players, FALSE),])
  positional.group.sample = lapply(positional.group.sample, function(x) team.numbers(x, seq(from=1, to=num.players, by=1)))
  utility.group.sample = positional.groups$UTIL[sample(nrow(positional.groups$UTIL), utility.size), ]
  utility.group.sample$teamID = rep(1:num.teams, each=num.players/2)
  
  # this is a pool with the correct number of positions per team
  pool = rbind(rbindlist(positional.group.sample), utility.group.sample)
  pool$teamID = as.factor(pool$teamID)
  setkey(pool, "teamID")
  
  ##########################################
  ### CALCULATE TEAM TOTALS & RANKS
  
  # sum up counting categories by team; need to name 1st column after calling aggregate
  #team.totals = aggregate(pool[,cats.initial],by=list(pool$teamID), sum)
  #team.totals
  team.totals = pool[, lapply(.SD, sum, na.rm=TRUE), by=pool$teamID, .SDcols=cats.initial]
  setnames(team.totals,"pool","teamID")
  
  # calculate team % categories (can't just sum %)
  team.totals$FGP = team.totals$FGM / team.totals$FGA
  team.totals$FTP = team.totals$FTM / team.totals$FTA
  
  # adjust turnovers
  team.totals$TO = 0 - team.totals$TO

  # calculate ranks
  team.ranks = team.totals[, lapply(.SD, rank, ties.method='min'), .SDcols=cats.totaling]
  team.ranks$teamID = team.totals$teamID
  setnames(team.ranks,cats.totaling,cats.ranking)
  team.results = merge(team.totals, team.ranks, by=c("teamID"))
  team.results[,"TOT_rank":=rowSums(.SD, na.rm = TRUE), .SDcols=cats.ranking]
  team.results$teamID = as.factor(team.results$teamID)
  setkey(team.results, "teamID")
  merge(team.results, pool[, c("Player", "teamID"), with=FALSE])
}

team.numbers = function(dt, rng){
  dt[,"teamID"] = rng
  dt
}
