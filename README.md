Execute in ipl/main.

I. DIRECTORY STRUCTURE of main : 

1. Data :
	Consists of all data files like Clusters, GvG Probabilites, PvP Probabilites. (Latest and fixed) 

2. Input :
	Consists of input files which has the team order info.
	The format would be column wise data. 
	<Team1 Batting Order>, <Team2 Bowling Order>, <Team2 Batting Order>, <Team1 Bowling Order>
	
	TestInputMatch.csv is the sample data of such a case from the match. 
	RCB vs DD
	http://www.espncricinfo.com/indian-premier-league-2016/engine/current/match/980921.html
	
3. predict_match.py
	The comments in the code provide explanation and working.

II. Instructions to Execute :

python -W ignore predict_match.py 
