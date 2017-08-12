import urllib2
import csv
from bs4 import BeautifulSoup
ipls = ["http://www.espncricinfo.com/ipl/engine/series/313494.html","http://www.espncricinfo.com/ipl2009/engine/series/374163.html","http://www.espncricinfo.com/ipl2010/engine/series/418064.html","http://www.espncricinfo.com/indian-premier-league-2011/engine/series/466304.html","http://www.espncricinfo.com/indian-premier-league-2012/engine/series/520932.html","http://www.espncricinfo.com/indian-premier-league-2013/engine/series/586733.html","http://www.espncricinfo.com/indian-premier-league-2014/engine/series/695871.html","http://www.espncricinfo.com/indian-premier-league-2015/engine/series/791129.html","http://www.espncricinfo.com/indian-premier-league-2016/engine/series/968923.html"]

year = 2008
ipl_lists = {}
prependum = "http://www.espncricinfo.com"
appendum = "?view=pvp"
print "Getting IPL URLs ..."
for tournament_url in ipls :
	tourney_url = urllib2.urlopen(tournament_url)
	page = BeautifulSoup(tourney_url,'lxml')
	list = []
	name = "ipl_" + str(year)
	for link in page.findAll('a'):
		if(link.getText() == 'Scorecard'):
				list.append(prependum + str(link.get('href')) + appendum)
	ipl_lists.update({name:list})
	year = year + 1	
print "IPL URLs done"
year = 2008
print "Running through and creating CSVs"
for ipl_year in sorted(ipl_lists.iterkeys()):
	name = 1
	for match_url in ipl_lists[ipl_year]:
		page = urllib2.urlopen(match_url).read()
		soup = BeautifulSoup(page,'lxml')
		tables = soup.findAll('table')
		#date = soup.findAll('title')[0].getText().split('|')[0].split(',')[1] + soup.findAll('title')[0].getText().split('|')[0].split(',')[2]  
		batsmen = []
		for batsman in soup.findAll('caption'):
			batsmen.append(batsman.getText())
		title = "ipl_" + str(year)+ "match" + str(name)
		match = open((title+".csv"), 'wb')
		#match.write(date + '\n')
		f = csv.writer(match)
		f.writerow(["Batsman","Bowler","0s","1s","2s","3s","4s","6s","Dismissal","Runs","Balls","SR"])
		for j in tables :
			for row in j.findAll('tr')[1:]:
					 batter_row = batsmen[tables.index(j)].split("-")[0][:(len(batsmen[tables.index(j)].split("-")[0])-1)]
					 col = row.findAll('td')
					 bowler = col[0].getText()
					 zeros = col[1].getText()
					 ones = col[2].getText()
					 twos = col[3].getText()
					 threes = col[4].getText()
					 fours = col[5].getText()
					 #fives = col[6].getText()
					 sixes = col[7].getText()
					 #sevens = col[8].getText()
					 if not(col[9].getText() == ''):
						dismissal = 1
						zeros = str(int(zeros) - 1)
					 else:
						dismissal = 0
					 runs = col[10].getText()
					 balls = col[11].getText()
					 sr = col[12].getText()
					 data = (batter_row,bowler,zeros,ones,twos,threes,fours,sixes,dismissal,runs,balls,sr)
					 f.writerow(data)
		print(match.name + " done")
		name = name + 1
	year = year + 1
	
print "Done creating CSVs"