require '../lib/util.rb'    

class Visit
	attr_accessor :time, :url, :referrer
	
	def initialize(time, url, referrer)
		@time = time
		@url = url
		@referrer = referrer
	end

end

class UserSession 
	attr_accessor :userID, :sessionID, :sessionStart, :sessionEnd, :numPages
	
	def initialize(userID, sessionStart, sessionEnd, numPages, idGen)
		@userID = userID
		@sessionID = idGen.generate(16)
		@sessionStart = sessionStart
		@sessionEnd = sessionEnd
		@numPages = numPages
		@visits = []
	end

	def genVisits(pages, keyWords, referrers)
		duration = @sessionEnd - @sessionStart
		avTimeSpent = duration / (@numPages + 1)
		time = @sessionStart
		1.upto @numPages do
			index = rand(pages.length)
			page = pages[index]
			if (page.rindex("search"))
				page = page + "/" + keyWords[rand(keyWords.length)]
			end
			if (@visits.length == 0)
				referrer = referrers[1 + rand(referrers.length - 1)]
			else 
				referrer = referrers[0] + @visits[@visits.length - 1].url
			end
			visit = Visit.new(time, page, referrer)
			@visits << visit
			time = time + avTimeSpent / 4  + rand((3 * avTimeSpent) / 2)
			#puts "user visit time #{@userID}  #{time}"
		end 
	end
	
	def clearVisits 
		@visits.clear
	end
	
	def findPage(time)
		@visits.find { |v| v.time == time }
	end
	
	def cookie
		c = "__RequestVerificationToken_Lw__=#{userID};+.ASPXAUTH=#{sessionID}"
	end
	
end

def timeFormatted(time)
	sec = time % 60
	min = time / 60
	hour = min / 60
	min = min % 60
	timeForm = "%02d:%02d:%02d" % [hour, min, sec]
end

def addPage(pages, page, count)
	1.upto count do
		pages << page
	end
end

date = ARGV[0]
numUser = ARGV[1].to_i
users = []
activeSessions = []
pages = []
secDay = 24 * 60 * 60
idGen = IdGenerator.new
authTokenName = "__RequestVerificationToken_Lw__"
sessionIDName = ".ASPXAUTH"

#page list
keyWords = []
keyWords << "multivitamin"
keyWords << "allergy"
keyWords << "cholestrol"
keyWords << "blood+pressure"
keyWords << "toxin+cleanser"

numPage = 12
addPage(pages, "/search", 30)
addPage(pages, "/myAccount", 8)
addPage(pages, "/myCart", 14)
addPage(pages, "/myWishList", 10)
addPage(pages, "/trackOrder", 16)
addPage(pages, "/addToCart", 14)
addPage(pages, "/help", 6)

#flow
flow = []
flow << "/shoppingCart"
flow << "/checkOut"
flow << "/signin"
flow << "/signup"
flow << "/billing"
flow << "/confirmShipping"
flow << "/placeOrder"

#refererrs
referrers = []
referrers << "http://www.shophealthy.com"
referrers << "-"
referrers << "http://www.google.com"
referrers << "http://www.facebook.com"
referrers << "http://www.twitter.com"

# product pages
File.open("product.txt", "r") do |p|
	while (line = p.gets)
		prodID = line.split[0]
		pages  << "/product/#{prodID}"
	end
end

#sample user from user list for the day
File.open("user.txt", "r") do |infile|
	count = 0
	skip = rand(10) + 1
	while ((line = infile.gets) && (count < numUser))
		if (skip > 0)
			skip = skip -1
		else 
			sessionStart = rand(secDay - 300)
			duration = 10 + rand(1200)
			sessionEnd = sessionStart + duration
			sessionEnd = sessionEnd < secDay ? sessionEnd : secDay - 1
			numPages = duration / 90 + rand(4) - 2
			numPages = numPages < 1  ? 1 : numPages
			#puts "#{line} #{sessionStart} #{sessionEnd} #{numPages}"
			userSession = UserSession.new(line, sessionStart, sessionEnd, numPages, idGen)
			users << userSession
			skip = rand(10) + 1
			count = count + 1
		end
	end
end

#scan through all secs in a day
i = 0
while i < secDay
	users.each do |u|
		if u.sessionStart == i
			activeSessions << u;
			u.genVisits(pages, keyWords, referrers)
		elsif u.sessionEnd == i
			activeSessions.delete(u)
			u.clearVisits
		end
	end
	#puts "active session count #{activeSessions.length}"
	activeSessions.each do |ac|
		visit = ac.findPage(i)
		if (visit)
			l =  "#{date}  #{timeFormatted(i)}  #{ac.cookie}  #{visit.url} #{visit.referrer}"
			puts l.gsub("\n", "")
		end
	end
	i = i + 1;
end


