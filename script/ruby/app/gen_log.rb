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
		@home = "http://www.healthyshopping.com"
	end

	def genVisits(pageDist, flow, keyWords, referrerDist)
		duration = @sessionEnd - @sessionStart
		avTimeSpent = duration / (@numPages + 1)
		time = @sessionStart
		
		r = rand(100)
		enteredFlow = r < 10
		completedFlow = r < 6
		np = enteredFlow ? (2 + rand(3)) : @numPages;
		#puts "*** flow ***#{enteredFlow}  #{completedFlow}"
		
		1.upto np do
			page = pageDist.value
			if (page.rindex("search"))
				page = page + "/" + keyWords[rand(keyWords.length)]
			end
			if (@visits.length == 0)
				#external referrer
				referrer = referrerDist.value
			else 
				#internal referrer
				referrer = @home + @visits[@visits.length - 1].url
			end
			visit = Visit.new(time, page, referrer)
			@visits << visit
			time = time + avTimeSpent / 4  + rand((3 * avTimeSpent) / 2)
			
			#if product page visit follow up by add to cart
			if (page.rindex("product") && rand(10) < 3)
				referrer = page
				product = page.split('/')[2]
				page = "/addToCart/" +  product
				visit = Visit.new(time, page, referrer)
				@visits << visit
				time = time + avTimeSpent / 4  + rand((3 * avTimeSpent) / 2)
			end
			
			#puts "user visit time #{@userID}  #{time}"
		end 
		
		#visits for flow
		if (enteredFlow)
			flow.each do |p|
				referrer = @home + @visits[@visits.length - 1].url
				visit = Visit.new(time, p, referrer)
				time = time + avTimeSpent / 4  + rand((3 * avTimeSpent) / 2)
				@visits << visit
				
				if (!completedFlow && (rand(3) == 0))
					break
				end
			 
			end
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
secDay = 24 * 60 * 60
secHour = 60 * 60
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


#flow
flow = []
flow << "/shoppingCart"
flow << "/checkOut"
flow << "/signin"
flow << "/signup"
flow << "/billing"
flow << "/confirmShipping"
flow << "/placeOrder"

referrerDist = CategoricalField.new("-", 2, "http://www.google.com", 6, 
  "http://www.facebook.com", 3, "http://www.twitter.com", 2, "http://www.myhealth.com", 5)

# pages
pageDistValues = []
pageDistValues << "/search"
pageDistValues << 30
pageDistValues << "/myAccount"
pageDistValues << 8
pageDistValues << "/myCart"
pageDistValues << 14
pageDistValues << "/myWishList"
pageDistValues << 10
pageDistValues << "/trackOrder"
pageDistValues << 16
pageDistValues << "/addToCart"
pageDistValues << 14
pageDistValues << "/help"
pageDistValues << 6


# product pages
File.open("product.txt", "r") do |p|
	while (line = p.gets)
		prodID = line.split[0]
		pageDistValues  << "/product/#{prodID}"
		pageDistValues << (1 + rand(5))
	end
end

pageDist = CategoricalField.new(pageDistValues)


#arrival time distribution
hourDist = NumericalField.new(false,0..4,10,5..8,20,9..14,30,15..16,50,17..18,30,19..20,40,21..23,20)
#duration distribution sec
durationDist = NumericalField.new(false,1..5,50,6..20,150,21..40,400,41..60,200,61..120,150,121..240,300,
  241..360,600,361..480,900,481..600,700,601..720,500,721..900,300,901..1200,100)

#sample user from user list for the day
File.open("user.txt", "r") do |infile|
	count = 0
	skip = rand(10) + 1
	while ((line = infile.gets) && (count < numUser))
		if (skip > 0)
			skip = skip -1
		else 
			#sessionStart = rand(secDay - 300)
			hour = hourDist.value
			sessionStart = hour * secHour + rand(secHour) - 600
			
			#duration = 10 + rand(1200)
			dval = durationDist.value
			spread = dval / 10
			duration = dval  + rand(spread) - spread/2 
			
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
			u.genVisits(pageDist, flow, keyWords, referrerDist)
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


